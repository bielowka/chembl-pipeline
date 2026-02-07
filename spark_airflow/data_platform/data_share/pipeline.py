import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, log10, trim, udf
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType
import argparse

import numpy as np
import pandas as pd

try:
    from rdkit import Chem
except ImportError:
    # Fallback dla drivera, jeśli nie ma RDKit (ale workery muszą go mieć!)
    print("OSTRZEŻENIE: Brak RDKit na Driverze.")
    Chem = None

parser = argparse.ArgumentParser()
parser.add_argument("--output_path", type=str, required=True, help="Ścieżka do zapisu wynikowego pliku")
parser.add_argument("--db_host", type=str, default="postgres-db") # Nazwa serwisu w docker-compose

parser.add_argument("--target_name", type=str, choices=["EGFR", "ALL"], default="ALL", help="Nazwa celu")
parser.add_argument("--organism_scope", type=str, choices=["HUMAN", "ALL"], default="HUMAN", help="Czy filtrować organizm")
parser.add_argument("--feature_mode", type=str, choices=["GRAPH_ONLY", "WITH_METADATA"], default="WITH_METADATA", help="Zakres kolumn")

args = parser.parse_args()

# --- KONFIGURACJA POŁĄCZENIA ---
# DB_HOST = "192.168.0.5"
DB_HOST = args.db_host
DB_PORT = "5432"
DB_NAME = "chembl_36"
DB_USER = "admin"
DB_PASSWORD = "admin"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def load_query(relative_path):
    full_path = os.path.join(BASE_DIR, relative_path)
    print(f"Wczytywanie SQL z: {full_path}")

    with open(full_path, 'r') as f:
        return f.read()

raw_sql_query = load_query("sql/allorgs.sql")

target_clause = ""
if args.target_name and args.target_name != "ALL":
    safe_target = args.target_name.replace("'", "")
    target_clause = f"AND td.pref_name ILIKE '%{safe_target}%'"

sql_query = raw_sql_query.format(target_filter=target_clause)

# --- 1. INICJALIZACJA SPARKA ---
spark = SparkSession.builder \
    .appName(f"ChEMBL_{args.target_name}_{args.organism_scope}") \
    .getOrCreate()

print("Rozpoczynam pobieranie danych z PostgreSQL...")

df = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}") \
    .option("dbtable", sql_query) \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(f"Pobrano surowych rekordów: {df.count()}")

# Usunięcie URLi w danych numerycznych
df = df.withColumn("standard_value", col("standard_value").cast("double"))
df = df.filter(col("standard_value").isNotNull())

# Usuniecie invalid
invalid_comments = [
    'Outside typical range',
    'Potential transcription error',
    'Potential missing data',
    'Potential author error'
]

df = df[~df['data_validity_comment'].isin(invalid_comments)]

# Usunięcie rekordów z brakującymi wartościami w kluczowych kolumnach
columns_to_check = [
    'hbd',
    'num_ro5_violations',
    'psa',
    'rtb',
    'alogp',
    'hba'
]

df_clean = df.dropna(subset=columns_to_check)
df = df_clean.copy()

# Filtrowanie po standard type
valid_standard_types = [
    'IC50',
    'Ki',
    'Kd',
    'EC50',
    'AC50',
    'GI50'
]

df = df[df['standard_type'].isin(valid_standard_types)].copy()

# Filtrowanie po assay type
df = df[df['assay_type'].isin(['B', 'F'])]

# Filtrowanie po standard relation
df = df[df['standard_relation'] == '=']

# Konwersja jednostek na Molar

conditions = [
    (df['units_norm'] == 'nm'),                           # Nano (10^-9)
    (df['units_norm'].isin(['um', 'µm', 'microm'])),      # Mikro (10^-6)
    (df['units_norm'] == 'pm'),                           # Piko (10^-12)
    (df['units_norm'] == 'mm'),                           # Mili (10^-3)
    (df['units_norm'] == 'm'),                            # Molar (1)

    (df['standard_units'].isnull()) & (df['standard_value'] > 100),
    (df['standard_units'].isnull()) & (df['standard_value'] <= 0.1)
]

choices = [
    df['standard_value'] * 1e-9,   # nM
    df['standard_value'] * 1e-6,   # uM / µm
    df['standard_value'] * 1e-12,  # pM
    df['standard_value'] * 1e-3,   # mM
    df['standard_value'] * 1.0,    # M
    df['standard_value'] * 1e-9,   # Imputacja jako nM
    df['standard_value'] * 1.0     # Imputacja jako M
]

df['value_molar'] = np.select(conditions, choices, default=np.nan)
df = df.dropna(subset=['value_molar']).copy()

# Wyliczenie pIC50 tam, gdzie jest brakująca wartość, ale mamy wartość molar
calculated_pic50 = -np.log10(df['value_molar'])
df['pchembl_value'] = df['pchembl_value'].fillna(calculated_pic50)

# Usuniecie zadeklarowanych duplikatów
df = df[df['potential_duplicate'] == False]
df.drop(columns=['potential_duplicate'], inplace=True)

df = df.reset_index(drop=True)

# Usuwanie duplikatów
target_id_col = next((col for col in ['molecule_chembl_id', 'target_name', 'tid'] if col in df.columns), 'target_name')
mol_col = 'standard_inchi_key' if 'standard_inchi_key' in df.columns else 'canonical_smiles'
value_col = 'pchembl_value'

potential_cols = [mol_col, target_id_col, 'organism', 'standard_type', 'bao_format']
group_cols = [c for c in potential_cols if c in df.columns]

for col in group_cols:
    df[col] = df[col].fillna('Unknown')

print(f"Rows before cleaning: {len(df)}")
print(f"Grouping by: {group_cols}")

stats = df.groupby(group_cols)[value_col].agg(['mean', 'std', 'count'])

threshold = 1.0
conflict_mask = (stats['count'] > 1) & (stats['std'] > threshold)
conflicting_groups = stats[conflict_mask]
valid_groups = stats[~conflict_mask]

if not conflicting_groups.empty:
    print(f"\nFound {len(conflicting_groups)} conflicting groups. Top examples:")
    top_conflicts = conflicting_groups.sort_values('std', ascending=False).head(3)

    for idx, row in top_conflicts.iterrows():
        print(f"\nConflict Group Keys: {list(zip(group_cols, idx))}")
        print(f"Stats: StdDev={row['std']:.2f} | Count={int(row['count'])}")

        mask = pd.Series(True, index=df.index)
        for col_name, val in zip(group_cols, idx):
            mask &= (df[col_name] == val)

        cols_to_show = ['activity_id', 'standard_value', 'standard_type', 'bao_format', value_col]
        display(df[mask][[c for c in cols_to_show if c in df.columns]])

df_dedup = df.groupby(group_cols).first().reset_index()
df_final = pd.merge(df_dedup, valid_groups[['mean']], on=group_cols, how='inner')

df_final[value_col] = df_final['mean']
df_final = df_final.drop(columns=['mean'])

print(f"\nRows after cleaning: {len(df_final)}")
print(f"Removed total: {len(df) - len(df_final)}")

df = df_final

# Usuwanie silnie skolerowanych cech, priorytetyzujc "Piatke Lipinskiego"
ro5_features = ['mw_freebase', 'alogp', 'hba', 'hbd']

numeric_cols = df_final.select_dtypes(include=[np.number]).columns.tolist()

cols_to_exclude = ['pchembl_value', 'activity_id', 'standard_value', 'molregno', 'tid']
feature_cols = [c for c in numeric_cols if c not in cols_to_exclude]

corr_matrix = df_final[feature_cols].corr().abs()

threshold = 0.75

to_drop = set()

cols = corr_matrix.columns
for i in range(len(cols)):
    for j in range(i + 1, len(cols)):
        if corr_matrix.iloc[i, j] > threshold:
            col_a = cols[i]
            col_b = cols[j]

            is_a_ro5 = col_a in ro5_features
            is_b_ro5 = col_b in ro5_features

            if is_a_ro5 and not is_b_ro5:
                to_drop.add(col_b)
                print(f"Konflikt: {col_a} vs {col_b} -> Usuwam {col_b} (Chronię Ro5)")

            elif is_b_ro5 and not is_a_ro5:
                to_drop.add(col_a)
                print(f"Konflikt: {col_a} vs {col_b} -> Usuwam {col_a} (Chronię Ro5)")

            else:
                if col_a not in to_drop:
                    to_drop.add(col_b)

print(f"\nLista cech do usunięcia: {list(to_drop)}")

df_reduced = df_final.drop(columns=list(to_drop))
print(f"Liczba cech przed: {len(feature_cols)}")
print(f"Liczba cech po: {len(feature_cols) - len(to_drop)}")

df = df_reduced.copy()

# Skalowanie
from sklearn.preprocessing import StandardScaler

num_cols = ['mw_freebase', 'alogp', 'hbd', 'rtb']

scaler = StandardScaler()
df[num_cols] = scaler.fit_transform(df[num_cols])

# Usuniecie bao_endpoint
if 'bao_endpoint' in df.columns:
    df = df.drop(columns=['bao_endpoint'])

df = df.reset_index(drop=True)

# Encoding
df_encoded = df.copy()

top_n = 5
top_organisms = df_encoded['organism'].value_counts().nlargest(top_n).index.tolist()

print(f"Top {top_n} organisms: {top_organisms}")

df_encoded['organism_group'] = df_encoded['organism'].apply(
    lambda x: x if x in top_organisms else 'Other'
)

categorical_cols = ['standard_type', 'bao_format', 'organism_group']

encoded_dummies = pd.get_dummies(df_encoded[categorical_cols], prefix=['type', 'bao', 'org'], dtype=int)

df_encoded = pd.concat([df_encoded, encoded_dummies], axis=1)

print(f"\nLiczba kolumn przed encodingiem: {df_clean.shape[1]}")
print(f"Liczba kolumn po encodingu: {df_encoded.shape[1]}")

new_cols = [c for c in df_encoded.columns if c.startswith(('type_', 'bao_', 'org_'))]
print("\nNowe kolumny (cechy dla modelu):")
print(new_cols)

df = df_encoded.copy()

# Usuniecie niepotrzebnych kolumn
cols_to_drop = [
    'standard_type', 'bao_format', 'organism', 'organism_group',

    'activity_id', 'assay_id', 'standard_inchi_key',
    'target_name',

    'data_validity_comment', 'activity_comment', 'assay_type',

    'standard_value', 'value_molar', 'standard_units', 'units_norm', 'standard_relation',

    'max_phase', 'confidence_score'
]

existing_cols_to_drop = [c for c in cols_to_drop if c in df.columns]

df_ml = df.drop(columns=existing_cols_to_drop)
df_ml = df_ml.where(pd.notnull(df_ml), None)

print(f"Gotowy DataFrame Pandas: {df_ml.shape}")
print(f"Kolumny: {list(df_ml.columns)}")

print(f"Usunięto {len(existing_cols_to_drop)} kolumn.")

print("\nTypy danych w df_ml (powinny być tylko liczbowe):")
print(df_ml.dtypes.value_counts())

df = df_ml.copy()

print(f"Unikalne ID? {df.index.nunique() == len(df)}")

# Grafy
print("Konwersja Pandas -> Spark DataFrame...")
df_spark_final = spark.createDataFrame(df)

graph_schema = StructType([
    StructField("atom_features", ArrayType(IntegerType()), False),
    StructField("edge_src", ArrayType(IntegerType()), False),
    StructField("edge_dst", ArrayType(IntegerType()), False)
])


def smiles_to_graph(smiles):
    if not smiles:
        return None
    try:
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            return None

        atom_features = [atom.GetAtomicNum() for atom in mol.GetAtoms()]

        src = []
        dst = []
        for bond in mol.GetBonds():
            s = bond.GetBeginAtomIdx()
            e = bond.GetEndAtomIdx()
            src.extend([s, e])
            dst.extend([e, s])

        return (atom_features, src, dst)
    except:
        return None


smiles_to_graph_udf = udf(smiles_to_graph, graph_schema)
df_output = df_spark_final.withColumn("graph_data", smiles_to_graph_udf(col("canonical_smiles")))
df_output = df_output.filter(col("graph_data").isNotNull())


# ==============================================================================
# KONIEC KODU TRANSFORMACJI
# ==============================================================================
ignore_cols = ['canonical_smiles', 'pchembl_value', 'graph_data']
feature_cols = [c for c in df_output.columns if c not in ignore_cols]

print(f"Zidentyfikowane cechy tabelaryczne ({len(feature_cols)}): {feature_cols}")

cols_to_select = []

if args.feature_mode == "GRAPH_ONLY":
    cols_to_select = [
        col("canonical_smiles"),
        col("pchembl_value").alias("target"),
        col("graph_data")
    ]
else:
    cols_to_select = [
        col("canonical_smiles"),
        col("pchembl_value").alias("target"),
        col("graph_data")
    ] + [col(c) for c in feature_cols]

final_df = df_output.select(*cols_to_select)

print("Próbka danych wyjściowych:")
final_df.show(5)

print(f"Zapisywanie danych do: {args.output_path}")
final_df.write.mode("overwrite").parquet(args.output_path)

spark.stop()