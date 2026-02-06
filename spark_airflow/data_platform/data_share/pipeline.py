import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, log10, trim, udf
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType
import argparse

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

if args.organism_scope == "HUMAN":
    raw_sql_query = load_query("sql/humanorgs.sql")
else:
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

# A. Usunięcie URLi w danych numerycznych
df = df.withColumn("standard_value", col("standard_value").cast("double"))
df = df.filter(col("standard_value").isNotNull())
# df = df.filter(col("standard_value") < 1e10)

# B. Imputacja jednostek i konwersja na Molary [M]
# Logika: > 100 to zazwyczaj nM, < 0.1 to zazwyczaj M
df_clean = df.withColumn("value_molar",
    when(col("standard_units") == "nM", col("standard_value") * 1e-9)
    .when(col("standard_units") == "uM", col("standard_value") * 1e-6)
    .when(col("standard_units") == "M", col("standard_value"))
    .when(col("standard_units").isNull() & (col("standard_value") > 100), col("standard_value") * 1e-9) # Imputacja nM
    .when(col("standard_units").isNull() & (col("standard_value") <= 0.1), col("standard_value"))       # Imputacja M
    .otherwise(None)
).filter(col("value_molar").isNotNull())

# C. Wyliczenie pIC50 (Target)
# Jeśli pchembl_value jest puste, wyliczamy: -log10(value_molar)
df_target = df_clean.withColumn("target_pIC50",
    when(col("pchembl_value").isNotNull(), col("pchembl_value").cast("float"))
    .otherwise(-log10(col("value_molar")))
)

# ----------------
# 1. Definicja schematu danych wyjściowych (Features + Adjacency List)
graph_schema = StructType([
    StructField("atom_features", ArrayType(IntegerType()), False),
    StructField("edge_src", ArrayType(IntegerType()), False),
    StructField("edge_dst", ArrayType(IntegerType()), False)
])


# 2. Funkcja Pythonowa (Logika RDKit)
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

print("Generowanie reprezentacji grafowej...")

df_graphs = df_target.withColumn("graph_data", smiles_to_graph_udf(col("canonical_smiles")))

df_graphs = df_graphs.filter(col("graph_data").isNotNull())

# ==============================================================================
# KONIEC KODU TRANSFORMACJI
# ==============================================================================
if args.feature_mode == "GRAPH_ONLY":
    cols_to_select = [
        col("canonical_smiles").alias("input_smiles"),
        col("target_pIC50"),
        col("graph_data")
    ]
else:
    cols_to_select = [
        col("canonical_smiles").alias("input_smiles"),
        col("organism").alias("meta_organism"),
        col("confidence_score").alias("meta_confidence"),
        col("target_pIC50"),
        col("graph_data")
    ]

final_df = df_graphs.select(*cols_to_select)

print("Próbka danych wyjściowych:")
final_df.show(5)

# output_path = "file:///opt/spark/data/chembl_egfr_output.parquet"
# final_df.write.mode("overwrite").parquet(output_path)

print(f"Zapisywanie danych do: {args.output_path}")
final_df.write.mode("overwrite").parquet(args.output_path)

spark.stop()