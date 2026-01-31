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
parser.add_argument("--mode", type=str, required=True, choices=["HUMAN", "ALL"], help="Tryb filtrowania")
parser.add_argument("--output_path", type=str, required=True, help="Ścieżka do zapisu wynikowego pliku")
parser.add_argument("--db_host", type=str, default="postgres-db") # Nazwa serwisu w docker-compose
args = parser.parse_args()

# --- KONFIGURACJA POŁĄCZENIA ---
# DB_HOST = "192.168.0.5"
DB_HOST = args.db_host
DB_PORT = "5432"
DB_NAME = "chembl_36"
DB_USER = "admin"
DB_PASSWORD = "admin"

sql_query = """
(
    SELECT 
        act.activity_id,
        act.assay_id,
        act.standard_type,
        CAST(act.standard_value AS DOUBLE PRECISION) as standard_value,
        act.standard_units,
        act.standard_relation,
        act.pchembl_value,
        act.activity_comment,
        act.data_validity_comment,
        a.confidence_score,
        md.chembl_id,
        cs.canonical_smiles,
        csq.organism,
        td.pref_name as target_name
    FROM activities act
    JOIN assays a ON act.assay_id = a.assay_id
    JOIN target_dictionary td ON a.tid = td.tid
    JOIN target_components tc ON td.tid = tc.tid
    JOIN component_sequences csq ON tc.component_id = csq.component_id
    JOIN molecule_dictionary md ON act.molregno = md.molregno
    JOIN compound_structures cs ON act.molregno = cs.molregno
    WHERE 
        act.standard_type = 'IC50'
        AND csq.organism = 'Homo sapiens'
        AND act.standard_value IS NOT NULL
        AND act.standard_value < 10000000000
) as chembl_data
"""

# AND csq.organism = 'Homo sapiens'

# --- 1. INICJALIZACJA SPARKA ---
spark = SparkSession.builder \
    .appName(f"Chembl_EGFR_Pipeline_{args.mode}") \
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

        # Cechy atomów (np. liczba atomowa)
        atom_features = [atom.GetAtomicNum() for atom in mol.GetAtoms()]

        # Krawędzie (Wiązania)
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


# 3. Rejestracja UDF
smiles_to_graph_udf = udf(smiles_to_graph, graph_schema)

print("Generowanie reprezentacji grafowej...")

# 4. Aplikacja UDF na danych
df_graphs = df_target.withColumn("graph_data", smiles_to_graph_udf(col("canonical_smiles")))

# 5. Odrzucenie błędów konwersji (tam gdzie RDKit zwrócił null)
df_graphs = df_graphs.filter(col("graph_data").isNotNull())

# ==============================================================================
# KONIEC KODU TRANSFORMACJI
# ==============================================================================

# Teraz wybieramy kolumny do zapisu, włączając nową strukturę grafową
final_df = df_graphs.select(
    col("canonical_smiles").alias("input_smiles"),
    col("organism").alias("meta_organism"),
    col("confidence_score").alias("meta_confidence"),
    col("target_pIC50"),
    col("graph_data")  # <--- Dodajemy wygenerowany graf
)
# ----------------


# Wybór kolumn do zapisu
# Input 1: SMILES (Tekst)
# Input 2: Metadata (Liczby/Kategorie)
# final_df = df_target.select(
#     col("canonical_smiles").alias("input_smiles"),
#     col("organism").alias("meta_organism"),
#     col("confidence_score").alias("meta_confidence"),
#     col("target_pIC50")
# )

# Opcjonalnie: Filtrowanie tylko człowieka
# final_df = final_df.filter(col("meta_organism") == "Homo sapiens")

print("Próbka danych wyjściowych:")
final_df.show(5)

# --- 5. ZAPIS DO PLIKU ---
# Zapisujemy w formacie Parquet (szybki dla Sparka) lub CSV
output_path = "file:///opt/spark/data/chembl_egfr_output.parquet"
# final_df.write.mode("overwrite").parquet(output_path)
#
# print(f"Dane zapisane w: {output_path}")
print(f"Zapisywanie danych do: {args.output_path}")
final_df.write.mode("overwrite").parquet(args.output_path)

spark.stop()