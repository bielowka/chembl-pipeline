import os
import sys

# --- KONFIGURACJA JAVY (HARDCODED) ---
# Wymuszamy użycie Java 11, niezależnie od ustawień systemowych
# Sprawdź, czy ta ścieżka pokrywa się z wynikami komendy z Kroku 1
# java11_path = "/opt/homebrew/opt/openjdk@11"
#
# if os.path.exists(java11_path):
#     os.environ["JAVA_HOME"] = java11_path
#     os.environ["PATH"] = f"{java11_path}/bin:{os.environ.get('PATH', '')}"
#     print(f"--> Wymuszono użycie Javy z: {java11_path}")
# else:
#     print(f"--> OSTRZEŻENIE: Nie znaleziono Javy 11 w {java11_path}. Sprawdź ścieżkę!")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, log10, trim

# --- KONFIGURACJA POŁĄCZENIA ---
# Upewnij się, że nazwa hosta (DB_HOST) odpowiada nazwie kontenera w docker-compose
# lub adresowi IP, który jest widoczny dla Sparka.
DB_HOST = "192.168.0.5"
DB_PORT = "5432"
DB_NAME = "chembl_36"  # Twoja wersja bazy
DB_USER = "admin"      # Zgodnie z Twoim poleceniem psql
DB_PASSWORD = "admin"

# --- PRZYGOTOWANIE ZAPYTANIA SQL ---
# Używamy Twojego zapytania, ale zamieniamy LIMIT na filtrowanie pod kątem projektu (EGFR)
# Zapytanie musi być w nawiasie i mieć alias (np. "as subquery") dla Sparka JDBC.
# sql_query = """
# (
#     SELECT
#         act.activity_id,
#         act.assay_id,
#         act.standard_type,
#         act.standard_value,
#         act.standard_units,
#         act.standard_relation,
#         act.pchembl_value,
#         act.activity_comment,
#         act.data_validity_comment,
#         a.confidence_score,
#         md.chembl_id,
#         cs.canonical_smiles,
#         csq.organism,
#         td.pref_name as target_name
#     FROM activities act
#     JOIN assays a ON act.assay_id = a.assay_id
#     JOIN target_dictionary td ON a.tid = td.tid
#     JOIN target_components tc ON td.tid = tc.tid
#     JOIN component_sequences csq ON tc.component_id = csq.component_id
#     JOIN molecule_dictionary md ON act.molregno = md.molregno
#     JOIN compound_structures cs ON act.molregno = cs.molregno
#     WHERE
#         act.standard_type = 'IC50'          -- Interesuje nas tylko IC50
#         AND td.pref_name ILIKE '%EGFR%'     -- Filtrujemy raka / EGFR
#         AND act.standard_value IS NOT NULL  -- Odrzucamy puste wyniki
# ) as chembl_data
# """

sql_query = """
(
    SELECT 
        act.activity_id,
        act.assay_id,
        act.standard_type,
        act.standard_value,
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
        act.standard_type = 'IC50'          -- Interesuje nas tylko IC50
        AND act.standard_value IS NOT NULL  -- Odrzucamy puste wyniki
) as chembl_data
"""

# --- 1. INICJALIZACJA SPARKA ---
# spark = SparkSession.builder \
#     .appName("Chembl_EGFR_Pipeline") \
#     .master("spark://localhost:7077") \
#     .config("spark.executor.memory", "1g") \
#     .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
#     .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("Chembl_EGFR_Pipeline") \
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
# Czasami w 'standard_value' pojawiają się śmieci, upewniamy się, że to liczby
df = df.withColumn("standard_value", col("standard_value").cast("double"))
df = df.filter(col("standard_value").isNotNull())

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

# Wybór kolumn do zapisu
# Input 1: SMILES (Tekst)
# Input 2: Metadata (Liczby/Kategorie)
final_df = df_target.select(
    col("canonical_smiles").alias("input_smiles"),
    col("organism").alias("meta_organism"),
    col("confidence_score").alias("meta_confidence"),
    col("target_pIC50")
)

# Opcjonalnie: Filtrowanie tylko człowieka (możesz to odkomentować)
# final_df = final_df.filter(col("meta_organism") == "Homo sapiens")

print("Próbka danych wyjściowych:")
final_df.show(5)

# --- 5. ZAPIS DO PLIKU ---
# Zapisujemy w formacie Parquet (szybki dla Sparka) lub CSV
output_path = "file:///opt/spark/data/chembl_egfr_output.parquet"
final_df.write.mode("overwrite").parquet(output_path)

print(f"Dane zapisane w: {output_path}")

spark.stop()