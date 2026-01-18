from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Definicja domyślnych argumentów
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Nazwa folderu wersji: np. "run_2023-10-27"
# {{ ds }} to makro Airflow zwracające datę wykonania (YYYY-MM-DD)
VERSION_PATH = "run_{{ ds }}"

with DAG('chembl_data_pipeline_v1',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # Zadanie 1: Generuj dataset tylko dla ludzi
    task_human = SparkSubmitOperator(
        task_id='process_human_data',
        conn_id='spark_default',             # Połączenie zdefiniowane w Airflow UI
        application='/opt/airflow/dags/scripts/spark_job.py', # Ścieżka do skryptu (wolumen)
        packages='org.postgresql:postgresql:42.6.0',          # Automatyczne pobranie sterownika
        application_args=[
            "--mode", "HUMAN",
            "--output_path", f"/opt/airflow/data/processed/{VERSION_PATH}/human_only.parquet",
            "--db_host", "postgres-db" # Nazwa kontenera bazy
        ],
        conf={
            "spark.master": "spark://spark-master:7077"
        }
    )

    # Zadanie 2: Generuj dataset dla wszystkich
    task_all = SparkSubmitOperator(
        task_id='process_all_data',
        conn_id='spark_default',
        application='/opt/airflow/dags/scripts/spark_job.py',
        packages='org.postgresql:postgresql:42.6.0',
        application_args=[
            "--mode", "ALL",
            "--output_path", f"/opt/airflow/data/processed/{VERSION_PATH}/all_species.parquet",
            "--db_host", "postgres-db"
        ],
        conf={
            "spark.master": "spark://spark-master:7077"
        }
    )

    # Zadania mogą iść równolegle
    [task_human, task_all]