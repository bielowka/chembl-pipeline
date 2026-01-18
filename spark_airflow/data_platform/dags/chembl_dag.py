from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Domyślne argumenty dla DAGa
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'chembl_processing_pipeline',
        default_args=default_args,
        description='Pipeline przetwarzający dane ChEMBL na Sparku',
        schedule_interval='@daily',  # Uruchamiaj codziennie
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['spark', 'chembl', 'eda'],
) as dag:
    # Definiujemy dynamiczne ścieżki przy użyciu Jinja Templating
    # {{ ts_nodash }} to timestamp uruchomienia, np. 20251221T143000
    # To zapewnia WERSJONOWANIE - każde uruchomienie to nowy folder.

    version_path = "runs/{{ ds }}/{{ ts_nodash }}"

    # Ścieżki wewnątrz kontenera (zgodne z docker-compose volume)
    script_path = "/opt/airflow/data/pipeline.py"
    output_path_human = f"file:///opt/airflow/data/{version_path}/human_egfr.parquet"

    # Zadanie 1: Przetwarzanie danych dla ludzi (Homo sapiens)
    process_human_data = SparkSubmitOperator(
        task_id='process_human_data',
        conn_id='spark_default',  # Połączenie, które skonfigurujemy w UI
        spark_binary="spark-submit",
        application=script_path,  # Ścieżka do skryptu python
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        driver_memory='1g',
        name='chembl_human_pipeline',
        packages='org.postgresql:postgresql:42.6.0',  # Automatyczne pobranie sterownika
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.jars.ivy": "/tmp/.ivy"  # Fix na problem z uprawnieniami w kontenerze
        },
        application_args=[
            "--mode", "HUMAN",
            "--output_path", output_path_human,
            "--db_host", "192.168.0.5"  # IP Twojego Maca lub nazwa serwisu bazy
        ],
        verbose=True
    )

    process_human_data