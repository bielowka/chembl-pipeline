from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATASET_VARIANTS = [
    {
        "id": "human_egfr_meta",
        "target": "EGFR",
        "org_scope": "HUMAN",
        "feat_mode": "WITH_METADATA"
    },
    {
        "id": "human_egfr_graph",
        "target": "EGFR",
        "org_scope": "HUMAN",
        "feat_mode": "GRAPH_ONLY"
    },
    {
        "id": "all_egfr_meta",
        "target": "EGFR",
        "org_scope": "ALL",
        "feat_mode": "WITH_METADATA"
    },
    {
        "id": "all_egfr_graph",
        "target": "EGFR",
        "org_scope": "ALL",
        "feat_mode": "GRAPH_ONLY"
    },
    {
        "id": "human_meta",
        "target": "ALL",
        "org_scope": "HUMAN",
        "feat_mode": "WITH_METADATA"
    },
    {
        "id": "human_graph",
        "target": "ALL",
        "org_scope": "HUMAN",
        "feat_mode": "GRAPH_ONLY"
    },
    {
        "id": "all_meta",
        "target": "ALL",
        "org_scope": "ALL",
        "feat_mode": "WITH_METADATA"
    },
    {
        "id": "all_graph",
        "target": "ALL",
        "org_scope": "ALL",
        "feat_mode": "GRAPH_ONLY"
    }
]

with DAG(
        'chembl_processing_pipeline_with_datasets',
        default_args=default_args,
        description='Pipeline przetwarzający dane ChEMBL na Sparku, prodokujacy datasety',
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['spark', 'chembl', 'eda'],
) as dag:
    script_path = "/opt/airflow/data/pipeline.py"

    base_output_dir = "file:///opt/airflow/data/runs/{{ ds }}/{{ ts_nodash }}"

    for config in DATASET_VARIANTS:
        physical_output_path = f"{base_output_dir}/{config['id']}.parquet"

        dataset_uri = f"chembl://{config['id']}"
        dataset_obj = Dataset(dataset_uri)

        SparkSubmitOperator(
            task_id=f"process_{config['id']}",
            conn_id='spark_default',
            spark_binary="spark-submit",
            application=script_path,

            total_executor_cores='1',
            executor_cores='1',
            executor_memory='1g',
            driver_memory='1g',

            name=f"chembl_pipe_{config['id']}",
            packages='org.postgresql:postgresql:42.6.0',

            conf={
                "spark.master": "spark://spark-master:7077",
                "spark.jars.ivy": "/tmp/.ivy",
                "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
                "spark.executor.extraJavaOptions": "-Duser.timezone=UTC"
            },
            application_args=[
                "--db_host", "host.docker.internal",
                "--target_name", config['target'],
                "--organism_scope", config['org_scope'],
                "--feature_mode", config['feat_mode'],
                "--output_path", physical_output_path
            ],

            outlets=[dataset_obj],

            verbose=True
        )