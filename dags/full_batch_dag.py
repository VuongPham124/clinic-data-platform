from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator
)
from datetime import timedelta
from pathlib import Path
import sys

# ======================================================
# Make repo root importable
# ======================================================
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

# ======================================================
# Import pipeline run functions
# ======================================================
from pipelines.postgres_to_gcs import run as pg_to_raw_run
from pipelines.staging_to_silver import run as staging_to_silver_run

# ======================================================
# CONFIG
# ======================================================
PROJECT_ID = "wata-clinicdataplatform-gcp"
REGION = "us-central1"
DATAPROC_CLUSTER = "cluster-8649"

RAW_BUCKET = "amaz-raw"
STAGING_BUCKET = "amaz-staging"
QUARANTINE_BUCKET = "amaz-quarantine"

CONTRACT_PATH = (
    "/home/airflow/gcs/dags/contracts/"
    "staging_contract_snapshot_v1.json"
)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# ======================================================
# DAG
# ======================================================
with DAG(
    dag_id="full_snapshot_batch_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["snapshot", "full-batch"],
) as dag:

    # -------------------------------
    # Runtime params
    # -------------------------------
    LOAD_DATE = "{{ ds }}"
    RUN_ID = "prod_{{ ts_nodash }}"

    # ==================================================
    # 1. Postgres → GCS Raw
    # ==================================================
    pg_to_gcs_raw = PythonOperator(
        task_id="pg_to_gcs_raw_snapshot",
        python_callable=pg_to_raw_run,
        op_kwargs={
            "load_date": LOAD_DATE,
            "run_id": RUN_ID,
        },
    )

    # ==================================================
    # 2. Raw → Staging (Spark on Dataproc)
    # ==================================================
    raw_to_staging = DataprocSubmitJobOperator(
        task_id="raw_to_staging_snapshot",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": DATAPROC_CLUSTER
            },
            "pyspark_job": {
                "main_python_file_uri": (
                    "/home/airflow/gcs/dags/"
                    "pipelines/raw_to_staging.py"
                ),
                "properties": {
                    "spark.raw.bucket": RAW_BUCKET,
                    "spark.staging.bucket": STAGING_BUCKET,
                    "spark.quarantine.bucket": QUARANTINE_BUCKET,
                    "spark.normalize.load_date": LOAD_DATE,
                    "spark.normalize.run_id": RUN_ID,
                    "spark.normalize.contract_path": CONTRACT_PATH,
                },
            },
        },
    )

    # ==================================================
    # 3. Staging → BigQuery Silver
    # ==================================================
    staging_to_silver = PythonOperator(
        task_id="staging_to_silver_snapshot",
        python_callable=staging_to_silver_run,
        op_kwargs={
            "load_date": LOAD_DATE,
            "run_id": RUN_ID,
            "contract_path": CONTRACT_PATH,
        },
    )

    # ==================================================
    # DEPENDENCIES
    # ==================================================
    pg_to_gcs_raw >> raw_to_staging >> staging_to_silver
