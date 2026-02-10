import os
import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="silver_to_silver_curated_dq_v2_3",
    start_date=pendulum.datetime(2026, 2, 9, tz=VN_TZ),
    schedule="0 3 * * *",  # chạy 03:00 ICT mỗi ngày (sau DAG silver)
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["silver", "curated", "dq", "dataproc"],
) as dag:

    PROJECT_ID = "wata-clinicdataplatform-gcp"
    REGION = "us-central1"
    CLUSTER_NAME = "cluster-8649"

    # Resolve Composer bucket for GCS paths
    COMPOSER_BUCKET = (
        os.environ.get("COMPOSER_BUCKET")
        or os.environ.get("COMPOSER_GCS_BUCKET")
        or os.environ.get("DAGS_BUCKET")
        or os.environ.get("GCS_BUCKET")
    )
    DAGS_GCS = f"gs://{COMPOSER_BUCKET}/dags" if COMPOSER_BUCKET else None
    if not DAGS_GCS:
        print("[WARN] COMPOSER_BUCKET not set; GCS paths will be invalid for Dataproc job.")

    MAIN_PY_GCS = (
        f"{DAGS_GCS}/pipelines/curated/silver_to_silver_curated_dq_v2_3.py"
        if DAGS_GCS else None
    )

    # Inputs stored alongside dags in the same bucket
    CONTRACT_GCS = (
        f"{DAGS_GCS}/contract/cdc_merge_contract_v1.json"
        if DAGS_GCS else None
    )
    RULES_GCS = (
        f"{DAGS_GCS}/contract/silver_curated_business_rules_v1.json"
        if DAGS_GCS else None
    )
    # Localized filenames (Dataproc will download via file_uris)
    CONTRACT_LOCAL = "cdc_merge_contract_v1.json"
    RULES_LOCAL = "silver_curated_business_rules_v1.json"

    TEMP_GCS_BUCKET = os.environ.get("TEMP_GCS_BUCKET") or "amaz-staging"
    if TEMP_GCS_BUCKET.startswith("gs://"):
        TEMP_GCS_BUCKET = TEMP_GCS_BUCKET.replace("gs://", "", 1)

    # Datasets
    SILVER_DATASET = "silver"
    CURATED_DATASET = "silver_curated"
    QUARANTINE_DATASET = "silver_curated_quarantine"
    DQ_DATASET = "silver_curated_dq"

    spark_curated_dq = DataprocSubmitJobOperator(
        task_id="spark_silver_to_curated_dq",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": MAIN_PY_GCS,
                "file_uris": [CONTRACT_GCS, RULES_GCS] if DAGS_GCS else [],
                "properties": {
                    "spark.sql.debug.maxToStringFields": "2000",
                },
                "args": [
                    "--project", PROJECT_ID,
                    "--silver_dataset", SILVER_DATASET,
                    "--curated_dataset", CURATED_DATASET,
                    "--quarantine_dataset", QUARANTINE_DATASET,
                    "--dq_dataset", DQ_DATASET,
                    "--contract_path", CONTRACT_LOCAL,
                    "--rules_json_path", RULES_LOCAL,
                    "--temp_gcs_bucket", TEMP_GCS_BUCKET,
                    "--tables", "clinic_doctors, clinic_patients, clinic_rooms", #can set to ALL
                    "--exclude_deleted", "true",
                ],
            },
        },
    )
