import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Dataproc operator (Composer 2 / Airflow 2.x)
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="cdc_daily_to_silver",
    start_date=pendulum.datetime(2026, 2, 2, tz=VN_TZ),
    schedule="0 2 * * *",     # chạy 02:00 ICT mỗi ngày (tuỳ bạn)
    catchup=False,            # IMPORTANT: tránh backfill tự động
    max_active_runs=1,        # IMPORTANT: tránh watermark race giữa runs
    default_args={"retries": 1},
    tags=["cdc", "dataproc", "bq", "silver"],
) as dag:

    # 1) SRC_DATE: daily CDC thường lấy ds-1
    SRC_DATE = "{{ macros.ds_add(ds, 0) }}"
    
    # 2) RUN_ID: Airflow là source of truth
    RUN_ID = (
        "prod_d_{{ ts_nodash }}_ict_"
        "{{ 'sched' if dag_run and dag_run.run_type == 'scheduled' "
        "else ('backfill' if dag_run and dag_run.run_type == 'backfill' else 'manual') }}"
    )

    # --------- (A) Dataproc Spark normalize -> GCS staging ----------
    # Bạn thay các biến dưới cho đúng dự án
    PROJECT_ID="wata-clinicdataplatform-gcp"
    REGION = "us-central1"
    CLUSTER_NAME = "cluster-8649"
    # PROJECT_ID="wata-clinicdataplatform-gcp"
    # LOCATION="us-central1"
    # STAGING_BUCKET="amaz-staging"

    # main.py & dependencies nên đặt trên GCS (py_files) hoặc image, tuỳ cách bạn deploy
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
        f"{DAGS_GCS}/pipelines/cdc/normalize/main.py"
        if DAGS_GCS else None
    )
    DEP_PY_FILES = (
        [
            f"{DAGS_GCS}/pipelines/cdc/normalize/cdc_engine.py",
            f"{DAGS_GCS}/pipelines/cdc/normalize/cdc_normalizer.py",
            f"{DAGS_GCS}/pipelines/cdc/normalize/contract_loader.py",
            f"{DAGS_GCS}/pipelines/cdc/normalize/io_utils.py",
            f"{DAGS_GCS}/pipelines/cdc/normalize/schema_applier.py",
            f"{DAGS_GCS}/pipelines/cdc/normalize/table_processor.py",
        ]
        if DAGS_GCS else []
    )
    CONTRACT_GCS = (
        f"{DAGS_GCS}/contract/cdc_staging_contract_v1.json"
        if DAGS_GCS else None
    )

    RAW_BUCKET = "gs://amaz-raw"
    STAGING_BUCKET = "gs://amaz-staging"
    QUAR_BUCKET = "gs://amaz-quarantine"

    spark_normalize = DataprocSubmitJobOperator(
        task_id="spark_normalize_to_gcs_staging",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": MAIN_PY_GCS,
                "python_file_uris": DEP_PY_FILES,

                # --- ADDED: include spark-avro jar so Spark can read AVRO inputs ---
                # Equivalent to: gcloud dataproc jobs submit pyspark ... --jars=...
                "jar_file_uris": ["gs://amaz-utils/spark-avro_2.12-3.5.0.jar"],
                # ---------------------------------------------------------------

                # --- ADDED: Spark properties (same as --properties) ---
                "properties": {
                    "spark.sql.debug.maxToStringFields": "2000",
                },
                # -----------------------------------------------------

                "args": [
                    "--tables", "ALL",
                    "--contract-path", CONTRACT_GCS,
                    "--source-date-local", SRC_DATE,
                    "--run-id", RUN_ID,
                    "--env", "prod",
                    "--cadence", "d",
                    "--trigger", "{{ 'sched' if dag_run and dag_run.run_type == 'scheduled' else 'manual' }}",
                    "--tz-tag", "ict",
                    "--tz-name", "Asia/Ho_Chi_Minh",
                    "--raw-bucket", RAW_BUCKET,
                    "--staging-bucket", STAGING_BUCKET,
                    "--quarantine-bucket", QUAR_BUCKET,
                ],
            },
        },
    )


    # --------- (B) Merge: GCS staging -> BQ stage -> MERGE silver ----------
    # Script bash & contract nên nằm trong DAGs folder hoặc GCS sync về worker
    # Ví dụ: dags/pipelines/cdc/merge/run_query_merge_v2.updated3.sh và dags/contract/cdc_staging_contract_v1.json
    merge_to_silver = BashOperator(
        task_id="merge_to_silver",
        bash_command=r"""
        set -euo pipefail
        set -x

        # --- Resolve DAGS folder at runtime (Composer variants)
        DAGS_DIR="${DAGS_FOLDER:-${AIRFLOW__CORE__DAGS_FOLDER:-}}"
        if [ -z "${DAGS_DIR}" ]; then
        for p in "/home/airflow/gcs/dags" "/opt/airflow/dags" "/usr/local/airflow/dags" "/etc/airflow/dags"; do
            if [ -d "$p" ]; then DAGS_DIR="$p"; break; fi
        done
        fi

        echo "[INFO] DAGS_DIR=${DAGS_DIR}"
        if [ -z "${DAGS_DIR}" ] || [ ! -d "${DAGS_DIR}" ]; then
        echo "[ERROR] Could not resolve DAGS folder"
        env | sort | sed -n '1,200p'
        exit 2
        fi
        # ----------------------------------------------------

        # --- MUST: define SCRIPT/CONTRACT before using them (avoid unbound variable)
        SCRIPT="${DAGS_DIR}/pipelines/cdc/merge/run_query_merge_v2.updated3.sh"
        CONTRACT_LOCAL="${DAGS_DIR}/contract/cdc_merge_contract_v1.json"
        # ----------------------------------------------------

        echo "[INFO] SCRIPT=${SCRIPT}"
        echo "[INFO] CONTRACT_LOCAL=${CONTRACT_LOCAL}"

        # Debug: verify files exist
        ls -lah "${DAGS_DIR}" || true
        ls -lah "${DAGS_DIR}/pipelines/cdc/merge" || true
        ls -lah "${DAGS_DIR}/contract" || true

        # Fail fast with clear message if missing
        if [ ! -f "${SCRIPT}" ]; then
        echo "[ERROR] Merge script not found at ${SCRIPT}"
        exit 3
        fi
        if [ ! -f "${CONTRACT_LOCAL}" ]; then
        echo "[ERROR] Contract not found at ${CONTRACT_LOCAL}"
        exit 4
        fi

        chmod +x "${SCRIPT}"

        SRC_DATE="{{ macros.ds_add(ds, 0) }}"
        
        RUN_ID="prod_d_{{ ts_nodash }}_ict_{{ 'sched' if dag_run and dag_run.run_type == 'scheduled' else ('backfill' if dag_run and dag_run.run_type == 'backfill' else 'manual') }}"
        RUN_ID="$(echo "${RUN_ID}" | tr '[:upper:]' '[:lower:]')"  # ADDED: match Spark sanitize

        # ONLY_TABLE="public_clinic_bookings"
        ONLY_TABLE=""

        # Optional env overrides used by updated script
        export PROJECT_ID="wata-clinicdataplatform-gcp"
        export LOCATION="us-central1"
        export STAGING_BUCKET="amaz-staging"

        echo "=== DEBUG: bq presence ==="
        which bq || true
        bq version || true

        echo "=== RUN MERGE SCRIPT ==="
        "${SCRIPT}" "${SRC_DATE}" "${RUN_ID}" "${CONTRACT_LOCAL}" "${ONLY_TABLE}" 2>&1 | tee /tmp/merge.log
        """,
    )

    spark_normalize >> merge_to_silver
