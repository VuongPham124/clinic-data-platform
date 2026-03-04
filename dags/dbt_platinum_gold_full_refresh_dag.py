import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="dbt_platinum_gold_full_refresh",
    start_date=pendulum.datetime(2026, 2, 11, tz=VN_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["dbt", "platinum", "gold", "full-refresh"],
) as dag:

    DBT_BASH_PREFIX = r"""
    set -euo pipefail

    HOME_DIR="${HOME:-/home/airflow}"
    export PATH="$PATH:${HOME_DIR}/.local/bin"

    DAGS_DIR="${DAGS_FOLDER:-${AIRFLOW__CORE__DAGS_FOLDER:-}}"
    if [ -z "${DAGS_DIR}" ]; then
      for p in "/home/airflow/gcs/dags" "/opt/airflow/dags" "/usr/local/airflow/dags" "/etc/airflow/dags"; do
        if [ -d "$p" ]; then DAGS_DIR="$p"; break; fi
      done
    fi

    if [ -z "${DAGS_DIR}" ] || [ ! -d "${DAGS_DIR}" ]; then
      echo "[ERROR] Cannot resolve DAGS folder"
      env | sort | sed -n '1,120p'
      exit 2
    fi

    DBT_DIR="${DAGS_DIR}/dbt"
    if [ ! -f "${DBT_DIR}/dbt_project.yml" ]; then
      echo "[ERROR] Missing dbt_project.yml at ${DBT_DIR}"
      exit 3
    fi

    cd "${DBT_DIR}"

    if command -v dbt >/dev/null 2>&1; then
      DBT_CMD="dbt"
    elif python3 -c "import dbt" >/dev/null 2>&1; then
      DBT_CMD="python3 -m dbt"
    elif python -c "import dbt" >/dev/null 2>&1; then
      DBT_CMD="python -m dbt"
    else
      echo "[ERROR] dbt is not installed on Composer worker. Install dbt-core and dbt-bigquery in Composer PyPI packages."
      exit 127
    fi

    echo "[INFO] DBT_DIR=${DBT_DIR}"
    echo "[INFO] DBT_CMD=${DBT_CMD}"
    """

    COMMON_DBT_FLAGS = "--no-version-check --project-dir . --profiles-dir ."

    COMMON_DBT_ENV = {
        "DBT_GCP_PROJECT": os.environ.get("DBT_GCP_PROJECT", "wata-clinicdataplatform-gcp"),
        "DBT_BQ_LOCATION": os.environ.get("DBT_BQ_LOCATION", "us-central1"),
        "DBT_DATASET_PLATINUM": os.environ.get("DBT_DATASET_PLATINUM", "platinum"),
        "DBT_DATASET_GOLD": os.environ.get("DBT_DATASET_GOLD", "gold"),
        "DBT_THREADS": os.environ.get("DBT_THREADS", "4"),
    }

    dbt_full_refresh_platinum = BashOperator(
        task_id="dbt_full_refresh_platinum",
        append_env=True,
        env={**COMMON_DBT_ENV, "DBT_TARGET": "platinum"},
        bash_command=(
            DBT_BASH_PREFIX
            + f"""
            ${{DBT_CMD}} build --full-refresh --target platinum --select path:models/platinum {COMMON_DBT_FLAGS}
            """
        ),
    )

    dbt_full_refresh_gold = BashOperator(
        task_id="dbt_full_refresh_gold",
        append_env=True,
        env={**COMMON_DBT_ENV, "DBT_TARGET": "gold"},
        bash_command=(
            DBT_BASH_PREFIX
            + f"""
            ${{DBT_CMD}} build --full-refresh --target gold --select path:models/gold {COMMON_DBT_FLAGS}
            """
        ),
    )

    dbt_full_refresh_platinum >> dbt_full_refresh_gold
