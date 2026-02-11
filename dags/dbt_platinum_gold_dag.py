import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="dbt_platinum_gold_build_test",
    start_date=pendulum.datetime(2026, 2, 11, tz=VN_TZ),
    schedule=None,  # set cron here if you want scheduled runs
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["dbt", "platinum", "gold"],
) as dag:

    # Resolve Composer DAGS folder for different runtime layouts.
    DBT_BASH_PREFIX = r"""
    set -euo pipefail

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

    echo "[INFO] DBT_DIR=${DBT_DIR}"
    dbt --version
    """

    COMMON_DBT_FLAGS = "--project-dir . --profiles-dir ."

    # Keep profile vars explicit so Composer can override at environment level.
    COMMON_DBT_ENV = {
        "DBT_GCP_PROJECT": os.environ.get("DBT_GCP_PROJECT", "wata-clinicdataplatform-gcp"),
        "DBT_BQ_LOCATION": os.environ.get("DBT_BQ_LOCATION", "us-central1"),
        "DBT_DATASET_PLATINUM": os.environ.get("DBT_DATASET_PLATINUM", "platinum"),
        "DBT_DATASET_GOLD": os.environ.get("DBT_DATASET_GOLD", "gold"),
        "DBT_THREADS": os.environ.get("DBT_THREADS", "4"),
    }

    dbt_test_platinum = BashOperator(
        task_id="dbt_test_platinum",
        env={**COMMON_DBT_ENV, "DBT_TARGET": "platinum"},
        bash_command=(
            DBT_BASH_PREFIX
            + f"""
            dbt test --target platinum --select path:models/platinum {COMMON_DBT_FLAGS}
            """
        ),
    )

    dbt_run_platinum = BashOperator(
        task_id="dbt_run_platinum",
        env={**COMMON_DBT_ENV, "DBT_TARGET": "platinum"},
        bash_command=(
            DBT_BASH_PREFIX
            + f"""
            dbt run --target platinum --select path:models/platinum {COMMON_DBT_FLAGS}
            """
        ),
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        env={**COMMON_DBT_ENV, "DBT_TARGET": "gold"},
        bash_command=(
            DBT_BASH_PREFIX
            + f"""
            dbt run --target gold --select path:models/gold {COMMON_DBT_FLAGS}
            """
        ),
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        env={**COMMON_DBT_ENV, "DBT_TARGET": "gold"},
        bash_command=(
            DBT_BASH_PREFIX
            + f"""
            dbt test --target gold --select path:models/gold {COMMON_DBT_FLAGS}
            """
        ),
    )

    dbt_test_platinum >> dbt_run_platinum >> dbt_run_gold >> dbt_test_gold
