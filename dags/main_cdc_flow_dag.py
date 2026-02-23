import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="main_cdc_flow",
    start_date=pendulum.datetime(2026, 2, 14, tz=VN_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["orchestration", "cdc"],
) as dag:

    trigger_cdc_silver = TriggerDagRunOperator(
        task_id="trigger_cdc_daily_to_silver",
        trigger_dag_id="cdc_daily_to_silver",
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_silver_curated_dq = TriggerDagRunOperator(
        task_id="trigger_silver_to_silver_curated_dq",
        trigger_dag_id="silver_to_silver_curated_dq_v2_3",
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_gen_master = TriggerDagRunOperator(
        task_id="trigger_gen_master_parallel",
        trigger_dag_id="gen_master_parallel",
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_platinum_gold",
        trigger_dag_id="dbt_platinum_gold_build_test",
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_cdc_silver >> trigger_silver_curated_dq >> trigger_gen_master >> trigger_dbt
