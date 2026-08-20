# dags/daily_analytics_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_analytics",
    default_args=default_args,
    schedule_interval="0 2 * * *",     # runs daily at 02:00
    start_date=datetime(2026, 8, 1),
    catchup=False,                    # don't backfill every historical day on first deploy
    tags=["analytics", "spark"],
) as dag:

    run_analytics = BashOperator(
        task_id="run_daily_analytics",
        bash_command=(
            "cd /opt/airflow/src/sparkScripts && "
            "python run_daily_analytics.py "
            "--run-date {{ ds }} "
            "--user-events-path /DB/topic_dumps/user_events "
            "--recommendations-path /DB/topic_dumps/recommendation_events "
            "--notifications-path /DB/topic_dumps/notification_events "
            "--output-path /DB/analytics"
        ),
    )