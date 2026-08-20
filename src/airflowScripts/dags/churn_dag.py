from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 8, 1),
}

with DAG(
    dag_id="churn_prediction_pipeline",
    default_args=default_args,
    description="Daily customer value scoring and churn model training",
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["churn", "mlops"],
) as dag:

    # ETL must run first to produce fresh events from Kafka parquet
    spark_etl = BashOperator(
        task_id="spark_etl",
        bash_command=(
            "cd /opt/airflow/src/sparkScripts && "
            "python spark_etl.py "
            "--input-path /opt/airflow/DB/topic_dumps/user_events "
            "--output-csv /opt/airflow/DB/events_from_kafka.csv"
        ),
    )

    dvc_push = BashOperator(
        task_id="dvc_push",
        bash_command="cd /opt/airflow && dvc push DB/topic_dumps.dvc 2>&1 || true",
    )

    feature_churn = BashOperator(
        task_id="feature_engineering_churn",
        bash_command=(
            "python /opt/airflow/src/pythonScripts/model_churn/feature_churn.py "
            "--events-path /opt/airflow/DB/events_from_kafka.csv "
            "--output-path /opt/airflow/DB/user_value_features.csv"
        ),
    )

    train_churn = BashOperator(
        task_id="train_churn_model",
        bash_command="python /opt/airflow/src/pythonScripts/model_churn/train_churn_model.py",
    )

    spark_etl >> dvc_push
    spark_etl >> feature_churn >> train_churn
