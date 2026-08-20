# dags/apriori_pipeline.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "samuel",
    "depends_on_past": False,
    "retries": 1,
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="mlops_pipeline",
    default_args=default_args,
    description="Recommendation (Apriori) pipeline",
    schedule_interval="0 2 * * *",
    catchup=False,
) as dag:

    # -----------------------------------------------------------------------
    # Step 0: Spark batch ETL — parquet topic_dumps → events CSV
    # -----------------------------------------------------------------------
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

    # -----------------------------------------------------------------------
    # Branch A: Recommendation (Apriori)
    # -----------------------------------------------------------------------
    feature_task = BashOperator(
        task_id="feature_engineering",
        bash_command=(
            "python /opt/airflow/src/pythonScripts/feature.py "
            "--events-path /opt/airflow/DB/events_from_kafka.csv "
            "--output-path /opt/airflow/DB/extracted_features.csv"
        ),
    )

    train_apriori = BashOperator(
        task_id="train_apriori",
        bash_command="/usr/local/bin/python /opt/airflow/src/pythonScripts/model_apriori/train_model_apriori.py",
    )

    feature_task >> train_apriori
    spark_etl >> dvc_push
    spark_etl >> feature_task

