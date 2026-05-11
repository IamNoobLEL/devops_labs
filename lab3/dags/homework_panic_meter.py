from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="homework_panic_meter",
    description="Aboba",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lab2", "spark", "домашка", "паника"],
) as dag:
    SparkSubmitOperator(
        task_id="run_homework_panic_meter",
        application="/opt/airflow/spark/homework_panic_meter.py",
        name="homework_panic_meter",
        conn_id="spark_local",
        application_args=[
            "--report-date",
            "{{ ds }}",
            "--output-dir",
            "/opt/airflow/output",
        ],
        verbose=True,
    )
