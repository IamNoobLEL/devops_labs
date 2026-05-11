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
    tags=["lab4", "spark", "grafana", "loki", "prometheus"],
) as dag:
    SparkSubmitOperator(
        task_id="run_homework_panic_meter",
        application="/opt/airflow/spark/homework_panic_meter.py",
        name="homework_panic_meter",
        conn_id="spark_local",
        conf={
            "spark.ui.prometheus.enabled": "true",
            "spark.executor.processTreeMetrics.enabled": "true",
            "spark.metrics.namespace": "homework_panic_meter",
        },
        application_args=[
            "--report-date",
            "{{ ds }}",
            "--output-dir",
            "/opt/airflow/output",
            "--log-dir",
            "/opt/airflow/spark-logs",
        ],
        verbose=True,
    )
