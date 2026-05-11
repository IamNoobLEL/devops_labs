from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

HOMEWORK = [
    {"position": 1, "subject": "DevOps", "hours": 2},
    {"position": 2, "subject": "Питон", "hours": 1},
    {"position": 3, "subject": "Математика", "hours": 3},
    {"position": 4, "subject": "Английский", "hours": 1},
]

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aboba",
    )
    parser.add_argument("--report-date", required=True)
    parser.add_argument("--output-dir", default="/opt/airflow/output")
    parser.add_argument("--log-dir", default="/opt/airflow/spark-logs")
    return parser.parse_args()

def configure_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("homework_panic_meter")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    file_handler = logging.FileHandler(
        log_dir / "homework_panic_meter.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger

def choose_status(total_hours: int) -> str:
    if total_hours <= 3:
        return "спокойно"
    if total_hours <= 6:
        return "легкая паника"
    return "полная паника"

def build_report(
    report_date: str,
    homework: list[dict[str, int | str]],
    total_hours: int,
    status: str,
) -> str:
    homework_lines = [
        f"- {item['subject']}: {item['hours']} ч"
        for item in homework
    ]

    return "\n".join(
        [
            f"# Измеритель паники по домашке за {report_date}",
            "",
            "## Домашние задания",
            *homework_lines,
            "",
            f"Всего часов: {total_hours}",
            f"Статус: {status}",
            "",
        ]
    )

def main() -> None:
    args = parse_args()
    logger = configure_logging(Path(args.log_dir))
    logger.info("Запускаю Spark-отчет за дату=%s", args.report_date)

    spark = (
        SparkSession.builder.appName("homework_panic_meter")
        .master("spark://spark-master:7077")
        .config("spark.ui.prometheus.enabled", "true")
        .config("spark.executor.processTreeMetrics.enabled", "true")
        .config("spark.metrics.namespace", "homework_panic_meter")
        .getOrCreate()
    )

    try:
        homework_df = spark.createDataFrame(HOMEWORK)
        total_hours = homework_df.agg(
            spark_sum(col("hours")).alias("total_hours")
        ).collect()[0]["total_hours"]
        ordered_homework = [
            row.asDict()
            for row in homework_df.orderBy("position").collect()
        ]
        status = choose_status(int(total_hours))

        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / f"homework_panic_{args.report_date}.md"
        report_path.write_text(
            build_report(
                report_date=args.report_date,
                homework=ordered_homework,
                total_hours=int(total_hours),
                status=status,
            ),
            encoding="utf-8",
        )
        logger.info(
            "Отчет записан: путь=%s всего_часов=%s статус=%s",
            report_path,
            total_hours,
            status,
        )
    finally:
        logger.info("Останавливаю Spark-сессию")
        spark.stop()

if __name__ == "__main__":
    main()
