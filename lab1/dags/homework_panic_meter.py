from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

@dag(
    dag_id="homework_panic_meter",
    description="Aboba",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lab1", "домашка", "паника"],
)
def homework_panic_meter() -> None:
    @task
    def load_homework() -> list[dict[str, int | str]]:
        return [
            {"subject": "DevOps", "hours": 2},
            {"subject": "Питон", "hours": 1},
            {"subject": "Математика", "hours": 3},
            {"subject": "Английский", "hours": 1},
        ]

    @task
    def count_total_hours(homework: list[dict[str, int | str]]) -> int:
        return sum(int(item["hours"]) for item in homework)

    @task
    def choose_status(total_hours: int) -> str:
        if total_hours <= 3:
            return "спокойно"
        if total_hours <= 6:
            return "легкая паника"
        return "полная паника"

    @task
    def write_report(
        homework: list[dict[str, int | str]],
        total_hours: int,
        status: str,
    ) -> str:
        context = get_current_context()
        ds = context["ds"]
        report_dir = Path("/opt/airflow/output")
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"homework_panic_{ds}.md"

        homework_lines = [
            f"- {item['subject']}: {item['hours']} ч"
            for item in homework
        ]

        report_path.write_text(
            "\n".join(
                [
                    f"# Измеритель паники по домашке за {ds}",
                    "",
                    "## Домашние задания",
                    *homework_lines,
                    "",
                    f"Всего часов: {total_hours}",
                    f"Статус: {status}",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        return str(report_path)

    @task
    def validate_report(report_path: str) -> None:
        if not Path(report_path).exists():
            raise FileNotFoundError(f"Отчет не был создан: {report_path}")

    homework = load_homework()
    total_hours = count_total_hours(homework)
    status = choose_status(total_hours)
    report_path = write_report(homework, total_hours, status)
    validate_report(report_path)


homework_panic_meter()
