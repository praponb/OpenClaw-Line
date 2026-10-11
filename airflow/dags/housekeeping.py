"""Retention for Airflow itself (docs/data-residency.md, recommendation 5).

Daily at 03:00 Bangkok: delete task log files older than AIRFLOW_LOG_RETENTION_DAYS
(default 30) and metadata rows (DAG runs, task instances, XComs, ...) older than
AIRFLOW_METADATA_RETENTION_DAYS (default 90). Local only: no Databricks, model or
LINE cost. No OpenClaw callback: a housekeeping failure shows in the Airflow UI
instead of paging LINE.
"""

from __future__ import annotations

import os
from datetime import datetime

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

LOG_DAYS = int(os.environ.get("AIRFLOW_LOG_RETENTION_DAYS", "30"))
META_DAYS = int(os.environ.get("AIRFLOW_METADATA_RETENTION_DAYS", "90"))


@dag(
    dag_id="housekeeping",
    schedule="0 3 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=pendulum.timezone("Asia/Bangkok")),
    catchup=False,
    default_args={"owner": "platform", "retries": 0},
    tags=["maintenance", "retention"],
)
def housekeeping():
    BashOperator(
        task_id="delete_old_task_logs",
        bash_command=(
            f'find "${{AIRFLOW_HOME:-/opt/airflow}}/logs" -type f -mtime +{LOG_DAYS} -print -delete | wc -l'
            f' | xargs -I{{}} echo "deleted {{}} log files older than {LOG_DAYS} days";'
            ' find "${AIRFLOW_HOME:-/opt/airflow}/logs" -mindepth 1 -type d -empty -delete'
        ),
    )
    BashOperator(
        task_id="clean_old_metadata",
        bash_command=(
            f'airflow db clean --clean-before-timestamp "$(date -u -d "-{META_DAYS} days" +%Y-%m-%dT%H:%M:%S+00:00)"'
            " --skip-archive --yes"
        ),
    )


housekeeping()
