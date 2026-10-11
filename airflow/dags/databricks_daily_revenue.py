"""Real pipeline: triggers the Databricks job daily_revenue_lakehouse (databricks/ bundle).

The job runs on Databricks serverless compute; this DAG only starts it and waits.
If any Databricks task fails, the Airflow task fails and notify_openclaw sends the
Databricks run id along, so the investigator can read the run's error directly.

Connection databricks_default comes from AIRFLOW_CONN_DATABRICKS_DEFAULT (sp-airflow-runner,
CAN_MANAGE_RUN on the job only). DATABRICKS_JOB_ID is the deployed job's id.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import Param, dag
from openclaw_callback import notify_openclaw

FAULTS = ["none", "schema_drift", "null_amounts", "empty"]


@dag(
    dag_id="databricks_daily_revenue",
    # Manual for now (Trigger DAG in the UI); switch to e.g. "0 6 * * *" with
    # timezone Asia/Bangkok once the pipeline should run daily.
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={"owner": "data-eng", "retries": 0, "on_failure_callback": notify_openclaw},
    params={
        "inject_fault": Param("none", enum=FAULTS, description="Simulate upstream breakage for incident tests."),
    },
    tags=["finance", "stripe", "databricks"],
)
def databricks_daily_revenue():
    DatabricksRunNowOperator(
        task_id="run_daily_revenue_lakehouse",
        databricks_conn_id="databricks_default",
        job_id=os.environ.get("DATABRICKS_JOB_ID", ""),
        job_parameters={
            # Bangkok calendar date (UTC+7, no DST). Manual runs in Airflow 3 may have no
            # logical date, and run_after is a plain datetime (no pendulum in_timezone).
            "run_date": "{{ ((dag_run.logical_date or dag_run.run_after) + macros.timedelta(hours=7)).strftime('%Y-%m-%d') }}",
            "inject_fault": "{{ params.inject_fault }}",
        },
        wait_for_termination=True,
        polling_period_seconds=15,
    )


databricks_daily_revenue()
