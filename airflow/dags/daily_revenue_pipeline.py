"""Demo DAG reproducing the spec's Stripe KeyError incident.

extract_stripe_transactions fails on a charge.refunded event. Everything
downstream becomes upstream_failed, except validate_extract_counts
(trigger_rule=all_done), which also fails and fires a second callback. The
ingestion worker must collapse both callbacks into one investigation.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task
from openclaw_callback import notify_openclaw

default_args = {
    "owner": "data-eng",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_openclaw,
}


@dag(
    dag_id="daily_revenue_pipeline",
    # Manual only (`make demo-trigger`): every run fails on purpose and costs a
    # model call plus a LINE message, so the demo never runs on a timer.
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args=default_args,
    tags=["finance", "stripe", "demo"],
)
def daily_revenue_pipeline():
    @task
    def extract_stripe_transactions() -> int:
        from stripe_ingest.extract_stripe import extract_transactions

        return extract_transactions()

    @task(trigger_rule="all_done")
    def validate_extract_counts(ti=None) -> None:
        row_count = ti.xcom_pull(task_ids="extract_stripe_transactions")
        if not row_count:
            raise ValueError("extract produced no rows; refusing to publish empty revenue snapshot")

    @task
    def stg_stripe_transactions() -> None:
        print("dbt run --select stg_stripe_transactions")

    @task
    def fct_daily_revenue() -> None:
        print("dbt run --select fct_daily_revenue")

    @task
    def finance_reconciliation() -> None:
        print("dbt run --select finance_reconciliation")

    extracted = extract_stripe_transactions()
    extracted >> validate_extract_counts()
    extracted >> stg_stripe_transactions() >> fct_daily_revenue() >> finance_reconciliation()


daily_revenue_pipeline()
