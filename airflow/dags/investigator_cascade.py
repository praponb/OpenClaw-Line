"""Live test for a big cascade (manual only; `make eval-live` runs it).

load_orders fails. Eight check tasks run anyway (trigger_rule all_done) and fail too,
each firing its own failure callback; twelve build tasks become upstream_failed.
The worker must turn the nine callbacks into one investigation of load_orders, and
the report must list the other tasks as blast radius.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task
from openclaw_callback import notify_openclaw

REGIONS = ["th", "sg", "my", "id", "vn", "ph", "jp", "au"]
MARTS = ["orders", "customers", "refunds", "payouts", "fx", "tax"]


@dag(
    dag_id="investigator_cascade",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={"owner": "data-eng", "retries": 0, "on_failure_callback": notify_openclaw},
    tags=["test", "investigator"],
)
def investigator_cascade():
    @task
    def load_orders() -> int:
        raise ConnectionError("source database orders-replica refused the connection (port 5432)")

    loaded = load_orders()
    for region in REGIONS:

        @task(task_id=f"check_orders_{region}", trigger_rule="all_done")
        def check(ti=None, region=region) -> None:
            if not ti.xcom_pull(task_ids="load_orders"):
                raise ValueError(f"no orders loaded; refusing to publish {region} order counts")

        loaded >> check()
    for mart in MARTS:

        @task(task_id=f"build_{mart}_staging")
        def staging(mart=mart) -> None:
            print(f"dbt run --select stg_{mart}")

        @task(task_id=f"build_{mart}_mart")
        def mart_task(mart=mart) -> None:
            print(f"dbt run --select fct_{mart}")

        loaded >> staging() >> mart_task()


investigator_cascade()
