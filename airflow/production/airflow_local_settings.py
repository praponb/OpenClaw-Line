"""Cluster policy for the production Airflow: report every failed task to the
OpenClaw ingestion worker without editing DAG files.

Install on the production Airflow (scheduler, DAG processor and workers):
- copy ../plugins/openclaw_callback.py into its plugins folder;
- copy this file into its config folder ($AIRFLOW_HOME/config), which Airflow
  puts on sys.path before importing airflow_local_settings;
- set OPENCLAW_INGEST_URL and OPENCLAW_WEBHOOK_SECRET (see openclaw_callback.py).

Existing failure callbacks are kept: the notifier is appended, never replaces one.
If the notifier can't be imported, tasks are left unchanged. DAGs tagged
"no-openclaw" (e.g. housekeeping) are left alone too.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

OPT_OUT_TAG = "no-openclaw"

try:
    from openclaw_callback import notify_openclaw
except ImportError:  # plugin not installed: leave every task as it is
    notify_openclaw = None
    log.warning("openclaw_callback not importable; OpenClaw failure notifications are off")


def _is_notifier(callback: Any) -> bool:
    # Compared by name: Airflow's plugin manager imports openclaw_callback.py again as
    # a new module object, so a DAG's own notify_openclaw is a different function.
    return callback is notify_openclaw or (
        getattr(callback, "__module__", None) == notify_openclaw.__module__
        and getattr(callback, "__qualname__", None) == notify_openclaw.__qualname__
    )


def task_policy(task: Any) -> None:
    if notify_openclaw is None:
        return
    dag = getattr(task, "dag", None)
    if dag is not None and OPT_OUT_TAG in (getattr(dag, "tags", None) or ()):
        return
    current = task.on_failure_callback
    if not current:
        task.on_failure_callback = notify_openclaw
    elif isinstance(current, (list, tuple)):
        if not any(_is_notifier(c) for c in current):
            task.on_failure_callback = [*current, notify_openclaw]
    elif not _is_notifier(current):
        task.on_failure_callback = [current, notify_openclaw]
