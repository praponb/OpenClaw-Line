"""Airflow failure callback that hands failed tasks to the OpenClaw ingestion worker.

Usage in a DAG::

    from openclaw_callback import notify_openclaw

    default_args = {"on_failure_callback": notify_openclaw}

Environment:
    OPENCLAW_INGEST_URL      e.g. http://ingestion-worker:8080/v1/airflow/failure
    OPENCLAW_WEBHOOK_SECRET  shared HMAC secret (same as the worker's WEBHOOK_SECRET)

The callback never raises: a broken notifier must not mask the original task failure.
Only stdlib is used so it works in any Airflow image.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
import urllib.request
from typing import Any

log = logging.getLogger(__name__)

TIMEOUT_S = 5
MAX_EXCEPTION_CHARS = 2000


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def build_payload(context: dict[str, Any]) -> dict[str, Any]:
    ti = context.get("ti") or context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")
    return {
        "dag_id": getattr(ti, "dag_id", None) or getattr(context.get("dag"), "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": getattr(dag_run, "run_id", None) or context.get("run_id"),
        "try_number": getattr(ti, "try_number", None),
        "logical_date": _iso(context.get("logical_date") or getattr(dag_run, "logical_date", None)),
        "log_url": getattr(ti, "log_url", None),
        "exception": (f"{type(exception).__name__}: {exception}"[:MAX_EXCEPTION_CHARS] if exception else None),
    }


def sign(secret: str, timestamp: str, body: bytes) -> str:
    digest = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def notify_openclaw(context: dict[str, Any]) -> None:
    """`on_failure_callback` entry point."""
    try:
        url = os.environ.get("OPENCLAW_INGEST_URL")
        secret = os.environ.get("OPENCLAW_WEBHOOK_SECRET")
        if not url or not secret:
            log.warning("OpenClaw notifier disabled: OPENCLAW_INGEST_URL / OPENCLAW_WEBHOOK_SECRET not set")
            return
        payload = build_payload(context)
        body = json.dumps(payload, default=str).encode()
        timestamp = str(int(time.time()))
        request = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-OpenClaw-Timestamp": timestamp,
                "X-OpenClaw-Signature": sign(secret, timestamp, body),
            },
        )
        with urllib.request.urlopen(request, timeout=TIMEOUT_S) as response:  # noqa: S310 (URL from trusted env)
            log.info("OpenClaw notified for %s/%s (HTTP %s)", payload["dag_id"], payload["task_id"], response.status)
    except Exception:  # noqa: BLE001 - must never fail the task's failure handling
        log.exception("Failed to notify OpenClaw ingestion worker")
