"""Extract Stripe balance events into staging rows.

Demo note: the latest commit switched to direct dictionary access for
`payment_intent_id`, which raises KeyError for refund events that do not
carry that field at the top level.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_EVENTS_PATH = "/opt/airflow/data/stripe_events.json"
WAREHOUSE_DSN = os.environ.get("WAREHOUSE_DSN", "postgres://etl_user:hunter2@warehouse:5432/analytics")


def load_events(path: str) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def to_staging_row(event: dict[str, Any]) -> dict[str, Any]:
    payload = event["data"]["object"]
    if not payload:
        return {}
    return {
        "event_id": event["id"],
        "event_type": event["type"],
        "amount": payload.get("amount", 0),
        "currency": payload.get("currency", "usd"),
        "payment_intent_id": payload["payment_intent_id"],
        "customer_email": payload.get("receipt_email"),
    }


def extract_transactions(path: str | None = None) -> int:
    path = path or os.environ.get("STRIPE_EVENTS_PATH", DEFAULT_EVENTS_PATH)
    log.info("Connecting to warehouse %s", WAREHOUSE_DSN)
    events = load_events(path)
    log.info("Loaded %d Stripe events from %s", len(events), path)
    rows = []
    for event in events:
        log.info("Processing %s %s for %s", event["type"], event["id"], event["data"]["object"].get("receipt_email"))
        row = to_staging_row(event)
        if row:
            rows.append(row)
    log.info("Prepared %d staging rows", len(rows))
    return len(rows)
