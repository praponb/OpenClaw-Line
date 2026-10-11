"""Synthetic Stripe-style payment events: the upstream source of the lakehouse pipeline.

Pure Python (no Spark) so it can be unit tested locally and imported by the
landing notebook. Output for a given date is deterministic, so reruns land the
same events and silver/gold stay idempotent.
"""

from __future__ import annotations

import json
import random
from datetime import date, datetime, time, timedelta, timezone

BANGKOK = timezone(timedelta(hours=7))
FAULTS = ("none", "schema_drift", "null_amounts", "empty")
DUPLICATE_DELIVERIES = 3


def generate_events(run_date: date, n_charges: int = 200) -> list[dict]:
    """One Bangkok day of charges, same-day refunds and a few duplicate deliveries."""
    rng = random.Random(run_date.toordinal())
    day_start = datetime.combine(run_date, time(0), BANGKOK)
    day_end = int((day_start + timedelta(days=1)).timestamp()) - 1
    tag = f"{run_date:%Y%m%d}"

    charges = []
    for i in range(n_charges):
        created = int((day_start + timedelta(seconds=rng.randrange(86_400))).timestamp())
        charges.append({
            "id": f"evt_{tag}_c{i:05d}",
            "type": "charge.succeeded",
            "created": created,
            "charge_id": f"ch_{tag}_{i:05d}",
            "amount": rng.choice([9_900, 19_900, 29_900, 49_900]) + rng.randrange(0, 5_000, 100),
            "currency": "thb",
            "customer": f"cus_{rng.randrange(1, 60):04d}",
        })

    refunds = []
    for j, charge in enumerate(rng.sample(charges, k=max(1, n_charges // 20))):
        full = rng.random() < 0.7
        refunds.append({
            "id": f"evt_{tag}_r{j:05d}",
            "type": "charge.refunded",
            "created": min(charge["created"] + rng.randrange(600, 6 * 3_600), day_end),
            "charge_id": charge["charge_id"],
            "amount": charge["amount"] if full else charge["amount"] // 2,
            "currency": charge["currency"],
            "customer": charge["customer"],
        })

    events = charges + refunds
    # Stripe webhooks are at-least-once: silver must dedupe on event id.
    events += [dict(e) for e in rng.sample(events, k=min(DUPLICATE_DELIVERIES, len(events)))]
    events.sort(key=lambda e: (e["created"], e["id"]))
    return events


def apply_fault(events: list[dict], fault: str) -> list[dict]:
    """Simulate upstream breakage so the investigator has real incidents to triage."""
    if fault not in FAULTS:
        raise ValueError(f"unknown fault {fault!r}; expected one of {', '.join(FAULTS)}")
    if fault == "none":
        return events
    if fault == "empty":
        return []
    if fault == "schema_drift":
        # Upstream API version renamed the field.
        return [{("amount_cents" if k == "amount" else k): v for k, v in e.items()} for e in events]
    # null_amounts: every 25th event arrives without an amount.
    return [{**e, "amount": None} if i % 25 == 0 else e for i, e in enumerate(events)]


def to_json_lines(events: list[dict]) -> str:
    return "".join(json.dumps(e, separators=(",", ":")) + "\n" for e in events)


def expected_net_revenue(events: list[dict]) -> int:
    """Net revenue in minor units after dedupe, the value gold must reproduce."""
    unique = {e["id"]: e for e in events}.values()
    return sum(e["amount"] if e["type"] == "charge.succeeded" else -e["amount"] for e in unique)
