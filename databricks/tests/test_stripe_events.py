import json
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import stripe_events  # noqa: E402
from stripe_events import BANGKOK, apply_fault, expected_net_revenue, generate_events, to_json_lines  # noqa: E402

DAY = date(2026, 10, 11)


def test_generation_is_deterministic_per_date():
    assert generate_events(DAY) == generate_events(DAY)
    assert generate_events(DAY) != generate_events(date(2026, 10, 12))


def test_events_fall_inside_the_bangkok_day():
    for e in generate_events(DAY):
        assert datetime.fromtimestamp(e["created"], BANGKOK).date() == DAY


def test_contains_refunds_and_duplicate_deliveries():
    events = generate_events(DAY, n_charges=100)
    ids = [e["id"] for e in events]
    assert len(ids) - len(set(ids)) == stripe_events.DUPLICATE_DELIVERIES
    assert any(e["type"] == "charge.refunded" for e in events)


def test_refunds_reference_charges_and_never_exceed_them():
    events = generate_events(DAY)
    charges = {e["charge_id"]: e["amount"] for e in events if e["type"] == "charge.succeeded"}
    for r in (e for e in events if e["type"] == "charge.refunded"):
        assert 0 < r["amount"] <= charges[r["charge_id"]]
    assert expected_net_revenue(events) > 0


def test_net_revenue_ignores_duplicate_deliveries():
    events = generate_events(DAY, n_charges=10)
    assert expected_net_revenue(events) == expected_net_revenue(events + events)


def test_schema_drift_renames_amount():
    drifted = apply_fault(generate_events(DAY), "schema_drift")
    assert all("amount" not in e and "amount_cents" in e for e in drifted)


def test_null_amounts_and_empty_faults():
    nulls = apply_fault(generate_events(DAY), "null_amounts")
    assert any(e["amount"] is None for e in nulls)
    assert apply_fault(generate_events(DAY), "empty") == []
    assert apply_fault(generate_events(DAY), "none") == generate_events(DAY)


def test_unknown_fault_is_rejected():
    with pytest.raises(ValueError, match="unknown fault"):
        apply_fault([], "typo")


def test_json_lines_round_trip():
    events = generate_events(DAY, n_charges=5)
    lines = to_json_lines(events).splitlines()
    assert [json.loads(line) for line in lines] == events
