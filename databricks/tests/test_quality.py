import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from quality import null_field_message, schema_drift_message, unexpected_fields  # noqa: E402

EXPECTED = ["id", "type", "created", "charge_id", "amount", "currency", "customer"]


def test_unexpected_fields_keeps_names_and_drops_values():
    rescued = [
        '{"amount_cents":33800,"_file_path":"/Volumes/lakehouse/bronze/landing/x.json"}',
        '{"amount_cents":120,"customer_email":"jane@example.com"}',
        None,
        "",
    ]
    assert unexpected_fields(rescued) == ["amount_cents", "customer_email"]


def test_unparseable_rescued_data_is_named_not_echoed():
    assert unexpected_fields(["not json, secret 1103700123458"]) == ["(unparseable rescued data)"]


def test_messages_contain_no_values():
    msg = schema_drift_message("events-2026-10-11.json", 213, 213, ["amount_cents"], EXPECTED)
    assert msg.endswith("Unexpected fields: amount_cents.")
    assert "33800" not in msg and "_file_path" not in msg
    assert null_field_message("events.json", "amount", 9, 213) == "9/213 events in events.json have a null amount; refusing to publish partial revenue."
