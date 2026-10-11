import hashlib
import hmac
import json
import sys
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins"))

import openclaw_callback  # noqa: E402


def make_context():
    ti = SimpleNamespace(
        dag_id="daily_revenue_pipeline",
        task_id="extract_stripe_transactions",
        try_number=1,
        log_url="http://airflow/log?x=1",
    )
    dag_run = SimpleNamespace(run_id="scheduled__2026-10-10T02:00:00+00:00", logical_date=None)
    return {
        "ti": ti,
        "dag_run": dag_run,
        "logical_date": datetime(2026, 10, 10, 2, tzinfo=timezone.utc),
        "exception": KeyError("payment_intent_id"),
    }


class FakeResponse:
    status = 202

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_build_payload():
    payload = openclaw_callback.build_payload(make_context())
    assert payload == {
        "dag_id": "daily_revenue_pipeline",
        "task_id": "extract_stripe_transactions",
        "run_id": "scheduled__2026-10-10T02:00:00+00:00",
        "try_number": 1,
        "logical_date": "2026-10-10T02:00:00+00:00",
        "log_url": "http://airflow/log?x=1",
        "exception": "KeyError: 'payment_intent_id'",
    }


class DatabricksRunNowOperator:
    """Stand-in: the callback only looks at the operator's class name."""


def test_build_payload_adds_databricks_run():
    ctx = make_context()
    xcom = {"run_id": 907227724231, "run_page_url": "https://dbx.example/#job/1/run/907227724231"}
    ctx["ti"].xcom_pull = lambda task_ids, key: xcom[key]
    ctx["task"] = DatabricksRunNowOperator()
    payload = openclaw_callback.build_payload(ctx)
    assert payload["databricks_run_id"] == "907227724231"
    assert payload["databricks_run_url"] == xcom["run_page_url"]


def test_build_payload_skips_xcom_for_other_operators():
    ctx = make_context()
    ctx["ti"].xcom_pull = mock.Mock(side_effect=AssertionError("should not be called"))
    payload = openclaw_callback.build_payload(ctx)
    assert "databricks_run_id" not in payload


def test_build_payload_survives_xcom_errors():
    ctx = make_context()
    ctx["ti"].xcom_pull = mock.Mock(side_effect=RuntimeError("no supervisor"))
    ctx["task"] = DatabricksRunNowOperator()
    payload = openclaw_callback.build_payload(ctx)
    assert payload["task_id"] == "extract_stripe_transactions"
    assert "databricks_run_id" not in payload


def test_sends_signed_request(monkeypatch):
    monkeypatch.setenv("OPENCLAW_INGEST_URL", "http://worker:8080/v1/airflow/failure")
    monkeypatch.setenv("OPENCLAW_WEBHOOK_SECRET", "s3cret")
    with mock.patch("urllib.request.urlopen", return_value=FakeResponse()) as urlopen:
        openclaw_callback.notify_openclaw(make_context())
    request = urlopen.call_args.args[0]
    assert request.full_url == "http://worker:8080/v1/airflow/failure"
    assert urlopen.call_args.kwargs["timeout"] == 5
    ts = request.get_header("X-openclaw-timestamp")
    expected = "sha256=" + hmac.new(b"s3cret", ts.encode() + b"." + request.data, hashlib.sha256).hexdigest()
    assert request.get_header("X-openclaw-signature") == expected
    assert json.loads(request.data)["task_id"] == "extract_stripe_transactions"


def test_never_raises_on_network_error(monkeypatch):
    monkeypatch.setenv("OPENCLAW_INGEST_URL", "http://worker:8080/v1/airflow/failure")
    monkeypatch.setenv("OPENCLAW_WEBHOOK_SECRET", "s3cret")
    with mock.patch("urllib.request.urlopen", side_effect=urllib.error.URLError("down")):
        openclaw_callback.notify_openclaw(make_context())  # must not raise


def test_never_raises_on_bad_context(monkeypatch):
    monkeypatch.setenv("OPENCLAW_INGEST_URL", "http://worker:8080/v1/airflow/failure")
    monkeypatch.setenv("OPENCLAW_WEBHOOK_SECRET", "s3cret")
    with mock.patch("urllib.request.urlopen", side_effect=RuntimeError("boom")):
        openclaw_callback.notify_openclaw({})


def test_disabled_without_env(monkeypatch):
    monkeypatch.delenv("OPENCLAW_INGEST_URL", raising=False)
    with mock.patch("urllib.request.urlopen") as urlopen:
        openclaw_callback.notify_openclaw(make_context())
    urlopen.assert_not_called()
