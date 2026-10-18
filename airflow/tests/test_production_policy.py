import importlib
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "plugins"))
sys.path.insert(0, str(ROOT / "production"))

import airflow_local_settings  # noqa: E402
from openclaw_callback import notify_openclaw  # noqa: E402


def task(callback=None, tags=()):
    return SimpleNamespace(on_failure_callback=callback, dag=SimpleNamespace(tags=set(tags)))


def test_sets_notifier_when_task_has_no_callback():
    t = task()
    airflow_local_settings.task_policy(t)
    assert t.on_failure_callback is notify_openclaw


def test_keeps_an_existing_callback():
    def page_oncall(context):
        pass

    t = task(page_oncall)
    airflow_local_settings.task_policy(t)
    assert t.on_failure_callback == [page_oncall, notify_openclaw]


def test_appends_to_a_callback_list_once():
    def page_oncall(context):
        pass

    t = task([page_oncall])
    airflow_local_settings.task_policy(t)
    airflow_local_settings.task_policy(t)  # policies can run more than once per parse
    assert t.on_failure_callback == [page_oncall, notify_openclaw]


def test_does_not_wrap_the_notifier_itself():
    t = task(notify_openclaw)
    airflow_local_settings.task_policy(t)
    assert t.on_failure_callback is notify_openclaw


def test_recognizes_a_reimported_notifier():
    # Airflow's plugin manager loads openclaw_callback.py a second time: same name,
    # different function object. The task must still get one notifier.
    spec = importlib.util.spec_from_file_location("openclaw_callback", ROOT / "plugins" / "openclaw_callback.py")
    copy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(copy)
    assert copy.notify_openclaw is not notify_openclaw
    t = task([copy.notify_openclaw])
    airflow_local_settings.task_policy(t)
    assert t.on_failure_callback == [copy.notify_openclaw]


def test_skips_dags_tagged_no_openclaw():
    t = task(tags={"maintenance", "no-openclaw"})
    airflow_local_settings.task_policy(t)
    assert t.on_failure_callback is None


def test_leaves_tasks_alone_without_the_plugin(monkeypatch):
    monkeypatch.setitem(sys.modules, "openclaw_callback", None)  # import fails
    policy = importlib.reload(airflow_local_settings)
    try:
        t = task()
        policy.task_policy(t)
        assert t.on_failure_callback is None
    finally:
        monkeypatch.delitem(sys.modules, "openclaw_callback")
        importlib.reload(airflow_local_settings)
