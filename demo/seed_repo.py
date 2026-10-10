#!/usr/bin/env python3
"""Create the demo git repository that `get_git_file_diff` (provider=local) inspects.

History (dates relative to now, so the lookback window always matches):
  1. ~10 days ago  "Add Stripe extraction pipeline"  - safe payload.get("payment_intent_id")
  2. ~1 day ago    "Bypass empty payloads in Stripe extraction" - adds the empty-payload guard
                   and switches to direct payload["payment_intent_id"] access (the bug)

The final tree equals airflow/dags, so traceback line numbers match the repo.
Usage: python3 demo/seed_repo.py [target_dir]   (default: demo/.state/repo)
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAGS = ROOT / "airflow" / "dags"
EXTRACT = Path("dags/stripe_ingest/extract_stripe.py")


def git(repo: Path, *args: str, when: datetime | None = None) -> None:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "data-eng",
        "GIT_AUTHOR_EMAIL": "data-eng@example.com",
        "GIT_COMMITTER_NAME": "data-eng",
        "GIT_COMMITTER_EMAIL": "data-eng@example.com",
        "GIT_CONFIG_NOSYSTEM": "1",
    }
    if when:
        stamp = when.strftime("%Y-%m-%dT%H:%M:%S%z")
        env["GIT_AUTHOR_DATE"] = env["GIT_COMMITTER_DATE"] = stamp
    subprocess.run(["git", "-C", str(repo), *args], check=True, env=env)


def before_version(source: str) -> str:
    """The pre-regression version of extract_stripe.py."""
    guard = "    if not payload:\n        return {}\n"
    if guard not in source or 'payload["payment_intent_id"]' not in source:
        sys.exit("extract_stripe.py no longer matches the demo story; update demo/seed_repo.py")
    return source.replace(guard, "").replace('payload["payment_intent_id"]', 'payload.get("payment_intent_id")')


def main() -> None:
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "demo" / ".state" / "repo"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    now = datetime.now(timezone.utc)

    git(target, "init", "-q", "-b", "main")
    shutil.copytree(DAGS, target / "dags", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    final_source = (target / EXTRACT).read_text()

    (target / EXTRACT).write_text(before_version(final_source))
    git(target, "add", ".")
    git(target, "commit", "-q", "-m", "Add Stripe extraction pipeline", when=now - timedelta(days=10))

    (target / EXTRACT).write_text(final_source)
    git(target, "add", ".")
    git(
        target,
        "commit",
        "-q",
        "-m",
        "Bypass empty payloads in Stripe extraction\n\nSkip events whose data.object is empty and read payment_intent_id directly.",
        when=now - timedelta(days=1),
    )
    sha = subprocess.run(["git", "-C", str(target), "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    print(f"seeded demo repo at {target} (HEAD {sha})")


if __name__ == "__main__":
    main()
