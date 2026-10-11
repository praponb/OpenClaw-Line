"""Data-quality helpers whose messages are safe to send to the investigator.

Failure messages travel to the model provider, LINE and the dashboard, so they
name fields and counts only, never data values (see docs/data-residency.md).
"""

from __future__ import annotations

import json
from collections.abc import Iterable

# Auto Loader adds the source file path to every rescued record.
_RESCUE_META = {"_file_path"}


def unexpected_fields(rescued: Iterable[str | None]) -> list[str]:
    """Field names found in Auto Loader `_rescued_data` JSON strings, sorted; values are discarded."""
    names: set[str] = set()
    for raw in rescued:
        if not raw:
            continue
        try:
            record = json.loads(raw)
        except ValueError:
            names.add("(unparseable rescued data)")
            continue
        if isinstance(record, dict):
            names.update(k for k in record if k not in _RESCUE_META)
    return sorted(names)


def schema_drift_message(landed_file: str, n_rescued: int, total: int, fields: list[str], expected: list[str]) -> str:
    return (
        f"Schema drift in {landed_file}: {n_rescued}/{total} events have fields outside the expected schema "
        f"({', '.join(expected)}). Unexpected fields: {', '.join(fields) or '(none recognised)'}."
    )


def null_field_message(landed_file: str, field: str, n_null: int, total: int) -> str:
    return f"{n_null}/{total} events in {landed_file} have a null {field}; refusing to publish partial revenue."
