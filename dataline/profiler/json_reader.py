"""JSON file profiling with enriched column statistics.

Handles KDD {table, records} format.
"""

from __future__ import annotations

import json
import os

import pandas as pd

from ..core.types import ManifestEntry
from .column_stats import compute_column_stats, compressed_value_repr


def read_json(file_path: str) -> ManifestEntry:
    """Profile a JSON file into a ManifestEntry."""
    size = os.path.getsize(file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    summary: dict = {}

    # KDD wrapper format: {"table": "name", "records": [...]}
    if isinstance(data, dict) and "records" in data and "table" in data:
        records = data["records"]
        summary["kdd_format"] = True
        summary["table_name"] = data["table"]
        summary.update(_profile_records(records))
    elif isinstance(data, list):
        summary["kdd_format"] = False
        summary.update(_profile_records(data))
    elif isinstance(data, dict):
        summary["kdd_format"] = False
        summary["type"] = "object"
        summary["keys"] = list(data.keys())[:20]
        summary["sample"] = {k: _truncate(v) for k, v in list(data.items())[:5]}
    else:
        summary["type"] = type(data).__name__
        summary["preview"] = str(data)[:500]

    return ManifestEntry(
        file_path=file_path,
        file_type="json",
        size_bytes=size,
        summary=summary,
    )


def _profile_records(records: list) -> dict:
    """Profile a list of record dicts with enriched stats."""
    if not records:
        return {"row_count": 0, "columns": []}

    row_count = len(records)
    sample_rows = records[:3]

    # Infer columns from first few records
    all_keys: set[str] = set()
    for r in records[:100]:
        if isinstance(r, dict):
            all_keys.update(r.keys())

    columns = []
    for key in sorted(all_keys):
        values = [r.get(key) for r in records[:100] if isinstance(r, dict)]
        non_null = [v for v in values if v is not None]
        dtype = _infer_dtype(non_null)

        col_info: dict = {
            "name": key,
            "dtype": dtype,
            "null_pct": round(1.0 - len(non_null) / max(len(values), 1), 3),
            "sample": [_truncate(v) for v in non_null[:3]],
        }

        # Enrich with column stats if we have enough data
        if non_null:
            series = pd.Series(non_null)
            col_info.update(compute_column_stats(series, col_name=key))
            col_info["value_repr"] = compressed_value_repr(series)

        columns.append(col_info)

    return {
        "row_count": row_count,
        "columns": columns,
        "sample_rows": sample_rows[:3],
    }


def _infer_dtype(values: list) -> str:
    if not values:
        return "unknown"
    types = {type(v).__name__ for v in values[:20]}
    if types <= {"int", "float"}:
        return "numeric"
    if types == {"str"}:
        return "string"
    if types == {"bool"}:
        return "boolean"
    return "mixed"


def _truncate(v: object, max_len: int = 100) -> object:
    if isinstance(v, str) and len(v) > max_len:
        return v[:max_len] + "..."
    return v
