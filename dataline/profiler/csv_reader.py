"""CSV file profiling with enriched column statistics."""

from __future__ import annotations

import logging
import os

import pandas as pd

from ..core.types import ManifestEntry
from .column_stats import compute_column_stats, compressed_value_repr, detect_anomalies, safe_scalar

logger = logging.getLogger(__name__)


def read_csv(file_path: str) -> ManifestEntry:
    """Profile a CSV file into a ManifestEntry with enriched stats."""
    size = os.path.getsize(file_path)
    try:
        df = pd.read_csv(file_path, nrows=100)
    except Exception:
        df = pd.read_csv(file_path, nrows=100, encoding="latin-1")

    columns = []
    anomalies: list[str] = []
    for col in df.columns:
        col_info: dict = {
            "name": str(col),
            "dtype": str(df[col].dtype),
            "null_pct": round(float(df[col].isna().mean()), 3),
        }
        if pd.api.types.is_numeric_dtype(df[col]):
            col_info["min"] = safe_scalar(df[col].min())
            col_info["max"] = safe_scalar(df[col].max())

        # Enriched stats (deterministic)
        col_info.update(compute_column_stats(df[col]))
        col_info["value_repr"] = compressed_value_repr(df[col])

        # Keep raw sample for backward compat
        col_info["sample"] = [safe_scalar(v) for v in df[col].dropna().head(3).tolist()]

        columns.append(col_info)
        anomalies.extend(detect_anomalies(df[col], str(col)))

    # Get full row count by counting lines (avoids loading entire file into memory)
    try:
        with open(file_path, "rb") as f:
            # Subtract 1 for header row; handle edge case of empty files
            row_count = max(sum(1 for _ in f) - 1, 0)
    except OSError:
        row_count = len(df)

    sample_rows = df.head(3).to_dict(orient="records")

    summary: dict = {
        "columns": columns,
        "row_count": row_count,
        "sample_rows": sample_rows,
    }
    if anomalies:
        summary["anomalies"] = anomalies

    return ManifestEntry(
        file_path=file_path,
        file_type="csv",
        size_bytes=size,
        summary=summary,
    )


