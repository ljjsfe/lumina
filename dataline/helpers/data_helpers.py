"""Sandbox helper functions — automatically available in generated code.

These functions reduce LLM token waste on boilerplate data loading,
type detection, and common transformations. Copied to TEMP_DIR at
sandbox init so generated code can `from data_helpers import *`.

All functions are deterministic, self-contained, and import only
standard library + pandas + numpy.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


# --- File loading ---


def safe_read_csv(
    filename: str,
    task_dir: str | None = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Read CSV with automatic encoding fallback and path resolution.

    Args:
        filename: File name (resolved relative to TASK_DIR) or absolute path.
        task_dir: Override for TASK_DIR env var.
        **kwargs: Passed to pd.read_csv.

    Returns:
        DataFrame with the CSV contents.
    """
    path = _resolve_path(filename, task_dir)
    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            return pd.read_csv(path, encoding=encoding, **kwargs)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, encoding="latin-1", errors="replace", **kwargs)


def safe_read_json(
    filename: str,
    task_dir: str | None = None,
) -> Any:
    """Read JSON file with path resolution.

    Returns parsed JSON (dict, list, etc.).
    """
    path = _resolve_path(filename, task_dir)
    for encoding in ("utf-8", "latin-1"):
        try:
            with open(path, encoding=encoding) as f:
                return json.load(f)
        except UnicodeDecodeError:
            continue
    with open(path, encoding="utf-8", errors="replace") as f:
        return json.load(f)


def safe_read_excel(
    filename: str,
    task_dir: str | None = None,
    sheet_name: str | int = 0,
    **kwargs: Any,
) -> pd.DataFrame:
    """Read Excel file with path resolution.

    Args:
        filename: File name or absolute path.
        task_dir: Override for TASK_DIR env var.
        sheet_name: Sheet to read (default: first sheet).
        **kwargs: Passed to pd.read_excel.
    """
    path = _resolve_path(filename, task_dir)
    return pd.read_excel(path, sheet_name=sheet_name, **kwargs)


# --- Data structure inspection ---


def describe_data(obj: object, name: str = "data", max_items: int = 5) -> str:
    """Print human-readable structure description of any data object.

    Use this FIRST when loading a new data source to understand its format.

    Args:
        obj: Any Python object (list, dict, DataFrame, Series, scalar, etc.)
        name: Display name for the object
        max_items: Maximum sample items to show per field

    Returns:
        Formatted string describing the structure (also prints it).
    """
    lines: list[str] = []

    if isinstance(obj, pd.DataFrame):
        lines.append(f"{name}: DataFrame ({obj.shape[0]:,} rows x {obj.shape[1]} cols)")
        for col in obj.columns:
            dtype = obj[col].dtype
            nunique = obj[col].nunique()
            null_pct = obj[col].isna().mean() * 100
            sample = obj[col].dropna().head(3).tolist()
            extra = f", {null_pct:.0f}% null" if null_pct > 0 else ""
            lines.append(f"  {col}: {dtype} ({nunique:,} unique{extra}) samples={sample}")

    elif isinstance(obj, pd.Series):
        lines.append(f"{name}: Series ({len(obj):,} items, dtype={obj.dtype})")
        lines.append(f"  unique={obj.nunique()}, null={obj.isna().sum()}")
        lines.append(f"  samples={obj.dropna().head(max_items).tolist()}")

    elif isinstance(obj, list):
        lines.append(f"{name}: list ({len(obj):,} items)")
        if len(obj) == 0:
            lines.append("  (empty)")
        elif isinstance(obj[0], dict):
            keys = list(obj[0].keys())
            lines.append(f"  Each item is a dict with {len(keys)} keys: {keys}")
            for key in keys[:15]:
                values = [item.get(key) for item in obj[: min(50, len(obj))]]
                types = set(type(v).__name__ for v in values if v is not None)
                type_str = "/".join(sorted(types)) if types else "null"
                if any(isinstance(v, list) for v in values):
                    non_empty = [v for v in values if isinstance(v, list) and len(v) > 0]
                    empty_count = sum(1 for v in values if isinstance(v, list) and len(v) == 0)
                    if non_empty:
                        sample_inner = non_empty[0][:3]
                        lines.append(f"  {key}: list (empty={empty_count}/{len(values)}) sample={sample_inner}")
                    else:
                        lines.append(f"  {key}: list (all empty)")
                else:
                    non_null = [v for v in values if v is not None]
                    unique_vals = set(str(v) for v in non_null[:50])
                    null_count = sum(1 for v in values if v is None)
                    if len(unique_vals) <= max_items:
                        lines.append(f"  {key}: {type_str} values={sorted(unique_vals)} null={null_count}")
                    else:
                        sample = [str(v) for v in non_null[:3]]
                        lines.append(f"  {key}: {type_str} ({len(unique_vals)}+ unique) samples={sample} null={null_count}")
        else:
            sample = obj[:max_items]
            types = set(type(v).__name__ for v in obj[:50])
            lines.append(f"  item types: {'/'.join(sorted(types))}")
            lines.append(f"  samples: {sample}")

    elif isinstance(obj, dict):
        lines.append(f"{name}: dict ({len(obj)} keys)")
        for key in list(obj.keys())[:15]:
            val = obj[key]
            val_type = type(val).__name__
            if isinstance(val, (list, dict)):
                val_preview = f"{val_type}({len(val)} items)"
            elif isinstance(val, str) and len(val) > 50:
                val_preview = f"str({len(val)} chars): '{val[:50]}...'"
            else:
                val_preview = repr(val)
            lines.append(f"  {key}: {val_preview}")

    else:
        lines.append(f"{name}: {type(obj).__name__} = {repr(obj)[:200]}")

    result = "\n".join(lines)
    print(result)
    return result


# --- DataFrame inspection ---


def describe_df(df: pd.DataFrame, name: str = "df") -> str:
    """Produce a compact summary of a DataFrame for printing.

    Includes: shape, dtypes, null counts, and first 3 rows.
    Much more info-dense than raw df.head().
    """
    lines = [
        f"=== {name}: {df.shape[0]} rows × {df.shape[1]} cols ===",
        "",
        "Columns:",
    ]
    for col in df.columns:
        dtype = df[col].dtype
        nulls = df[col].isna().sum()
        nunique = df[col].nunique()
        null_info = f", {nulls} nulls" if nulls > 0 else ""
        lines.append(f"  {col} ({dtype}, {nunique} unique{null_info})")

    lines.append(f"\nFirst 3 rows:\n{df.head(3).to_string()}")
    return "\n".join(lines)


# --- Column detection ---


def find_join_keys(df_a: pd.DataFrame, df_b: pd.DataFrame) -> list[str]:
    """Find columns shared by name between two DataFrames.

    Returns column names that exist in both (case-insensitive match).
    """
    cols_a = {c.lower(): c for c in df_a.columns}
    cols_b = {c.lower(): c for c in df_b.columns}
    shared = set(cols_a.keys()) & set(cols_b.keys())
    return sorted(cols_a[k] for k in shared)


def detect_date_columns(df: pd.DataFrame) -> list[str]:
    """Detect columns that look like dates (string or datetime).

    Returns list of column names that are likely date/datetime.
    """
    date_patterns = [
        re.compile(r"^\d{4}-\d{2}-\d{2}"),
        re.compile(r"^\d{2}/\d{2}/\d{4}"),
        re.compile(r"^\d{4}/\d{2}/\d{2}"),
    ]
    result: list[str] = []
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            result.append(str(col))
            continue
        if df[col].dtype == object:
            sample = df[col].dropna().head(10)
            if len(sample) >= 3:
                matches = sum(
                    1 for v in sample
                    if isinstance(v, str) and any(p.match(v) for p in date_patterns)
                )
                if matches / len(sample) > 0.5:
                    result.append(str(col))
    return result


def clean_numeric(series: pd.Series) -> pd.Series:
    """Convert string series with currency/percentage markers to numeric.

    Handles: $1,234.56, €100, 45.6%, 1,000, etc.
    Returns numeric Series (non-convertible values become NaN).
    """
    if pd.api.types.is_numeric_dtype(series):
        return series

    cleaned = (
        series.astype(str)
        .str.strip()
        .str.replace(r"^[\$€£¥]", "", regex=True)
        .str.replace(r"%$", "", regex=True)
        .str.replace(",", "", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


# --- Pickle helpers ---


def save_intermediate(data: Any, name: str, temp_dir: str | None = None) -> str:
    """Save intermediate result to TEMP_DIR as pickle.

    Args:
        data: Any picklable object.
        name: Short name (e.g., 'filtered_payments'). .pkl extension added automatically.
        temp_dir: Override for TEMP_DIR env var.

    Returns:
        Full path to saved file.
    """
    import pickle

    td = temp_dir or os.environ.get("TEMP_DIR", ".")
    if not name.endswith(".pkl"):
        name = f"{name}.pkl"
    path = os.path.join(td, name)
    with open(path, "wb") as f:
        pickle.dump(data, f)
    return path


def load_intermediate(name: str, temp_dir: str | None = None) -> Any:
    """Load intermediate result from TEMP_DIR.

    Args:
        name: Name used in save_intermediate (with or without .pkl).
        temp_dir: Override for TEMP_DIR env var.

    Returns:
        The unpickled object.
    """
    import pickle

    td = temp_dir or os.environ.get("TEMP_DIR", ".")
    if not name.endswith(".pkl"):
        name = f"{name}.pkl"
    path = os.path.join(td, name)
    with open(path, "rb") as f:
        return pickle.load(f)


# --- Private helpers ---


def _resolve_path(filename: str, task_dir: str | None = None) -> str:
    """Resolve filename to absolute path using TASK_DIR.

    Searches TASK_DIR root first, then all subdirectories.
    This handles tasks where data lives in context/, json/, etc.
    """
    if os.path.isabs(filename):
        return filename
    td = task_dir or os.environ.get("TASK_DIR", ".")

    # Direct path first
    direct = os.path.join(td, filename)
    if os.path.exists(direct):
        return direct

    # Search subdirectories (common: context/, json/, data/)
    for root, _dirs, files in os.walk(td):
        if filename in files:
            return os.path.join(root, filename)

    # Fallback to direct path (will fail with clear FileNotFoundError)
    return direct
