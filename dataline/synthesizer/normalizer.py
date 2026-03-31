"""Value normalization for prediction output."""

from __future__ import annotations

import re


def normalize_value(v: object) -> object:
    """Normalize a single value for prediction output."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v

    s = str(v).strip()
    if not s:
        return None

    # Try to extract numeric value
    numeric = _try_numeric(s)
    if numeric is not None:
        return numeric

    return s


def _try_numeric(s: str) -> float | int | None:
    """Try to parse a string as a number."""
    # Remove currency symbols and commas
    cleaned = re.sub(r"[$€£¥,]", "", s)
    # Remove trailing %
    is_percent = cleaned.endswith("%")
    if is_percent:
        cleaned = cleaned[:-1].strip()

    try:
        val = float(cleaned)
        # Keep as int if it's a whole number and not a percentage
        if not is_percent and val == int(val) and "." not in cleaned:
            return int(val)
        return val
    except ValueError:
        return None
