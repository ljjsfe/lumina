"""KDD Cup column-vector matching scorer.

Scoring formula (April 2026 rules):
    Score = Recall − λ × (Extra Columns / Predicted Columns)

    Recall = matched_gold_columns / total_gold_columns
    Extra  = predicted columns that match no gold column
    λ      = penalty weight (default 1.0, official value TBC)

Column matching: content-based only, names ignored, rows unordered.

Value normalization (official rules):
    - Null:    empty / "null" / "none" / "nan" / "nat" / "<na>" → ""
    - Numeric: Decimal, ROUND_HALF_UP to 2 decimal places
    - Date:    ISO 8601 YYYY-MM-DD
    - String:  strip whitespace, CASE-SENSITIVE (no lowercasing)
"""

from __future__ import annotations

import re
from decimal import Decimal, ROUND_HALF_UP

import numpy as np
import pandas as pd

_NULL_STRINGS = {"", "null", "none", "nan", "nat", "<na>"}


def score_task(
    prediction: pd.DataFrame,
    gold: pd.DataFrame,
    lam: float = 1.0,
) -> float:
    """Score a single task using the official recall-minus-penalty formula.

    Args:
        prediction: Agent's prediction DataFrame.
        gold:       Gold-standard DataFrame.
        lam:        Penalty weight λ for extra columns (default 1.0).

    Returns:
        Float in [0, 1]. Binary-equivalent when prediction exactly covers gold.
    """
    if prediction.empty or gold.empty:
        return 0.0

    pred_sigs = [_column_signature(prediction.iloc[:, i]) for i in range(len(prediction.columns))]
    gold_sigs = [_column_signature(gold.iloc[:, i]) for i in range(len(gold.columns))]

    matched_gold = 0
    matched_pred_indices: set[int] = set()

    for g_sig in gold_sigs:
        for p_idx, p_sig in enumerate(pred_sigs):
            if p_idx in matched_pred_indices:
                continue
            if g_sig == p_sig:
                matched_gold += 1
                matched_pred_indices.add(p_idx)
                break

    total_gold = len(gold_sigs)
    total_pred = len(pred_sigs)

    recall = matched_gold / total_gold if total_gold > 0 else 0.0
    extra = total_pred - len(matched_pred_indices)
    penalty = lam * (extra / total_pred) if total_pred > 0 else 0.0

    return max(0.0, recall - penalty)


def _column_signature(series: pd.Series) -> tuple:
    """Build a sorted tuple of normalized values for column-content matching."""
    return tuple(sorted(_normalize_value(v) for v in series))


def _normalize_value(v: object) -> str:
    """Normalize a single cell value per official rules.

    - Null variants → ""
    - Numeric → ROUND_HALF_UP 2dp string
    - Date-like → ISO 8601 YYYY-MM-DD string
    - String → strip, preserve case
    """
    # Pandas NA / float NaN
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass

    if isinstance(v, (int, np.integer)):
        # Integers: still format as 2dp per rules (e.g. 42 → "42.00")
        return str(Decimal(int(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    if isinstance(v, (float, np.floating)):
        if np.isnan(v) or np.isinf(v):
            return ""
        return str(Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    s = str(v).strip()

    # Null string variants
    if s.lower() in _NULL_STRINGS:
        return ""

    # Try numeric parse
    numeric = _try_numeric(s)
    if numeric is not None:
        return str(Decimal(str(numeric)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    # Try date parse
    date_str = _try_date(s)
    if date_str is not None:
        return date_str

    # Plain string — strip only, preserve case
    return s


def _try_numeric(s: str) -> float | None:
    """Parse string as number, returning float or None."""
    cleaned = re.sub(r"[$€£¥,\s]", "", s)
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1]
    try:
        return float(cleaned)
    except ValueError:
        return None


def _try_date(s: str) -> str | None:
    """Parse common date strings to ISO 8601 YYYY-MM-DD, or None."""
    # Already ISO: YYYY-MM-DD
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    # YYYY/MM/DD or YYYY.MM.DD
    m = re.fullmatch(r"(\d{4})[/.](\d{1,2})[/.](\d{1,2})", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # MM/DD/YYYY or DD/MM/YYYY — ambiguous, skip to avoid false positives
    # YYYY-M-D (single digit month/day)
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return None
