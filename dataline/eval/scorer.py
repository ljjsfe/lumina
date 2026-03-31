"""KDD Cup column-vector matching scorer."""

from __future__ import annotations

import numpy as np
import pandas as pd


def score_task(prediction: pd.DataFrame, gold: pd.DataFrame, tolerance: float = 0.001) -> int:
    """Score a single task: 1 if ALL gold columns matched, else 0.

    Column names are ignored. Values are compared as unordered vectors.
    Extra prediction columns are OK (no penalty).
    """
    if prediction.empty or gold.empty:
        return 0

    gold_matched = set()

    for gold_col_idx in range(len(gold.columns)):
        gold_values = _normalize_column(gold.iloc[:, gold_col_idx])
        matched = False

        for pred_col_idx in range(len(prediction.columns)):
            pred_values = _normalize_column(prediction.iloc[:, pred_col_idx])
            if _columns_match(gold_values, pred_values, tolerance):
                matched = True
                break

        if matched:
            gold_matched.add(gold_col_idx)
        else:
            return 0  # Any unmatched gold column → score 0

    return 1


def _normalize_column(series: pd.Series) -> list:
    """Normalize column values for comparison."""
    values = []
    for v in series:
        if pd.isna(v):
            values.append(None)
        elif isinstance(v, (int, float, np.integer, np.floating)):
            values.append(float(v))
        else:
            # Normalize string: strip, lowercase
            values.append(str(v).strip().lower())
    return sorted(values, key=lambda x: (x is None, str(x)))


def _columns_match(gold: list, pred: list, tolerance: float) -> bool:
    """Check if two sorted value lists match (with float tolerance)."""
    if len(gold) != len(pred):
        return False

    for g, p in zip(gold, pred):
        if g is None and p is None:
            continue
        if g is None or p is None:
            return False
        if isinstance(g, float) and isinstance(p, float):
            if abs(g - p) > tolerance:
                return False
        elif isinstance(g, float) or isinstance(p, float):
            # One is float, one is string — try numeric comparison
            try:
                gf = float(g) if not isinstance(g, float) else g
                pf = float(p) if not isinstance(p, float) else p
                if abs(gf - pf) > tolerance:
                    return False
            except (ValueError, TypeError):
                return False
        else:
            if g != p:
                return False
    return True
