"""DABstep scalar answer scorer with fuzzy string matching and numeric tolerance."""

from __future__ import annotations

import re

from Levenshtein import ratio as levenshtein_ratio


def score_answer(prediction: str, gold: str, tolerance: float = 0.001) -> int:
    """Score a single DABstep answer: 1 if correct, 0 otherwise.

    Handles:
    - Exact match (after normalization)
    - Numeric match with tolerance
    - Comma-separated list match (order-insensitive)
    - Fuzzy string match (Levenshtein >= 0.95)
    """
    pred = _normalize(str(prediction))
    gold_n = _normalize(str(gold))

    if not pred:
        return 0

    # 1. Exact match
    if pred == gold_n:
        return 1

    # 2. Numeric match
    pred_num = _try_float(pred)
    gold_num = _try_float(gold_n)
    if pred_num is not None and gold_num is not None:
        if abs(pred_num - gold_num) <= tolerance:
            return 1
        # Also try relative tolerance for large numbers
        if gold_num != 0 and abs(pred_num - gold_num) / abs(gold_num) <= tolerance:
            return 1

    # 3. List match (comma-separated, order-insensitive)
    if "," in gold_n:
        gold_items = sorted(_normalize(x) for x in gold_n.split(","))
        pred_items = sorted(_normalize(x) for x in pred.split(","))
        if gold_items == pred_items:
            return 1
        # Try numeric tolerance on each element
        if _lists_match_numeric(gold_items, pred_items, tolerance):
            return 1

    # 4. Fuzzy string match
    if levenshtein_ratio(pred, gold_n) >= 0.95:
        return 1

    return 0


def _normalize(s: str) -> str:
    """Strip whitespace, lowercase, remove trailing punctuation."""
    s = s.strip().lower()
    s = re.sub(r"[,\s]+", " ", s)
    s = s.strip(" .")
    return s


def _try_float(s: str) -> float | None:
    """Try to parse as float, handling currency and % symbols."""
    cleaned = re.sub(r"[$€£¥%,]", "", s).strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _lists_match_numeric(gold_items: list[str], pred_items: list[str], tolerance: float) -> bool:
    """Check if two sorted string lists match element-wise with numeric tolerance."""
    if len(gold_items) != len(pred_items):
        return False
    for g, p in zip(gold_items, pred_items):
        g_num = _try_float(g)
        p_num = _try_float(p)
        if g_num is not None and p_num is not None:
            if abs(g_num - p_num) > tolerance:
                return False
        elif g != p:
            return False
    return True
