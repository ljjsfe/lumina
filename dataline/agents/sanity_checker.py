"""Deterministic sanity checks for Judge: pre-LLM evidence injection.

Runs on raw stdout BEFORE the LLM judge call. Produces evidence strings that
get injected into the judge context as high-priority flags. The LLM judge
then uses these as evidence — deterministic detection, LLM judgment.

Three checks:
1. Zero rows after filter — filter condition likely wrong
2. Magnitude error — ratio > 100 or average looks like a sum
3. Filter no-effect — filter returned same count as total loaded
"""

from __future__ import annotations

import re

from ..core.types import AnalysisState


def compute_flags(state: AnalysisState) -> list[str]:
    """Run all deterministic checks on the latest step's stdout.

    Returns a list of evidence strings (empty if no issues found).
    """
    if not state.full_step_details:
        return []

    last = state.full_step_details[-1]
    stdout = last.result.stdout or ""
    question = state.question

    flags: list[str] = []
    flags.extend(_check_zero_rows(stdout))
    flags.extend(_check_magnitude(stdout, question))
    flags.extend(_check_filter_no_effect(stdout, state))
    return flags


def _check_zero_rows(stdout: str) -> list[str]:
    """Flag when a filter explicitly returns 0 rows."""
    patterns = [
        r'after filter[^:]*:\s*0\b',
        r'after filter[^:]*:\s*0\s+rows?',
        r'filtered[^:]*:\s*0\s+rows?',
        r'result[^:]*:\s*0\s+rows?',
        r'0 rows? (?:returned|found|matched)',
        r'empty dataframe',
        r'no matches found',
    ]
    stdout_lower = stdout.lower()
    for p in patterns:
        if re.search(p, stdout_lower):
            return [
                "ZERO_ROWS: filter returned 0 rows — "
                "check filter value, column name, and comparison direction"
            ]
    return []


def _check_magnitude(stdout: str, question: str) -> list[str]:
    """Flag suspicious magnitudes for ratio/average questions."""
    flags = []
    q_lower = question.lower()

    # Ratio/rate check: result > 100 is likely a unit error
    is_ratio = any(
        w in q_lower
        for w in ["ratio", " rate", "proportion", "percentage", "percent", " %", "share of"]
    )
    if is_ratio:
        numbers = re.findall(r'\b(\d+(?:\.\d+)?)\b', stdout)
        last_positive = next((float(n) for n in reversed(numbers) if float(n) > 0), None)
        if last_positive is not None and last_positive > 100:
            flags.append(
                f"MAGNITUDE_WARNING: question asks for ratio/rate/percentage "
                f"but last printed value is {last_positive} (> 100) — "
                "possible unit error (multiply by 100?) or sum instead of ratio"
            )

    # Average/mean check: result >= 1,000,000 is likely a sum
    is_average = any(w in q_lower for w in ["average", "mean", " avg"])
    if is_average:
        large = re.findall(r'\b(\d{7,}(?:\.\d+)?)\b', stdout)
        if large:
            flags.append(
                f"MAGNITUDE_WARNING: question asks for average/mean "
                f"but result {large[0]} looks very large — "
                "may be a sum() instead of mean()"
            )

    return flags


def _check_filter_no_effect(stdout: str, state: AnalysisState) -> list[str]:
    """Flag if filter row count equals total loaded rows (filter didn't apply).

    Requires an earlier step to have printed 'Loaded: N rows'.
    """
    # Extract "after filter: N rows" from current stdout
    m_after = re.search(r'after filter[^:]*:\s*(\d+)\s*rows?', stdout, re.IGNORECASE)
    if not m_after:
        return []
    after_count = int(m_after.group(1))
    if after_count == 0:
        return []  # handled by _check_zero_rows

    # Extract total rows from any prior step stdout
    loaded_count = _extract_loaded_rows(state)
    if loaded_count > 0 and after_count == loaded_count:
        return [
            f"FILTER_NO_EFFECT: filter returned all {loaded_count} rows — "
            "filter condition may not have applied (check column name and value)"
        ]
    return []


def _extract_loaded_rows(state: AnalysisState) -> int:
    """Try to extract total-rows-loaded from early steps' stdout.

    Looks for the coder's standard 'Loaded: N rows' pattern (rule 4).
    Only matches explicit load-time prints — not post-filter counts.
    """
    for step in state.full_step_details[:3]:
        # Match "Loaded: N rows" or "Loaded N rows" — coder rule 4 format
        m = re.search(
            r'\bLoaded[:\s]+(\d+)\s+rows?\b',
            step.result.stdout or "",
        )
        if m:
            return int(m.group(1))
    return 0
