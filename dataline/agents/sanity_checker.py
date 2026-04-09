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

import json
import re

from ..core.types import AnalysisState


def compute_flags(state: AnalysisState) -> list[str]:
    """Run all deterministic checks on the latest step's output.

    Reads from structured_json (save_result) first; falls back to stdout regex.
    Returns a list of evidence strings (empty if no issues found).
    """
    if not state.full_step_details:
        return []

    last = state.full_step_details[-1]
    stdout = last.result.stdout or ""
    question = state.question

    # Parse structured output for reliable row_counts and debug values
    structured = _parse_structured(last.result.structured_json)

    # Find last non-empty step index for backtrack hints (Direction D)
    last_non_empty_step = _find_last_non_empty_step(state)

    flags: list[str] = []
    flags.extend(_check_zero_rows(stdout, structured, last_non_empty_step))
    flags.extend(_check_magnitude(stdout, question, structured))
    flags.extend(_check_filter_no_effect(stdout, state, structured))
    flags.extend(_check_missing_structured(last, stdout))
    flags.extend(_check_count_is_integer(stdout, question, structured))
    return flags


def _parse_structured(structured_json: str) -> dict:
    """Parse structured_json from save_result(). Returns empty dict on failure."""
    if not structured_json:
        return {}
    try:
        return json.loads(structured_json)
    except (json.JSONDecodeError, ValueError):
        return {}


def _find_last_non_empty_step(state: AnalysisState) -> int:
    """Return the index of the last step with non-empty output (not 0 rows).

    Used to suggest a safe truncate_to point when a computation step returns empty.
    Returns 0 (restart) if no non-empty step is found before the latest.
    """
    steps = state.full_step_details
    if len(steps) <= 1:
        return 0
    for step in reversed(steps[:-1]):  # exclude the latest step
        stdout = step.result.stdout or ""
        if step.result.return_code != 0:
            continue
        # Step has useful output if it's not all-zero-rows or empty
        zero_signals = [
            r'after filter[^:]*:\s*0\b',
            r'0 rows? (?:returned|found|matched)',
            r'empty dataframe',
        ]
        if not any(re.search(p, stdout.lower()) for p in zero_signals):
            if len(stdout.strip()) > 20:  # has real content
                return step.step_index
    return 0


def _check_zero_rows(stdout: str, structured: dict, last_non_empty_step: int) -> list[str]:
    """Flag when a filter returns 0 rows.

    Prefers structured row_counts from save_result(); falls back to stdout regex.
    Includes last_non_empty_step hint for Judge backtrack targeting.
    """
    backtrack_hint = f" If backtracking, truncate_to={last_non_empty_step} (last non-empty step)."

    # Structured path: check row_counts dict
    row_counts = structured.get("row_counts", {})
    for key, val in row_counts.items():
        if "filter" in key.lower() and val == 0:
            return [
                f"ZERO_ROWS: {key}=0 — "
                f"check filter value, column name, and comparison direction.{backtrack_hint}"
            ]

    # Fallback: stdout regex
    patterns = [
        r'after filter[^:]*:\s*0\b',
        r'filtered[^:]*:\s*0\s+rows?',
        r'0 rows? (?:returned|found|matched)',
        r'empty dataframe',
        r'no matches found',
    ]
    for p in patterns:
        if re.search(p, stdout.lower()):
            return [
                "ZERO_ROWS: filter returned 0 rows — "
                f"check filter value, column name, and comparison direction.{backtrack_hint}"
            ]
    return []


def _check_magnitude(stdout: str, question: str, structured: dict) -> list[str]:
    """Flag suspicious magnitudes for ratio/average questions.

    Uses structured debug values when available for reliable number extraction.
    """
    flags = []
    q_lower = question.lower()

    # Get the ratio/result value: structured debug takes priority over stdout regex
    debug = structured.get("debug", {})

    is_ratio = any(
        w in q_lower
        for w in ["ratio", " rate", "proportion", "percentage", "percent", " %", "share of"]
    )
    if is_ratio:
        ratio_val = None
        # Try structured debug first
        for key in ("ratio", "rate", "proportion", "result", "value"):
            if key in debug and isinstance(debug[key], (int, float)):
                ratio_val = float(debug[key])
                break
        # Fall back to last number in stdout
        if ratio_val is None:
            numbers = re.findall(r'\b(\d+(?:\.\d+)?)\b', stdout)
            ratio_val = next((float(n) for n in reversed(numbers) if float(n) > 0), None)
        if ratio_val is not None and ratio_val > 100:
            flags.append(
                f"MAGNITUDE_WARNING: question asks for ratio/rate/percentage "
                f"but result is {ratio_val} (> 100) — "
                "possible unit error or sum instead of ratio"
            )

    is_average = any(w in q_lower for w in ["average", "mean", " avg"])
    if is_average:
        avg_val = debug.get("average") or debug.get("mean") or debug.get("result")
        if avg_val is None:
            large = re.findall(r'\b(\d{7,}(?:\.\d+)?)\b', stdout)
            avg_val = float(large[0]) if large else None
        if avg_val is not None and float(avg_val) >= 1_000_000:
            flags.append(
                f"MAGNITUDE_WARNING: question asks for average/mean "
                f"but result {avg_val} looks very large — "
                "may be sum() instead of mean()"
            )

    return flags


def _check_filter_no_effect(stdout: str, state: AnalysisState, structured: dict) -> list[str]:
    """Flag if filter row count equals total loaded rows (filter didn't apply).

    Prefers structured row_counts; falls back to stdout patterns.
    """
    row_counts = structured.get("row_counts", {})

    # Structured path
    loaded = row_counts.get("rows_loaded")
    for key, val in row_counts.items():
        if "filter" in key.lower() and loaded and val == loaded and val > 0:
            return [
                f"FILTER_NO_EFFECT: {key}={val} equals rows_loaded={loaded} — "
                "filter condition may not have applied"
            ]

    # Fallback: stdout regex + prior steps
    m_after = re.search(r'after filter[^:]*:\s*(\d+)\s*rows?', stdout, re.IGNORECASE)
    if not m_after:
        return []
    after_count = int(m_after.group(1))
    if after_count == 0:
        return []  # handled by _check_zero_rows
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


def _check_missing_structured(last_step, stdout: str) -> list[str]:
    """Flag when a successful computation step printed a numeric answer but skipped save_result().

    Only fires when:
    - Step succeeded (return_code == 0)
    - No structured_json was written
    - stdout contains patterns suggesting a final computed answer
    """
    if last_step.result.return_code != 0:
        return []
    if last_step.result.structured_json:
        return []

    answer_patterns = [
        r'\banswer\s*[:=]\s*[\d\.\-]+',
        r'\bresult\s*[:=]\s*[\d\.\-]+',
        r'\btotal\s*[:=]\s*[\d\.\-]+',
        r'\bratio\s*[:=]\s*[\d\.\-]+',
        r'\brate\s*[:=]\s*[\d\.\-]+',
        r'\bcount\s*[:=]\s*\d+',
        r'\bfinal\s+(?:answer|result|value)\s*[:=]\s*[\d\.\-]+',
    ]
    for p in answer_patterns:
        if re.search(p, stdout.lower()):
            return [
                "MISSING_STRUCTURED: step produced a numeric answer but did not call save_result() — "
                "finalizer will use the LLM path which may lose precision. "
                "Add save_result(answer={...}) as the last line."
            ]
    return []


def _check_count_is_integer(stdout: str, question: str, structured: dict) -> list[str]:
    """Flag when 'how many'/'count of' question returns a non-integer float.

    Counts must be whole numbers. A decimal answer indicates a ratio/average was
    computed instead of a count.
    """
    q_lower = question.lower()
    is_count_question = any(
        phrase in q_lower
        for phrase in ["how many", "count of", "number of", "how much"]
    )
    if not is_count_question:
        return []

    # Check structured answer first
    answer = structured.get("answer", {})
    for val_list in answer.values():
        if isinstance(val_list, list) and len(val_list) == 1:
            v = val_list[0]
            if isinstance(v, float) and v != int(v):
                return [
                    f"COUNT_NOT_INTEGER: question asks 'how many/count' but result is {v} "
                    "(not a whole number) — may be an average or ratio instead of a count"
                ]

    # Fallback: last decimal number in stdout
    decimals = re.findall(r'\b(\d+\.\d+)\b', stdout)
    if decimals:
        last_val = float(decimals[-1])
        if last_val != int(last_val):
            return [
                f"COUNT_NOT_INTEGER: question asks 'how many/count' but result appears to be "
                f"{last_val} (not a whole number) — may be an average or ratio instead of a count"
            ]
    return []
