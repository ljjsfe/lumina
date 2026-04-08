"""Deterministic post-processor for finalizer output.

Fixes column-merging errors that survive even explicit prompt rules.
No LLM — pure pattern matching and structural analysis.

Two targeted fixes:
1. Score columns: "1-1" → home_score=1, away_score=1
2. Merged name columns: "Sacha Harrison" → first_name=Sacha, last_name=Harrison
   (only when last-step stdout confirms separate source columns)
"""

from __future__ import annotations

import re

from ..core.types import AnalysisState


_SCORE_PATTERN = re.compile(r'^\d+\s*[-\u2013]\s*\d+$')
_FIRST_NAME_RE = re.compile(r'\b(first[_\-\s]?name|given[_\-\s]?name|fname)\b', re.IGNORECASE)
_LAST_NAME_RE = re.compile(r'\b(last[_\-\s]?name|sur[_\-\s]?name|family[_\-\s]?name|lname)\b', re.IGNORECASE)


def post_process(answer: dict, state: AnalysisState | None = None) -> dict:
    """Apply all deterministic post-processing to finalizer output.

    Safe to call even when state is None (skips stdout-dependent fixes).
    """
    if not answer:
        return answer

    result = _fix_score_columns(dict(answer))

    if state is not None:
        stdout = _get_last_stdout(state)
        if stdout:
            result = _fix_merged_name_columns(result, stdout)

    return result


# --- Fix 1: Score columns ---

def _fix_score_columns(answer: dict) -> dict:
    """Split 'N-N' score values into separate integer columns.

    Only fires when ALL values in a column match the score pattern —
    never splits partial matches to avoid false positives.
    """
    result = {}
    for col, values in answer.items():
        if (values and
                all(isinstance(v, str) and _SCORE_PATTERN.match(str(v).strip()) for v in values)):
            home_vals, away_vals = [], []
            for v in values:
                parts = re.split(r'\s*[-\u2013]\s*', str(v).strip(), maxsplit=1)
                home_vals.append(int(parts[0]))
                away_vals.append(int(parts[1]))
            result["home_score"] = home_vals
            result["away_score"] = away_vals
        else:
            result[col] = values
    return result


# --- Fix 2: Merged name columns ---

def _get_last_stdout(state: AnalysisState) -> str:
    for step in reversed(state.full_step_details):
        if step.result.return_code == 0 and step.result.stdout.strip():
            return step.result.stdout
    return ""


def _fix_merged_name_columns(answer: dict, stdout: str) -> dict:
    """Re-split merged 'First Last' name columns when stdout had separate columns.

    Conditions:
    1. Stdout contains both first_name AND last_name column patterns (strong evidence)
    2. Answer has a column where every value is exactly 2 capitalized words
    3. Answer does not already have separate first/last name columns
    """
    has_first = bool(_FIRST_NAME_RE.search(stdout))
    has_last = bool(_LAST_NAME_RE.search(stdout))
    if not (has_first and has_last):
        return answer

    # Skip if answer already has separate name columns
    cols_lower = {k.lower() for k in answer}
    already_split = (
        any(_FIRST_NAME_RE.search(c) for c in cols_lower) and
        any(_LAST_NAME_RE.search(c) for c in cols_lower)
    )
    if already_split:
        return answer

    first_col = _normalize_col_name(_FIRST_NAME_RE.search(stdout).group(0))
    last_col = _normalize_col_name(_LAST_NAME_RE.search(stdout).group(0))

    result = {}
    for col, values in answer.items():
        if values and all(isinstance(v, str) and _is_two_word_name(v) for v in values):
            result[first_col] = [v.split()[0] for v in values]
            result[last_col] = [v.split()[-1] for v in values]
        else:
            result[col] = values
    return result


def _is_two_word_name(value: str) -> bool:
    """Return True if value looks like 'FirstName LastName'."""
    parts = value.strip().split()
    return (
        len(parts) == 2 and
        parts[0][0].isupper() and
        parts[1][0].isupper() and
        all(c.isalpha() or c in "-'" for c in parts[0] + parts[1])
    )


def _normalize_col_name(raw: str) -> str:
    """Convert matched pattern to a clean column name."""
    return re.sub(r'[\s\-]+', '_', raw.strip().lower())
