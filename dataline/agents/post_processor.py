"""Deterministic post-processor for finalizer output.

Conservative structural fix: if the last step's stdout contained a
JSON dict with more columns than the finalizer returned, re-extract
from that JSON directly.

No heuristic column splitting (names, scores, etc.) — those are
task-specific and overfit to particular benchmarks. The general fix
for column merging is the save_result() structured path (bypasses
LLM entirely) or JSON re-extraction from stdout.
"""

from __future__ import annotations

import json

from ..core.types import AnalysisState


def post_process(answer: dict, state: AnalysisState | None = None) -> dict:
    """Re-extract from stdout JSON if it has more columns than the current answer.

    Only fires when stdout has unambiguous JSON with more keys — never guesses
    column names or applies pattern-based splits.
    """
    if not answer or state is None:
        return answer

    stdout = _get_last_stdout(state)
    if not stdout:
        return answer

    return _try_json_reextract(answer, stdout)


def _get_last_stdout(state: AnalysisState) -> str:
    for step in reversed(state.full_step_details):
        if step.result.return_code == 0 and step.result.stdout.strip():
            return step.result.stdout
    return ""


def _try_json_reextract(answer: dict, stdout: str) -> dict:
    """Re-extract answer from stdout JSON if it has more columns than current answer.

    Checks two shapes:
    - {"col": [values], ...}           — direct column dict
    - {"columns": {"col": [values]}}   — wrapped column dict
    """
    for candidate in _json_candidates(stdout):
        try:
            data = json.loads(candidate)
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(data, dict):
            continue

        # Wrapped shape
        if "columns" in data and isinstance(data["columns"], dict):
            cols = data["columns"]
            if (len(cols) > len(answer) and
                    all(isinstance(v, list) for v in cols.values())):
                return cols

        # Direct shape
        if (len(data) > len(answer) and
                all(isinstance(v, list) for v in data.values())):
            return data

    return answer


def _json_candidates(stdout: str) -> list[str]:
    candidates = [stdout]
    lines = [ln for ln in stdout.split("\n") if ln.strip()]
    if lines and lines[-1].strip() != stdout:
        candidates.append(lines[-1].strip())
    return candidates
