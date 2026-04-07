"""Finalizer agent: format accumulated results into answer table."""

from __future__ import annotations

import json
import re
from pathlib import Path

from ..core.context_manager import ContextManager, Section
from ..core.llm_client import LLMClient

from ..core.token_estimator import cap_text
from ..core.types import AnalysisState, StepRecord


def format_answer(
    question: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
    cm: ContextManager | None = None,
    benchmark: str = "kdd",
    guidelines: str = "",
) -> dict:
    """Format step results into final answer dict.

    If benchmark == "dabstep", uses a scalar-answer prompt and returns {"answer": [val]}.
    If benchmark == "kdd" (default), uses table-format prompt and returns {"col": [values]}.
    If state + cm are provided, uses budget-managed context via ContextManager.
    """
    if benchmark == "dabstep":
        return _format_dabstep(question, steps_done, llm, state=state, cm=cm, guidelines=guidelines)
    return _format_kdd(question, steps_done, llm, state=state, cm=cm)


def _format_kdd(
    question: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
    cm: ContextManager | None = None,
) -> dict:
    """KDD table-format answer (original logic).

    Fast path: if the last successful step's stdout already contains a clean
    structured answer (JSON dict/list, or a simple printed table), extract it
    directly without an LLM call.  This avoids the LLM rewriting numbers
    (precision loss) and reduces cost.
    """
    # --- Fast path: try direct extraction from stdout ---
    direct = _try_direct_extract(steps_done, state)
    if direct is not None:
        return direct

    # --- Slow path: LLM formatting ---
    prompt_path = Path(__file__).parent.parent / "prompts" / "finalizer.md"
    template = prompt_path.read_text(encoding="utf-8")

    if state is not None and cm is not None:
        sections = _build_sections(state)
        context = cm.assemble(sections, llm=llm)
        system_prompt = (
            template
            .replace("{question}", state.question)
            .replace("{steps_summary}", context)
        )
    else:
        steps_summary = _format_steps(steps_done)
        system_prompt = (
            template
            .replace("{question}", question)
            .replace("{steps_summary}", steps_summary)
        )

    response = llm.chat(system_prompt, "Format the final answer now.")

    try:
        data = json.loads(_extract_json(response))
        if "columns" in data:
            return data["columns"]
        return data
    except (json.JSONDecodeError, ValueError):
        return _fallback_extract(steps_done, state)


def _format_dabstep(
    question: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
    cm: ContextManager | None = None,
    guidelines: str = "",
) -> dict:
    """DABstep scalar-answer format.

    Fast path: if the last successful step's stdout contains a single clear
    scalar value (number, short string), extract it directly.
    """
    # --- Fast path: try direct scalar extraction ---
    direct_scalar = _try_direct_scalar_extract(steps_done, state)
    if direct_scalar is not None:
        return {"answer": [direct_scalar]}

    # --- Slow path: LLM formatting ---
    prompt_path = Path(__file__).parent.parent / "prompts" / "finalizer_dabstep.md"
    template = prompt_path.read_text(encoding="utf-8")

    if state is not None and cm is not None:
        sections = _build_sections(state)
        context = cm.assemble(sections, llm=llm)
        system_prompt = (
            template
            .replace("{question}", state.question)
            .replace("{guidelines}", guidelines)
            .replace("{steps_summary}", context)
        )
    else:
        steps_summary = _format_steps(steps_done)
        system_prompt = (
            template
            .replace("{question}", question)
            .replace("{guidelines}", guidelines)
            .replace("{steps_summary}", steps_summary)
        )

    response = llm.chat(system_prompt, "Format the final answer now.")

    try:
        data = json.loads(_extract_json(response))
        scalar_val = data.get("answer", "")
        return {"answer": [scalar_val]}
    except (json.JSONDecodeError, ValueError):
        # Try to use raw response as scalar answer
        cleaned = response.strip()
        if cleaned:
            return {"answer": [cleaned]}
        return _fallback_extract(steps_done, state)


def _last_successful_stdout(
    steps_done: list[StepRecord],
    state: AnalysisState | None,
) -> str | None:
    """Return stdout from the last step with return_code == 0, or None."""
    details = state.full_step_details if state is not None else tuple(steps_done)
    for step in reversed(details):
        if step.result.return_code == 0 and step.result.stdout.strip():
            return step.result.stdout.strip()
    return None


def _try_direct_extract(
    steps_done: list[StepRecord],
    state: AnalysisState | None,
) -> dict | None:
    """Try to extract a structured answer directly from the last successful stdout.

    Returns a dict {"col": [values]} on success, None if LLM formatting is needed.

    Only fires on unambiguous JSON structures — never guesses from free-text output.
    Rejects stdout that looks like a raw DataFrame printout (aligned columns + index).
    """
    stdout = _last_successful_stdout(steps_done, state)
    if stdout is None:
        return None

    # Reject raw DataFrame printouts: they look like "  col1  col2\n0  val  val"
    # The LLM must extract actual values, not copy a formatted table.
    if _looks_like_dataframe_printout(stdout):
        return None

    # Strategy: try parsing as JSON at two granularities (whole stdout, last line).
    # Only accept dict-of-lists shapes that look like a table answer.
    for candidate in _json_candidates(stdout):
        try:
            data = json.loads(candidate)
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        if "columns" in data and isinstance(data["columns"], dict):
            return data["columns"]
        if all(isinstance(v, list) for v in data.values()) and data:
            return data

    return None


def _looks_like_dataframe_printout(stdout: str) -> bool:
    """Heuristic: return True if stdout looks like a pandas DataFrame printout.

    DataFrames have: integer index column on the left, aligned spacing, multiple rows.
    This guards against the fast path copying a raw table repr instead of extracted values.
    """
    lines = [ln for ln in stdout.splitlines() if ln.strip()]
    if len(lines) < 3:
        return False
    # Look for lines starting with integer index (e.g. "0  ", "1  ", "10  ")
    index_lines = sum(1 for ln in lines if re.match(r"^\s*\d+\s{2,}", ln))
    return index_lines >= 2


def _try_direct_scalar_extract(
    steps_done: list[StepRecord],
    state: AnalysisState | None,
) -> str | None:
    """Try to extract a scalar answer directly from the last successful stdout.

    Returns the scalar value as a string, or None if LLM formatting is needed.

    Only fires on unambiguous JSON {"answer": ...} — never guesses from free text.
    """
    stdout = _last_successful_stdout(steps_done, state)
    if stdout is None:
        return None

    for candidate in _json_candidates(stdout):
        try:
            data = json.loads(candidate)
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(data, dict) and "answer" in data:
            return str(data["answer"])

    return None


def _json_candidates(stdout: str) -> list[str]:
    """Yield JSON candidate strings from stdout: whole text, then last non-empty line."""
    candidates = [stdout]
    lines = [ln for ln in stdout.split("\n") if ln.strip()]
    if lines and lines[-1].strip() != stdout:
        candidates.append(lines[-1].strip())
    return candidates


def _build_sections(state: AnalysisState) -> list[Section]:
    """Build prioritized sections for finalizer context.

    Excludes question (already in template {question}).
    """
    sections: list[Section] = []

    if state.domain_rules:
        sections.append(Section(
            "domain_rules", state.domain_rules,
            priority=80, heading="## Domain Rules",
        ))

    # Step results: split into earlier steps (compressible) and last step (non-compressible).
    # The last step's output contains the final answer — must not be compressed.
    if state.full_step_details:
        if len(state.full_step_details) > 1:
            earlier_parts = []
            for s in state.full_step_details[:-1]:
                stdout = cap_text(s.result.stdout) if s.result.stdout else "(no output)"
                earlier_parts.append(
                    f"Step {s.step_index}: {s.plan.step_description}\n"
                    f"  Output:\n{stdout}"
                )
            sections.append(Section(
                "earlier_step_results", "\n\n".join(earlier_parts),
                priority=75, heading="## Earlier Step Results",
            ))

        last = state.full_step_details[-1]
        last_stdout = cap_text(last.result.stdout) if last.result.stdout else "(no output)"
        sections.append(Section(
            "last_step_result",
            f"Step {last.step_index}: {last.plan.step_description}\n"
            f"  Output:\n{last_stdout}",
            priority=95, compressible=False,
            heading="## Latest Step Result (primary answer source)",
        ))

    if state.key_findings:
        sections.append(Section(
            "key_findings",
            "\n".join(f"- {f}" for f in state.key_findings),
            priority=70, heading="## Key Findings",
        ))

    return sections


def _format_steps(steps: list[StepRecord]) -> str:
    parts = []
    for s in steps:
        stdout = s.result.stdout[:1500] if s.result.stdout else "(no output)"
        parts.append(
            f"Step {s.step_index}: {s.plan.step_description}\n"
            f"  Output:\n{stdout}"
        )
    return "\n\n".join(parts) if parts else "No steps."


def _extract_json(text: str) -> str:
    match = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return match.group(0)
    return text


def _fallback_extract(
    steps_done: list[StepRecord],
    state: AnalysisState | None = None,
) -> dict:
    """Try to extract answer from last step's stdout."""
    details = state.full_step_details if state is not None else tuple(steps_done)
    if not details:
        return {}
    last_stdout = details[-1].result.stdout
    try:
        return json.loads(last_stdout)
    except (json.JSONDecodeError, ValueError):
        pass
    return {"answer": [last_stdout.strip()]}
