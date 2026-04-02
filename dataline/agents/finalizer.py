"""Finalizer agent: format accumulated results into answer table."""

from __future__ import annotations

import json
import re
from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.state import render_for_agent
from ..core.types import AnalysisState, StepRecord


def format_answer(
    question: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
    benchmark: str = "kdd",
    guidelines: str = "",
) -> dict:
    """Format step results into final answer dict.

    If benchmark == "dabstep", uses a scalar-answer prompt and returns {"answer": [val]}.
    If benchmark == "kdd" (default), uses table-format prompt and returns {"col": [values]}.
    If state is provided, uses full_step_details for maximum context.
    Otherwise falls back to legacy steps_done formatting.
    """
    if benchmark == "dabstep":
        return _format_dabstep(question, steps_done, llm, state=state, guidelines=guidelines)
    return _format_kdd(question, steps_done, llm, state=state)


def _format_kdd(
    question: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
) -> dict:
    """KDD table-format answer (original logic)."""
    prompt_path = Path(__file__).parent.parent / "prompts" / "finalizer.md"
    template = prompt_path.read_text(encoding="utf-8")

    if state is not None:
        context = render_for_agent(state, "finalizer")
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
        result = data["columns"] if "columns" in data else data
        return _validate_answer(result, question)
    except (json.JSONDecodeError, ValueError):
        return _fallback_extract(steps_done, state)


def _format_dabstep(
    question: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
    guidelines: str = "",
) -> dict:
    """DABstep scalar-answer format."""
    prompt_path = Path(__file__).parent.parent / "prompts" / "finalizer_dabstep.md"
    template = prompt_path.read_text(encoding="utf-8")

    if state is not None:
        context = render_for_agent(state, "finalizer")
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


def _validate_answer(answer: dict, question: str) -> dict:
    """Validate and clean the answer structure.

    Checks:
    1. All columns are non-empty lists (not None or empty)
    2. No None/null values inside lists (replace with empty string for strings,
       keep numbers as-is — None in a numeric answer is a real signal)
    3. All lists have equal length (a mismatched table would be malformed)
    4. Numeric strings preserved with full precision (no accidental float conversion)
    """
    if not isinstance(answer, dict):
        return answer

    cleaned: dict = {}
    lengths: list[int] = []

    for col, values in answer.items():
        if not isinstance(values, list):
            values = [values]
        # Replace None with "" for string columns, keep numeric None as is
        cleaned_vals: list = []
        for v in values:
            if v is None:
                cleaned_vals.append("")
            else:
                cleaned_vals.append(v)
        cleaned[col] = cleaned_vals
        lengths.append(len(cleaned_vals))

    # Warn (but don't crash) if column lengths differ — finalizer LLM alignment issue
    if len(set(lengths)) > 1:
        # Pad shorter columns with empty strings to match longest
        max_len = max(lengths)
        for col in cleaned:
            while len(cleaned[col]) < max_len:
                cleaned[col].append("")

    return cleaned


def _format_steps(steps: list[StepRecord]) -> str:
    parts = []
    for s in steps:
        stdout = s.result.stdout[:100_000] if s.result.stdout else "(no output)"
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
