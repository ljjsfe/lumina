"""Verifier agent: check if accumulated results are sufficient."""

from __future__ import annotations

import json
import re
from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.state import render_for_agent
from ..core.types import AnalysisState, StepRecord, VerifierVerdict


def check(
    question: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
) -> VerifierVerdict:
    """Check if results are sufficient to answer the question.

    If state is provided, uses structured context rendering.
    Otherwise falls back to legacy steps_done formatting.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "verifier.md"
    template = prompt_path.read_text(encoding="utf-8")

    if state is not None:
        context = render_for_agent(state, "verifier")
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

    response = llm.chat(system_prompt, "Verify sufficiency now.")

    try:
        data = json.loads(_extract_json(response))
    except (json.JSONDecodeError, ValueError):
        return VerifierVerdict(sufficient=False, reasoning=response[:300], missing="Parse error")

    return VerifierVerdict(
        sufficient=data.get("sufficient", False),
        reasoning=data.get("reasoning", ""),
        missing=data.get("missing", ""),
    )


def _format_steps(steps: list[StepRecord]) -> str:
    parts = []
    for s in steps:
        stdout = s.result.stdout[:800] if s.result.stdout else "(no output)"
        status = "OK" if s.result.return_code == 0 else f"ERROR (rc={s.result.return_code})"
        parts.append(
            f"Step {s.step_index}: {s.plan.step_description}\n"
            f"  Status: {status}\n"
            f"  Output: {stdout}"
        )
    return "\n\n".join(parts) if parts else "No steps completed."


def _extract_json(text: str) -> str:
    match = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return match.group(0)
    return text
