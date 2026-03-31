"""Router agent: decide continue / backtrack / finish."""

from __future__ import annotations

import json
import re
from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.state import render_for_agent
from ..core.types import AnalysisState, RouterDecision, StepRecord, VerifierVerdict


def decide(
    question: str,
    steps_done: list[StepRecord],
    verdict: VerifierVerdict,
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
) -> RouterDecision:
    """Decide next action based on progress and verifier feedback.

    If state is provided, uses structured context rendering.
    Otherwise falls back to legacy steps_done formatting.
    """
    # Fast path: if verifier says sufficient, finish
    if verdict.sufficient:
        return RouterDecision(action="finish", reasoning="Verifier confirmed sufficiency")

    prompt_path = Path(__file__).parent.parent / "prompts" / "router.md"
    template = prompt_path.read_text(encoding="utf-8")

    if state is not None:
        context = render_for_agent(state, "router")
        effective_question = state.question
    else:
        context = _format_steps(steps_done)
        effective_question = question

    verifier_feedback = (
        f"sufficient: {verdict.sufficient}\n"
        f"reasoning: {verdict.reasoning}\n"
        f"missing: {verdict.missing}"
    )

    system_prompt = (
        template
        .replace("{question}", effective_question)
        .replace("{steps_summary}", context)
        .replace("{verifier_feedback}", verifier_feedback)
    )

    response = llm.chat(system_prompt, "Decide the next action now.")

    try:
        data = json.loads(_extract_json(response))
    except (json.JSONDecodeError, ValueError):
        return RouterDecision(action="continue", reasoning="Parse error, defaulting to continue")

    return RouterDecision(
        action=data.get("action", "continue"),
        truncate_to=data.get("truncate_to", 0),
        reasoning=data.get("reasoning", ""),
    )


def _format_steps(steps: list[StepRecord]) -> str:
    parts = []
    for s in steps:
        stdout = s.result.stdout[:500] if s.result.stdout else "(no output)"
        parts.append(f"Step {s.step_index}: {s.plan.step_description}\n  Output: {stdout}")
    return "\n\n".join(parts) if parts else "No steps."


def _extract_json(text: str) -> str:
    match = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return match.group(0)
    return text
