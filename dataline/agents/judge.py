"""Judge agent: unified sufficiency check + routing + guidance.

Replaces the separate Verifier → Router pipeline with a single LLM call,
saving ~30% token cost per iteration while providing richer guidance.

The old verifier.py and router.py are kept for backward compatibility
but are no longer called by the orchestrator.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.state import render_for_agent
from ..core.types import AnalysisState, JudgeDecision, StepRecord


def evaluate(
    question: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
) -> JudgeDecision:
    """Evaluate progress and decide next action in a single LLM call.

    If state is provided, uses structured context rendering.
    Otherwise falls back to legacy steps_done formatting.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "judge.md"
    template = prompt_path.read_text(encoding="utf-8")

    if state is not None:
        context = render_for_agent(state, "judge")
        effective_question = state.question
    else:
        context = _format_steps(steps_done)
        effective_question = question

    system_prompt = (
        template
        .replace("{question}", effective_question)
        .replace("{analysis_context}", context)
    )

    response = llm.chat(system_prompt, "Evaluate progress and decide the next action now.")

    try:
        data = json.loads(_extract_json(response))
    except (json.JSONDecodeError, ValueError):
        return JudgeDecision(
            sufficient=False,
            action="continue",
            reasoning="Parse error, defaulting to continue",
        )

    return JudgeDecision(
        sufficient=data.get("sufficient", False),
        action=data.get("action", "continue"),
        reasoning=data.get("reasoning", ""),
        missing=data.get("missing", ""),
        guidance_for_next_step=data.get("guidance_for_next_step", ""),
        truncate_to=data.get("truncate_to", 0),
    )


def _format_steps(steps: list[StepRecord]) -> str:
    """Legacy formatting when AnalysisState is not available."""
    if not steps:
        return "No steps completed."
    parts: list[str] = []
    for s in steps:
        stdout = s.result.stdout[:100_000] if s.result.stdout else "(no output)"
        status = "OK" if s.result.return_code == 0 else f"ERROR (rc={s.result.return_code})"
        parts.append(
            f"Step {s.step_index}: {s.plan.step_description}\n"
            f"  Status: {status}\n"
            f"  Output: {stdout}"
        )
    return "\n\n".join(parts)


def _extract_json(text: str) -> str:
    """Extract JSON from response, handling markdown wrapping."""
    match = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return match.group(0)
    return text
