"""Planner agent: incremental single-step planning."""

from __future__ import annotations

import json
from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.state import render_for_agent
from ..core.types import AnalysisState, PlanStep, StepRecord


def plan_next(
    question: str,
    manifest_json: str,
    data_profile: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
) -> PlanStep:
    """Plan the next single step based on question and prior results.

    If state is provided, uses structured context rendering.
    Otherwise falls back to legacy steps_done formatting.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "planner.md"
    template = prompt_path.read_text(encoding="utf-8")

    if state is not None:
        context = render_for_agent(state, "planner")
        system_prompt = (
            template
            .replace("{question}", state.question)
            .replace("{manifest_json}", state.manifest_summary)
            .replace("{data_profile}", state.data_profile_summary[:10000])
            .replace("{steps_done_summary}", context)
        )
    else:
        steps_summary = _format_steps(steps_done)
        system_prompt = (
            template
            .replace("{question}", question)
            .replace("{manifest_json}", manifest_json)
            .replace("{data_profile}", data_profile[:10000])
            .replace("{steps_done_summary}", steps_summary)
        )

    response = llm.chat(system_prompt, "Plan the next step now.")

    try:
        plan_data = json.loads(_extract_json(response))
    except (json.JSONDecodeError, ValueError):
        plan_data = {
            "step_description": response[:500],
            "data_sources": [],
            "depends_on_prior": bool(steps_done) or (state is not None and state.completed_steps),
            "expected_output": "analysis result",
        }

    return PlanStep(
        step_description=plan_data.get("step_description", ""),
        data_sources=tuple(plan_data.get("data_sources", [])),
        depends_on_prior=plan_data.get("depends_on_prior", False),
        expected_output=plan_data.get("expected_output", ""),
    )


def _format_steps(steps: list[StepRecord]) -> str:
    if not steps:
        return "No steps completed yet."
    parts = []
    for s in steps:
        stdout_preview = s.result.stdout[:500] if s.result.stdout else "(no output)"
        parts.append(
            f"Step {s.step_index}: {s.plan.step_description}\n"
            f"  Result: {stdout_preview}"
        )
    return "\n\n".join(parts)


def _extract_json(text: str) -> str:
    """Extract JSON from response, handling markdown wrapping."""
    import re
    match = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return match.group(0)
    return text
