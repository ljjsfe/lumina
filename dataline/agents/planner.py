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

    If state is provided, builds context from state (no duplication).
    Otherwise falls back to legacy steps_done formatting.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "planner.md"
    template = prompt_path.read_text(encoding="utf-8")

    if state is not None:
        # State path: build full context, inject once via {context}
        context_parts = []
        context_parts.append(f"## Question\n{state.question}")

        if state.judge_guidance:
            context_parts.append(
                f"## Judge Guidance (MUST ADDRESS in this step)\n"
                f"> **{state.judge_guidance}**"
            )

        context_parts.append(f"## Data Sources\n{state.manifest_summary}")

        if state.domain_rules:
            context_parts.append(f"## Domain Rules (from documentation)\n{state.domain_rules}")

        if state.data_profile_summary:
            context_parts.append(f"## Data Profile\n{state.data_profile_summary}")

        # Execution state from render_for_agent (findings + completed steps only)
        exec_context = render_for_agent(state, "planner")
        if exec_context:
            context_parts.append(exec_context)

        context = "\n\n".join(context_parts)
        system_prompt = template.replace("{context}", context)
    else:
        # Legacy path: build context from individual arguments
        context_parts = []
        context_parts.append(f"## Question\n{question}")
        context_parts.append(f"## Data Sources\n{manifest_json}")
        if data_profile:
            context_parts.append(f"## Data Profile\n{data_profile}")
        context_parts.append(f"## Steps Completed\n{_format_steps(steps_done)}")
        context = "\n\n".join(context_parts)
        system_prompt = template.replace("{context}", context)

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
