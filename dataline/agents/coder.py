"""Coder agent: convert plan step to executable Python code."""

from __future__ import annotations

import json
import re
from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.state import render_for_agent
from ..core.types import AnalysisState, PlanStep, StepRecord


def generate(
    plan_step: PlanStep,
    manifest_json: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
) -> str:
    """Generate Python code for a plan step.

    If state is provided, uses structured context rendering.
    Otherwise falls back to legacy steps_done formatting.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "coder.md"
    template = prompt_path.read_text(encoding="utf-8")

    plan_json = json.dumps({
        "step_description": plan_step.step_description,
        "data_sources": list(plan_step.data_sources),
        "depends_on_prior": plan_step.depends_on_prior,
        "expected_output": plan_step.expected_output,
    }, ensure_ascii=False)

    if state is not None:
        context = render_for_agent(state, "coder")
        system_prompt = (
            template
            .replace("{plan_step}", plan_json)
            .replace("{manifest_json}", state.manifest_summary)
            .replace("{prior_results_summary}", context)  # includes data profile + recent results
        )
    else:
        prior_summary = _format_prior_results(steps_done)
        system_prompt = (
            template
            .replace("{plan_step}", plan_json)
            .replace("{manifest_json}", manifest_json)
            .replace("{prior_results_summary}", prior_summary)
        )

    response = llm.chat(system_prompt, "Generate the Python code now.")
    return _extract_code(response)


def _extract_code(response: str) -> str:
    """Extract Python code from markdown code blocks."""
    match = re.search(r"```python\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"```\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    return response.strip()


def _format_prior_results(steps: list[StepRecord]) -> str:
    if not steps:
        return "No prior steps."
    parts = []
    for s in steps:
        stdout = s.result.stdout[:100_000] if s.result.stdout else "(no output)"
        parts.append(f"Step {s.step_index} ({s.plan.step_description}):\n{stdout}")
    return "\n\n".join(parts)
