"""Coder agent: convert plan step to executable Python code."""

from __future__ import annotations

import json
import re
from pathlib import Path

from ..core.context_manager import ContextManager, Section
from ..core.llm_client import LLMClient

from ..core.token_estimator import cap_text
from ..core.types import AnalysisState, PlanStep, StepRecord


def generate(
    plan_step: PlanStep,
    manifest_json: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
    cm: ContextManager | None = None,
) -> str:
    """Generate Python code for a plan step.

    If state + cm are provided, uses budget-managed context via ContextManager.
    If only state, uses structured context rendering (legacy).
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

    if state is not None and cm is not None:
        # Budget-managed context via ContextManager
        # Note: manifest goes into template {manifest_json} directly,
        # so CM sections exclude manifest to avoid duplication.
        sections = _build_sections(state)
        context = cm.assemble(sections, llm=llm)
        system_prompt = (
            template
            .replace("{plan_step}", plan_json)
            .replace("{manifest_json}", state.manifest_summary)
            .replace("{prior_results_summary}", context)
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


def _build_sections(state: AnalysisState) -> list[Section]:
    """Build prioritized sections for coder context.

    Excludes manifest_summary (already in template {manifest_json}).
    """
    sections: list[Section] = []

    if state.domain_rules:
        sections.append(Section(
            "domain_rules", state.domain_rules,
            priority=80, heading="## Domain Rules (from documentation)",
        ))

    if state.question_analysis:
        sections.append(Section(
            "question_analysis", state.question_analysis,
            priority=55, compressible=True,
            heading="## Question Analysis (expected approach — adapt if data differs from expectations)",
        ))

    if state.data_profile_summary:
        sections.append(Section(
            "data_profile", state.data_profile_summary,
            priority=50, heading="## Data Profile (sample rows, value distributions)",
        ))

    if state.variables_in_scope:
        vars_text = "\n".join(
            f"- {name}: {desc}" for name, desc in state.variables_in_scope
        )
        sections.append(Section(
            "variables", vars_text,
            priority=85, compressible=False,
            heading="## Available Variables (in TEMP_DIR)",
        ))

    # Last 2 steps with full output for coding context
    recent = state.full_step_details[-2:] if state.full_step_details else ()
    if recent:
        parts = []
        for s in recent:
            stdout = cap_text(s.result.stdout) if s.result.stdout else "(no output)"
            parts.append(f"Step {s.step_index} ({s.plan.step_description}):\n{stdout}")
        sections.append(Section(
            "recent_results", "\n\n".join(parts),
            priority=65, heading="## Recent Results",
        ))

    return sections


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
        stdout = s.result.stdout[:800] if s.result.stdout else "(no output)"
        parts.append(f"Step {s.step_index} ({s.plan.step_description}):\n{stdout}")
    return "\n\n".join(parts)
