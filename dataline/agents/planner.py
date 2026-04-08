"""Planner agent: incremental single-step planning."""

from __future__ import annotations

import json
from pathlib import Path

from ..core.context_manager import ContextManager, Section
from ..core.llm_client import LLMClient

from ..core.types import AnalysisState, PlanStep, StepRecord


def plan_next(
    question: str,
    manifest_json: str,
    data_profile: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
    cm: ContextManager | None = None,
) -> PlanStep:
    """Plan the next single step based on question and prior results.

    If state + cm are provided, builds budget-managed context via ContextManager.
    If only state, builds context directly (legacy).
    Otherwise falls back to legacy steps_done formatting.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "planner.md"
    template = prompt_path.read_text(encoding="utf-8")

    if state is not None and cm is not None:
        # Budget-managed context via ContextManager
        sections = _build_sections(state)
        context = cm.assemble(sections, llm=llm)
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


def _build_sections(state: AnalysisState) -> list[Section]:
    """Build prioritized sections for planner context."""
    sections: list[Section] = [
        Section("question", state.question, priority=100,
                compressible=False, heading="## Question"),
    ]

    if state.judge_guidance:
        sections.append(Section(
            "judge_guidance", f"> **{state.judge_guidance}**",
            priority=95, compressible=False,
            heading="## Judge Guidance (MUST ADDRESS in this step)",
        ))

    sections.append(Section(
        "manifest", state.manifest_summary,
        priority=75, heading="## Data Sources",
    ))

    if state.domain_rules:
        sections.append(Section(
            "domain_rules", state.domain_rules,
            priority=80, heading="## Domain Rules (from documentation)",
        ))

    if state.question_analysis:
        sections.append(Section(
            "question_analysis", state.question_analysis,
            priority=50, compressible=True,
            heading="## Question Analysis (pre-execution strategy — adapt based on actual data)",
        ))

    if state.data_profile_summary:
        sections.append(Section(
            "data_profile", state.data_profile_summary,
            priority=50, heading="## Data Profile",
        ))

    if state.key_findings:
        sections.append(Section(
            "key_findings",
            "\n".join(f"- {f}" for f in state.key_findings),
            priority=70, heading="## Key Findings So Far",
        ))

    if state.completed_steps:
        sections.append(Section(
            "completed_steps",
            "\n".join(state.completed_steps),
            priority=60, heading="## Completed Steps",
        ))

    return sections


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
