"""QuestionAnalyzer: pre-execution analysis step (GSD discuss-phase).

Runs ONCE before the plan-code-verify loop. Analyzes the question against
the data and domain rules, producing a strategic analysis plan that
all downstream agents can reference.

Cost: 1 LLM call. Value: prevents planner from going in wrong direction.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.workspace import Workspace


@dataclass(frozen=True)
class AnswerSchema:
    """Machine-readable answer expectations from QuestionAnalyzer.

    Used as soft constraint by judge (structural checks) and finalizer (guidance).
    """
    sub_questions: tuple[str, ...] = ()
    expected_answer_type: str = "scalar"  # scalar | list | table
    expected_columns: tuple[str, ...] = ()
    required_steps_min: int = 2
    domain_rules_applied: tuple[str, ...] = ()


def analyze_question(
    question: str,
    manifest_summary: str,
    workspace: Workspace,
    llm: LLMClient,
    decomposition: str = "",
) -> str:
    """Analyze question and produce strategic analysis plan.

    Reads domain_rules and data_profile from workspace files.
    Accepts pre-committed decomposition from decomposer.py.
    Writes ANALYSIS_PLAN.md to workspace for observability.
    Returns the plan text.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "question_analyzer.md"
    template = prompt_path.read_text(encoding="utf-8")

    domain_rules = workspace.read_domain_rules()
    data_profile = workspace.read_data_profile()

    # Smart truncation for very long inputs
    if len(domain_rules) > 150_000:
        domain_rules = domain_rules[:150_000] + "\n... (truncated)"
    if len(data_profile) > 100_000:
        data_profile = data_profile[:100_000] + "\n... (truncated)"

    system_prompt = (
        template
        .replace("{question}", question)
        .replace("{decomposition}", decomposition or "(no decomposition available)")
        .replace("{domain_rules}", domain_rules or "(no documentation files found)")
        .replace("{manifest_summary}", manifest_summary)
        .replace("{data_profile}", data_profile or "(profiling failed)")
    )

    plan = llm.chat(system_prompt, "Analyze this question and produce the analysis plan now.")

    workspace.write_analysis_plan(plan)
    return plan


def parse_answer_schema(plan_text: str) -> AnswerSchema:
    """Extract ANSWER_SCHEMA JSON block from QA output.

    Returns default schema if parsing fails (fail-open — never blocks pipeline).
    """
    match = re.search(
        r"ANSWER_SCHEMA\s*\n\s*(\{.*?\})",
        plan_text,
        re.DOTALL,
    )
    if not match:
        # Try fallback: look for json block after ANSWER_SCHEMA marker
        match = re.search(
            r"```(?:json)?\s*\n\s*ANSWER_SCHEMA\s*\n\s*(\{.*?\})\s*\n\s*```",
            plan_text,
            re.DOTALL,
        )
    if not match:
        return AnswerSchema()

    try:
        data = json.loads(match.group(1))
    except (json.JSONDecodeError, ValueError):
        return AnswerSchema()

    return AnswerSchema(
        sub_questions=tuple(data.get("sub_questions", [])),
        expected_answer_type=data.get("expected_answer_type", "scalar"),
        expected_columns=tuple(data.get("expected_columns", [])),
        required_steps_min=max(1, int(data.get("required_steps_min", 2))),
        domain_rules_applied=tuple(data.get("domain_rules_applied", [])),
    )
