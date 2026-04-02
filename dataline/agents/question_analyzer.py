"""QuestionAnalyzer: pre-execution analysis step (GSD discuss-phase).

Runs ONCE before the plan-code-verify loop. Analyzes the question against
the data and domain rules, producing a strategic analysis plan that
all downstream agents can reference.

Cost: 1 LLM call. Value: prevents planner from going in wrong direction.
"""

from __future__ import annotations

from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.workspace import Workspace


def analyze_question(
    question: str,
    manifest_summary: str,
    workspace: Workspace,
    llm: LLMClient,
) -> str:
    """Analyze question and produce strategic analysis plan.

    Reads domain_rules and data_profile from workspace files.
    If prior progress exists (replan scenario), includes it for context.
    Writes ANALYSIS_PLAN.md to workspace.
    Returns the plan text.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "question_analyzer.md"
    template = prompt_path.read_text(encoding="utf-8")

    domain_rules = workspace.read_domain_rules()
    data_profile = workspace.read_data_profile()
    prior_progress = workspace.read_progress()

    # Smart truncation for very long inputs
    if len(domain_rules) > 150_000:
        domain_rules = domain_rules[:150_000] + "\n... (truncated)"
    if len(data_profile) > 100_000:
        data_profile = data_profile[:100_000] + "\n... (truncated)"

    # Inject prior progress for replan scenarios
    effective_question = question
    if prior_progress:
        effective_question = (
            f"{question}\n\n"
            f"## REPLAN CONTEXT: Prior Attempts and Findings\n"
            f"The previous analysis direction was incorrect. Here is what was tried and discovered:\n\n"
            f"{prior_progress[:30_000]}\n\n"
            f"Use these findings to avoid repeating the same mistakes. "
            f"Choose a fundamentally different approach."
        )

    system_prompt = (
        template
        .replace("{question}", effective_question)
        .replace("{domain_rules}", domain_rules or "(no documentation files found)")
        .replace("{manifest_summary}", manifest_summary)
        .replace("{data_profile}", data_profile or "(profiling failed)")
    )

    plan = llm.chat(system_prompt, "Analyze this question and produce the analysis plan now.")

    workspace.write_analysis_plan(plan)
    return plan
