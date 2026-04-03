"""Judge agent: unified sufficiency check + routing + guidance.

Replaces the separate Verifier → Router pipeline with a single LLM call,
saving ~30% token cost per iteration while providing richer guidance.

Includes:
- Prompt inversion (list what's NOT done before deciding)
- Evidence-based reasoning (must quote stdout lines)
- Confidence scoring (gate for finish decisions)
- Structural notes injection (soft deterministic observations)
- Conditional adversarial second opinion (only on finish)
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.state import render_for_agent
from ..core.types import AnalysisState, JudgeDecision, StepRecord


# Confidence threshold: only accept "finish" when judge is this confident.
# Below this threshold, finish is downgraded to continue.
FINISH_CONFIDENCE_THRESHOLD = 0.8


def evaluate(
    question: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
    structural_notes: str = "",
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
        .replace("{structural_notes}", structural_notes or "None.")
    )

    response = llm.chat(system_prompt, "Evaluate progress and decide the next action now.")

    try:
        data = json.loads(_extract_json(response))
    except (json.JSONDecodeError, ValueError):
        return JudgeDecision(
            sufficient=False,
            action="continue",
            reasoning="Parse error, defaulting to continue",
            confidence=0.0,
        )

    # Parse confidence, clamp to [0, 1]
    raw_confidence = data.get("confidence", 0.0)
    try:
        confidence = max(0.0, min(1.0, float(raw_confidence)))
    except (TypeError, ValueError):
        confidence = 0.0

    return JudgeDecision(
        sufficient=data.get("sufficient", False),
        action=data.get("action", "continue"),
        reasoning=data.get("reasoning", ""),
        missing=data.get("missing", ""),
        guidance_for_next_step=data.get("guidance_for_next_step", ""),
        truncate_to=data.get("truncate_to", 0),
        confidence=confidence,
    )


def second_opinion(
    question: str,
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
    primary_reasoning: str = "",
    steps_done: list[StepRecord] | None = None,
) -> tuple[bool, str]:
    """Adversarial second opinion: only called when primary judge says finish.

    Returns (confirmed, reason). If not confirmed, reason explains the flaw.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "judge_second_opinion.md"
    template = prompt_path.read_text(encoding="utf-8")

    if state is not None:
        context = render_for_agent(state, "judge")
        effective_question = state.question
    else:
        context = _format_steps(steps_done or [])
        effective_question = question

    system_prompt = (
        template
        .replace("{question}", effective_question)
        .replace("{analysis_context}", context)
        .replace("{primary_reasoning}", primary_reasoning or "No reasoning provided.")
    )

    response = llm.chat(system_prompt, "Review the answer critically now.")

    try:
        data = json.loads(_extract_json(response))
    except (json.JSONDecodeError, ValueError):
        # Parse error → be conservative, don't confirm
        return False, "Second opinion parse error, defaulting to not confirmed"

    confirmed = data.get("confirm", False)
    flaws = data.get("flaws_found", "")
    fix = data.get("suggested_fix", "")

    reason = flaws if not confirmed else ""
    if fix and not confirmed:
        reason = f"{flaws} Fix: {fix}"

    return confirmed, reason


def compute_structural_notes(
    steps_done: list[StepRecord],
    latest_result: object | None = None,
) -> str:
    """Compute deterministic structural observations for judge context.

    These are objective facts, not judgments. The judge uses them as evidence.
    """
    notes: list[str] = []

    if not steps_done:
        notes.append("- No steps completed yet.")
        return "\n".join(notes)

    last_step = steps_done[-1]
    stdout = last_step.result.stdout if last_step.result.stdout else ""
    code = last_step.code

    # Observation 1: stdout length
    stdout_len = len(stdout.strip())
    if stdout_len == 0:
        notes.append("- Latest stdout is EMPTY (no output printed).")
    elif stdout_len < 20:
        notes.append(f"- Latest stdout is very short ({stdout_len} chars).")

    # Observation 2: zero-row indicators
    stdout_lower = stdout.lower()
    zero_indicators = ["0 rows", "0 matches", "empty dataframe", "no rows", "no matches"]
    found = [p for p in zero_indicators if p in stdout_lower]
    if found:
        notes.append(f"- Stdout contains zero-row indicators: {found}")

    # Observation 3: code pattern analysis
    explore_calls = [p for p in [".head(", ".describe(", ".info()", ".columns", ".dtypes", ".shape"]
                     if p in code]
    compute_calls = [p for p in [".mean(", ".sum(", ".count(", ".groupby(", ".merge(",
                                  ".agg(", ".value_counts(", ".pivot"]
                     if p in code]
    if explore_calls and not compute_calls:
        notes.append(f"- Latest code only has exploration calls: {explore_calls}. No computation detected.")
    elif compute_calls:
        notes.append(f"- Latest code has computation: {compute_calls}")

    # Observation 4: step count
    notes.append(f"- Total steps completed: {len(steps_done)}")

    # Observation 5: WARNING patterns
    if "warning" in stdout_lower:
        warning_lines = [l.strip() for l in stdout.splitlines()
                        if "warning" in l.lower()][:3]
        if warning_lines:
            notes.append(f"- Warnings in stdout: {warning_lines}")

    return "\n".join(notes) if notes else "None."


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
