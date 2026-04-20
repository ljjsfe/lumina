"""Skeptic: adversarial evaluator for final answers.

Inspired by Anthropic's harness design (2026-04): agents praise their own work
even when quality is mediocre. The fix is a separate evaluator with adversarial
framing that looks for what's WRONG, not what's right.

Design principles:
- ONE extra LLM call per task (bounded cost, 200 tokens max response)
- Adversarial framing: "Find what's wrong" not "Check if it's right"
- Sees ONLY question + final answer (not code/reasoning — prevents rationalization)
- Structured JSON output: {likely_wrong: bool, concern: str}
- Fail-open: any failure returns likely_wrong=False (never blocks a good answer)
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_SKEPTIC_SYSTEM_PROMPT = """You are an adversarial reviewer for data-analysis answers.

Your job is NOT to confirm the answer is right. Your job is to find what might be wrong.

Look for:
- Wrong column selected (e.g., asked for event.type but got budget.category)
- Wrong aggregation (COUNT vs SUM, MIN vs MAX, average vs total)
- Wrong output shape (extra columns, missing rows, wrong row count)
- Wrong number of values (asked for 4 things, got 3)
- Answer to a sibling question rather than the exact question asked
- Wrong unit/scale (percentage vs ratio, thousands vs millions)
- Answer is just schema/metadata, not actual computed values
- Single scalar when a list was requested, or vice versa

Be skeptical. If the answer looks plausible but you're not 100% sure it matches the question, flag it.

If after careful review the answer is clearly correct, return likely_wrong: false. Do not hedge.

Return STRICT JSON only, no prose:
{
  "likely_wrong": true|false,
  "concern": "<one-sentence specific concern, or empty string if likely_wrong is false>"
}"""


def check(
    question: str,
    answer: str,
    llm: Any,
    max_tokens: int = 200,
) -> dict[str, Any]:
    """Run adversarial evaluation on a formed answer.

    Args:
        question: Original task question.
        answer: Formed answer (CSV/JSON representation).
        llm: LLM client with .chat() method.
        max_tokens: Response budget (200 is plenty for structured output).

    Returns:
        {"likely_wrong": bool, "concern": str, "raw": str|None}

    Fail-open: any error returns likely_wrong=False.
    """
    if not answer or not answer.strip():
        return {"likely_wrong": False, "concern": "empty answer, skeptic skipped", "raw": None}

    # Format answer for display
    answer_display = _format_answer_for_review(answer)

    user_prompt = (
        f"QUESTION:\n{question}\n\n"
        f"PROPOSED ANSWER:\n{answer_display}\n\n"
        "Is this answer likely wrong? Return strict JSON."
    )

    try:
        raw = llm.chat(
            system=_SKEPTIC_SYSTEM_PROMPT,
            user=user_prompt,
            max_tokens=max_tokens,
        )
    except Exception as e:
        logger.warning("Skeptic LLM error: %s", e)
        return {"likely_wrong": False, "concern": f"skeptic LLM error: {e}", "raw": None}

    parsed = _extract_json(raw)
    if not parsed or "likely_wrong" not in parsed:
        logger.warning("Skeptic response unparseable: %s", raw[:200])
        return {"likely_wrong": False, "concern": "skeptic response unparseable", "raw": raw}

    likely_wrong = bool(parsed.get("likely_wrong", False))
    concern = str(parsed.get("concern", "") or "")

    logger.info("Skeptic verdict: likely_wrong=%s, concern='%s'", likely_wrong, concern)
    return {"likely_wrong": likely_wrong, "concern": concern, "raw": raw}


def _format_answer_for_review(answer: str) -> str:
    """Format answer dict/JSON as readable text for the skeptic."""
    # If answer is already a string, use as-is
    if not answer.startswith("{"):
        return answer

    try:
        data = json.loads(answer)
        if isinstance(data, dict):
            # Format as a simple table representation
            lines = []
            if "columns" in data:
                data = data["columns"]
            for col, values in data.items():
                if isinstance(values, list):
                    lines.append(f"{col}: {values}")
                else:
                    lines.append(f"{col}: {values}")
            return "\n".join(lines)
    except (json.JSONDecodeError, TypeError):
        pass

    return answer


def _extract_json(text: str) -> dict | None:
    """Extract JSON object from LLM output, tolerating markdown fences."""
    if not text:
        return None
    # Strip markdown fences
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    candidate = m.group(1) if m else text
    # Find first {...} block
    if not candidate.strip().startswith("{"):
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start == -1 or end == -1 or end < start:
            return None
        candidate = candidate[start:end + 1]
    try:
        return json.loads(candidate)
    except (json.JSONDecodeError, ValueError):
        return None
