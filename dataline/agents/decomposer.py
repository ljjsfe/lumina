"""Decomposer: single-purpose question decomposition with constraint isolation.

Runs once before QuestionAnalyzer. Breaks the question into sub-questions
with strictly isolated constraints — no constraint bleeding between sub-questions.

Cost: 1 LLM call. Validated by DIN-SQL: focused decomposition before synthesis
outperforms embedding decomposition inside a larger multi-task prompt.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from ..core.llm_client import LLMClient


@dataclass(frozen=True)
class SubQuestion:
    id: str
    description: str
    constraints: tuple[str, ...]
    data_source: str
    output_type: str  # scalar | list | table


@dataclass(frozen=True)
class DecomposedQuestion:
    sub_questions: tuple[SubQuestion, ...]
    raw_text: str  # full LLM response, injected into downstream context


def decompose(
    question: str,
    manifest_summary: str,
    domain_rules: str,
    llm: LLMClient,
) -> DecomposedQuestion:
    """Decompose question into sub-questions with isolated constraints.

    Intentionally minimal prompt — single-purpose focus improves quality.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "decomposer.md"
    template = prompt_path.read_text(encoding="utf-8")

    system_prompt = (
        template
        .replace("{question}", question)
        .replace("{manifest_summary}", manifest_summary)
        .replace("{domain_rules}", domain_rules or "(no domain documentation)")
    )

    response = llm.chat(system_prompt, "Decompose the question now.")
    sub_questions = _parse(response)
    return DecomposedQuestion(sub_questions=sub_questions, raw_text=response)


def _parse(response: str) -> tuple[SubQuestion, ...]:
    """Extract sub-questions from JSON response. Fail-open on parse error."""
    json_match = re.search(r"\{.*\}", response, re.DOTALL)
    if not json_match:
        return ()
    try:
        data = json.loads(json_match.group(0))
    except (json.JSONDecodeError, ValueError):
        return ()

    results = []
    for sq in data.get("sub_questions", []):
        results.append(SubQuestion(
            id=sq.get("id", "Q?"),
            description=sq.get("description", ""),
            constraints=tuple(sq.get("constraints", [])),
            data_source=sq.get("data_source", ""),
            output_type=sq.get("output_type", "scalar"),
        ))
    return tuple(results)
