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
    candidate_columns: tuple[str, ...] = ()  # column names from manifest for output schema


@dataclass(frozen=True)
class DecomposedQuestion:
    sub_questions: tuple[SubQuestion, ...]
    raw_text: str  # full LLM response, injected into downstream context
    validation_warnings: tuple[str, ...] = ()  # non-fatal warnings about data source refs


def decompose(
    question: str,
    manifest_summary: str,
    domain_rules: str,
    llm: LLMClient,
) -> DecomposedQuestion:
    """Decompose question into sub-questions with isolated constraints.

    Intentionally minimal prompt — single-purpose focus improves quality.
    Validates that referenced data sources match manifest files (non-blocking).
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
    warnings = _validate_data_sources(sub_questions, manifest_summary)
    return DecomposedQuestion(
        sub_questions=sub_questions,
        raw_text=response,
        validation_warnings=tuple(warnings),
    )


def _validate_data_sources(
    sub_questions: tuple[SubQuestion, ...],
    manifest_summary: str,
) -> list[str]:
    """Check that sub-question data_source fields reference files in the manifest.

    Non-blocking: returns warning strings only. Does not raise.
    """
    if not sub_questions:
        return []
    known_files = set(re.findall(
        r'\b[\w\-]+\.(?:csv|sqlite|db|json|parquet|xlsx?|md|pdf|docx)\b',
        manifest_summary,
        re.IGNORECASE,
    ))
    if not known_files:
        return []
    warnings = []
    for sq in sub_questions:
        ds_lower = sq.data_source.lower()
        if not any(fn.lower() in ds_lower for fn in known_files):
            warnings.append(
                f"{sq.id}: data_source '{sq.data_source}' does not match "
                f"any manifest file: {sorted(known_files)}"
            )
    return warnings


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
            candidate_columns=tuple(sq.get("candidate_columns", [])),
        ))
    return tuple(results)
