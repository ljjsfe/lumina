"""Dynamic context budget engine.

Estimates question complexity and computes per-section budget allocation
for the 200K-token API context window. Replaces hardcoded budget_chars.

Budget calculation:
- API limit: 200K tokens
- Reserve: 5K (system prompt) + 8K (output) + 2K (safety) = 15K tokens
- Available: 185K tokens
- Char-to-token ratio: ~2.5 chars/token (conservative for mixed CN/EN)
- Max usable: ~462K chars

Complexity tiers:
- Simple: 230K chars (single filter/lookup)
- Medium: 350K chars (aggregation, formula)
- Complex: 462K chars (multi-source join + computation)
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from ..core.types import Manifest


@dataclass(frozen=True)
class ContextBudget:
    """Immutable budget allocation for a single task."""

    total_chars: int
    domain_rules_pct: float       # fraction of total for domain rules
    data_profile_pct: float       # fraction of total for data profile
    analysis_plan_pct: float      # fraction of total for analysis plan
    progress_pct: float           # fraction of total for progress/steps
    compact_trigger_chars: int    # trigger summarization when context exceeds this
    compact_target_chars: int     # target size after summarization
    complexity: str               # "simple" | "medium" | "complex"


# --- Complexity estimation ---

_SUB_QUESTION_SPLITTERS = re.compile(
    r"\band\b|\balso\b|\badditionally\b|;\s|\d+\)\s|\d+\.\s",
    re.IGNORECASE,
)


def estimate_complexity(
    question: str,
    manifest: Manifest,
    has_domain_rules: bool,
) -> str:
    """Classify question complexity based on structural signals.

    Returns "simple", "medium", or "complex".
    """
    score = 0

    # Sub-question count
    sub_questions = len(_SUB_QUESTION_SPLITTERS.split(question))
    if sub_questions >= 3:
        score += 2
    elif sub_questions >= 2:
        score += 1

    # Question length as proxy for complexity
    if len(question) > 300:
        score += 1

    # Number of structured data sources
    structured_types = {"csv", "json", "sqlite", "excel", "parquet"}
    source_count = sum(1 for e in manifest.entries if e.file_type in structured_types)
    if source_count >= 3:
        score += 2
    elif source_count >= 2:
        score += 1

    # Cross-source relations imply joins
    if manifest.cross_source_relations:
        score += 2

    # Domain rules + calculation keywords
    calc_keywords = {"average", "sum", "total", "ratio", "percentage", "rate",
                     "calculate", "compute", "formula", "difference", "change"}
    q_lower = question.lower()
    if has_domain_rules and any(kw in q_lower for kw in calc_keywords):
        score += 1

    # Aggregation keywords suggest multi-step
    agg_keywords = {"group by", "per ", "for each", "by ", "across", "compare"}
    if any(kw in q_lower for kw in agg_keywords):
        score += 1

    if score >= 5:
        return "complex"
    if score >= 2:
        return "medium"
    return "simple"


# --- Budget computation ---

# Chars available per complexity tier
_TIER_CHARS = {
    "simple": 230_000,
    "medium": 350_000,
    "complex": 462_000,
}


def compute_budget(
    complexity: str,
    has_domain_rules: bool,
    api_token_limit: int = 200_000,
) -> ContextBudget:
    """Compute context budget for the given complexity and API limits.

    The api_token_limit parameter is kept for future-proofing but the
    char tiers are pre-calculated for the 200K default.
    """
    total = _TIER_CHARS.get(complexity, _TIER_CHARS["medium"])

    # Adjust section ratios based on whether domain docs exist
    if has_domain_rules:
        domain_pct = 0.30
        profile_pct = 0.20
    else:
        domain_pct = 0.05
        profile_pct = 0.35

    return ContextBudget(
        total_chars=total,
        domain_rules_pct=domain_pct,
        data_profile_pct=profile_pct,
        analysis_plan_pct=0.15,
        progress_pct=0.15,
        compact_trigger_chars=int(total * 0.60),
        compact_target_chars=int(total * 0.40),
        complexity=complexity,
    )
