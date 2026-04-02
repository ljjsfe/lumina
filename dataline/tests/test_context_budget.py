"""Tests for context budget engine."""

import pytest

from dataline.core.context_budget import ContextBudget, compute_budget, estimate_complexity
from dataline.core.types import CrossSourceRelation, Manifest, ManifestEntry


def _make_manifest(
    n_csv: int = 1,
    n_sqlite: int = 0,
    cross_relations: int = 0,
) -> Manifest:
    entries = []
    for i in range(n_csv):
        entries.append(ManifestEntry(
            file_path=f"file_{i}.csv", file_type="csv", size_bytes=100,
            summary={"columns": [{"name": "col1"}], "row_count": 10},
        ))
    for i in range(n_sqlite):
        entries.append(ManifestEntry(
            file_path=f"db_{i}.sqlite", file_type="sqlite", size_bytes=200,
            summary={"tables": [{"name": "t", "columns": [{"name": "id"}]}]},
        ))
    relations = tuple(
        CrossSourceRelation(source_a=f"a_{i}", source_b=f"b_{i}", relation="shared", confidence=0.8)
        for i in range(cross_relations)
    )
    return Manifest(entries=tuple(entries), cross_source_relations=relations)


class TestEstimateComplexity:
    def test_simple_question(self) -> None:
        manifest = _make_manifest(n_csv=1)
        result = estimate_complexity("What is the average age?", manifest, False)
        assert result == "simple"

    def test_medium_question_with_domain_rules(self) -> None:
        manifest = _make_manifest(n_csv=2)
        result = estimate_complexity("Calculate the average fee", manifest, True)
        assert result == "medium"

    def test_complex_question_with_joins(self) -> None:
        manifest = _make_manifest(n_csv=3, cross_relations=2)
        result = estimate_complexity(
            "What is the total revenue and also the average rate per department?",
            manifest, True,
        )
        assert result == "complex"

    def test_multi_sub_questions_boosts(self) -> None:
        manifest = _make_manifest(n_csv=1)
        q = "Find X and also Y; additionally compute Z"
        result = estimate_complexity(q, manifest, False)
        # Should be at least medium due to sub-questions
        assert result in ("medium", "complex")


class TestComputeBudget:
    def test_simple_budget(self) -> None:
        budget = compute_budget("simple", has_domain_rules=False)
        assert budget.total_chars == 230_000
        assert budget.domain_rules_pct == 0.05
        assert budget.data_profile_pct == 0.35

    def test_complex_budget_with_rules(self) -> None:
        budget = compute_budget("complex", has_domain_rules=True)
        assert budget.total_chars == 462_000
        assert budget.domain_rules_pct == 0.30
        assert budget.data_profile_pct == 0.20

    def test_compact_trigger_is_60_percent(self) -> None:
        budget = compute_budget("medium", has_domain_rules=True)
        assert budget.compact_trigger_chars == int(350_000 * 0.60)

    def test_compact_target_is_40_percent(self) -> None:
        budget = compute_budget("medium", has_domain_rules=True)
        assert budget.compact_target_chars == int(350_000 * 0.40)

    def test_budget_is_frozen(self) -> None:
        budget = compute_budget("simple", has_domain_rules=False)
        with pytest.raises(AttributeError):
            budget.total_chars = 999  # type: ignore[misc]

    def test_unknown_complexity_defaults_to_medium(self) -> None:
        budget = compute_budget("unknown", has_domain_rules=False)
        assert budget.total_chars == 350_000
