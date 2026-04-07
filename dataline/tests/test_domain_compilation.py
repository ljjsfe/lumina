"""Tests for domain rules compilation in analyzer.py."""

from __future__ import annotations

from dataline.agents.analyzer import (
    compile_domain_rules,
    _split_into_chunks,
    _split_by_headings,
)
from dataline.core.token_estimator import estimate_tokens


class TestCompileDomainRules:
    """compile_domain_rules: conditional compilation based on budget."""

    def test_small_rules_unchanged(self) -> None:
        """Rules under threshold should pass through unchanged."""
        rules = "## Formula\nrevenue = price * quantity"
        result = compile_domain_rules(rules, llm=None, budget_tokens=100_000)
        assert result == rules

    def test_empty_rules_unchanged(self) -> None:
        result = compile_domain_rules("", llm=None, budget_tokens=100_000)
        assert result == ""

    def test_whitespace_only_unchanged(self) -> None:
        result = compile_domain_rules("   ", llm=None, budget_tokens=100_000)
        assert result == "   "

    def test_large_rules_triggers_compilation(self) -> None:
        """Rules exceeding 30% of budget should trigger compilation."""

        class MockLLM:
            def __init__(self) -> None:
                self.called = False

            def chat(self, system: str, user: str) -> str:
                self.called = True
                return "### Rule: revenue_formula\n- **Quote**: revenue = price * qty"

        # Create rules that exceed 30% of a 1000-token budget (300 tokens)
        large_rules = "# Domain Manual\n" + "Business rule detail. " * 200
        assert estimate_tokens(large_rules) > 300  # sanity check

        mock_llm = MockLLM()
        result = compile_domain_rules(large_rules, llm=mock_llm, budget_tokens=1_000)
        assert mock_llm.called
        assert "revenue_formula" in result

    def test_compilation_failure_returns_raw(self) -> None:
        """If LLM fails, raw rules are returned."""

        class FailLLM:
            def chat(self, system: str, user: str) -> str:
                raise RuntimeError("LLM down")

        rules = "# Rules\n" + "Important rule. " * 200
        result = compile_domain_rules(rules, llm=FailLLM(), budget_tokens=1_000)
        # Should return the original, not crash
        assert result == rules

    def test_compilation_empty_result_returns_raw(self) -> None:
        """If LLM returns empty/short result, raw rules are returned."""

        class EmptyLLM:
            def chat(self, system: str, user: str) -> str:
                return ""

        rules = "# Rules\n" + "Important rule. " * 200
        result = compile_domain_rules(rules, llm=EmptyLLM(), budget_tokens=1_000)
        assert result == rules


class TestSplitIntoChunks:
    """_split_into_chunks: splitting large docs at boundaries."""

    def test_small_text_single_chunk(self) -> None:
        text = "Hello world"
        chunks = _split_into_chunks(text)
        assert len(chunks) == 1
        assert chunks[0] == text

    def test_splits_at_file_separators(self) -> None:
        text = (
            "=== file1.md ===\nContent of file 1.\n\n"
            "=== file2.md ===\nContent of file 2."
        )
        chunks = _split_into_chunks(text)
        assert len(chunks) >= 1
        # Both file contents should be in chunks
        combined = " ".join(chunks)
        assert "file1.md" in combined
        assert "file2.md" in combined

    def test_respects_chunk_size_limit(self) -> None:
        """Each chunk should not exceed the max size."""
        # Create text larger than chunk limit
        text = "=== big.md ===\n" + "x" * 100_000
        chunks = _split_into_chunks(text)
        for chunk in chunks:
            assert len(chunk) <= 80_001  # _CHUNK_MAX_CHARS + 1 char tolerance


class TestSplitByHeadings:
    """_split_by_headings: splitting single large documents."""

    def test_splits_at_headings(self) -> None:
        text = (
            "# Section 1\nContent 1.\n\n"
            "# Section 2\nContent 2.\n\n"
            "# Section 3\nContent 3."
        )
        chunks = _split_by_headings(text)
        assert len(chunks) >= 1
        combined = " ".join(chunks)
        assert "Section 1" in combined
        assert "Section 3" in combined

    def test_no_headings_single_chunk(self) -> None:
        text = "Just plain text without any headings."
        chunks = _split_by_headings(text)
        assert len(chunks) == 1
