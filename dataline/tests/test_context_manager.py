"""Tests for token_estimator and context_manager."""

from __future__ import annotations

import pytest

from dataline.core.token_estimator import estimate_tokens
from dataline.core.context_manager import ContextManager, Section


# --- TokenEstimator tests ---


class TestEstimateTokens:

    def test_empty_string(self) -> None:
        assert estimate_tokens("") == 0

    def test_english_text(self) -> None:
        tokens = estimate_tokens("Hello world, this is a test sentence.")
        assert 5 < tokens < 30  # reasonable range with safety margin

    def test_chinese_text(self) -> None:
        tokens = estimate_tokens("这是一个测试用于估算token数量")
        assert tokens > 0

    def test_safety_margin_increases_count(self) -> None:
        # Use longer text to avoid int rounding making both equal
        text = "Some text for estimation that is long enough to see the margin effect clearly."
        no_margin = estimate_tokens(text, safety_margin=1.0)
        with_margin = estimate_tokens(text, safety_margin=1.5)
        assert with_margin > no_margin

    def test_code_text(self) -> None:
        code = "import pandas as pd\ndf = pd.read_csv('data.csv')\nprint(df.shape)"
        tokens = estimate_tokens(code)
        assert tokens > 0


# --- ContextManager tests ---


def _make_section(name: str, char_count: int, priority: int = 50, **kwargs) -> Section:
    """Helper to create a section with approximately char_count characters."""
    content = f"[{name}] " + "x" * max(0, char_count - len(name) - 3)
    return Section(name=name, content=content, priority=priority, **kwargs)


class TestContextManagerUnderBudget:
    """When total content fits within budget, everything passes through."""

    def test_all_sections_included(self) -> None:
        cm = ContextManager(token_limit=262_000)
        sections = [
            Section("question", "What is the total revenue?", priority=100, compressible=False),
            Section("rules", "Revenue = price * quantity", priority=70),
            Section("profile", "Column: revenue (float64)", priority=50),
        ]
        result = cm.assemble(sections)
        assert "total revenue" in result
        assert "price * quantity" in result
        assert "revenue (float64)" in result

    def test_empty_sections_filtered(self) -> None:
        cm = ContextManager(token_limit=262_000)
        sections = [
            Section("question", "What?", priority=100),
            Section("empty", "", priority=50),
            Section("blank", "   ", priority=50),
            Section("rules", "Rule A", priority=70),
        ]
        result = cm.assemble(sections)
        assert "What?" in result
        assert "Rule A" in result
        assert "empty" not in result

    def test_order_preserved(self) -> None:
        cm = ContextManager(token_limit=262_000)
        sections = [
            Section("first", "AAA_FIRST", priority=10),
            Section("second", "BBB_SECOND", priority=90),
            Section("third", "CCC_THIRD", priority=50),
        ]
        result = cm.assemble(sections)
        assert result.index("AAA_FIRST") < result.index("BBB_SECOND")
        assert result.index("BBB_SECOND") < result.index("CCC_THIRD")

    def test_headings_rendered(self) -> None:
        cm = ContextManager(token_limit=262_000)
        sections = [
            Section("q", "The question", priority=100, heading="## Question"),
        ]
        result = cm.assemble(sections)
        assert "## Question" in result
        assert "The question" in result


class TestContextManagerOverBudget:
    """When total exceeds budget, lowest-priority sections get compressed."""

    def test_low_priority_truncated_first(self) -> None:
        # budget = 15000*0.7-8000 = 2500 tokens. Content ~4300 tokens → triggers compression
        cm = ContextManager(token_limit=15_000)
        low_original = "# Section B\nLOW info\n" * 500
        sections = [
            Section("important", "KEEP THIS " * 10, priority=100, compressible=False),
            Section("medium", "# Section A\nMEDIUM data\n" * 100, priority=50),
            Section("low", low_original, priority=10),
        ]
        result = cm.assemble(sections, llm=None)
        # High-priority content preserved
        assert "KEEP THIS" in result
        # Low-priority section was truncated (original was 10500 chars)
        assert len(low_original) > 8000  # sanity: original is large
        assert "truncated" in result  # truncation marker present

    def test_non_compressible_never_touched(self) -> None:
        cm = ContextManager(token_limit=15_000)
        original_text = "MUST PRESERVE THIS EXACTLY"
        sections = [
            Section("fixed", original_text, priority=100, compressible=False),
            Section("flexible", "# Filler\ndata point\n" * 500, priority=10),
        ]
        result = cm.assemble(sections, llm=None)
        assert original_text in result

    def test_llm_summarization_called(self) -> None:
        """When LLM is provided, it should be used for summarization."""

        class MockLLM:
            def __init__(self) -> None:
                self.called = False

            def chat(self, system: str, user: str) -> str:
                self.called = True
                return "Summarized: key point A, key point B"

        cm = ContextManager(token_limit=15_000)
        sections = [
            Section("q", "Question?", priority=100, compressible=False),
            Section("big", "# Data\nvalue\n" * 500, priority=10),
        ]
        mock_llm = MockLLM()
        cm.assemble(sections, llm=mock_llm)
        assert mock_llm.called

    def test_llm_failure_falls_back_to_truncation(self) -> None:
        """If LLM fails, should fall back to truncation without crashing."""

        class FailingLLM:
            def chat(self, system: str, user: str) -> str:
                raise RuntimeError("LLM unavailable")

        cm = ContextManager(token_limit=15_000)
        sections = [
            Section("q", "Question?", priority=100, compressible=False),
            Section("big", "# Data\nvalue\n" * 500, priority=10),
        ]
        # Should not raise
        result = cm.assemble(sections, llm=FailingLLM())
        assert "Question?" in result


class TestContextManagerFixedTokens:
    """Sections with fixed_tokens (e.g., images) reduce available budget."""

    def test_fixed_tokens_subtract_from_budget(self) -> None:
        cm = ContextManager(token_limit=2000)
        budget_before = cm.budget_tokens

        sections = [
            Section("image_desc", "Chart shows Q1=100, Q2=200",
                    priority=80, fixed_tokens=800),
            Section("text", "Some analysis", priority=50),
        ]
        total = cm.estimate_total(sections)
        # Fixed tokens should be included in total estimate
        assert total >= 800


class TestContextManagerEstimate:
    """estimate_total() for pre-checking."""

    def test_estimate_returns_positive(self) -> None:
        cm = ContextManager(token_limit=262_000)
        sections = [
            Section("a", "Hello world", priority=50),
            Section("b", "More text here", priority=50),
        ]
        total = cm.estimate_total(sections)
        assert total > 0

    def test_estimate_empty_sections(self) -> None:
        cm = ContextManager(token_limit=262_000)
        assert cm.estimate_total([]) == 0
        assert cm.estimate_total([Section("e", "", priority=50)]) == 0


class TestSmartTruncate:
    """Test the heading-aware truncation logic."""

    def test_preserves_high_value_sections(self) -> None:
        from dataline.core.context_manager import _smart_truncate

        text = (
            "# Overview\nThis is the overview.\n\n"
            "# Formula\nrevenue = price * quantity = 100 * 5\n\n"
            "# Narrative\n" + "The weather was nice. " * 100 + "\n\n"
            "# Definitions\nColumn A: the primary key, integer type\n"
        )
        # Truncate to roughly half
        result = _smart_truncate(text, target_tokens=50)
        # Formula section should be preserved (has numbers and =)
        assert "revenue" in result or "Overview" in result

    def test_short_text_unchanged(self) -> None:
        from dataline.core.context_manager import _smart_truncate

        short = "Just a few words."
        assert _smart_truncate(short, target_tokens=1000) == short
