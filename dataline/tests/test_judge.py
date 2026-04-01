"""Tests for Judge agent (Phase 4: merged Verifier+Router)."""

import pytest

from dataline.core.types import (
    AnalysisState,
    JudgeDecision,
    Manifest,
    ManifestEntry,
    PlanStep,
    SandboxResult,
    StepRecord,
)
from dataline.core.state import (
    add_step,
    create_initial_state,
    render_for_agent,
    update_judge_guidance,
)


def _make_manifest() -> Manifest:
    return Manifest(
        entries=(
            ManifestEntry(
                file_path="payments.csv",
                file_type="csv",
                size_bytes=5000,
                summary={
                    "columns": [
                        {"name": "tx_id", "dtype": "int64"},
                        {"name": "amount", "dtype": "float64"},
                        {"name": "merchant", "dtype": "object"},
                    ],
                    "row_count": 1000,
                },
            ),
        ),
    )


def _make_state_with_steps(n_steps: int = 1) -> AnalysisState:
    manifest = _make_manifest()
    state = create_initial_state("t1", "What is the total amount?", manifest, "profile text")
    for i in range(n_steps):
        step = StepRecord(
            plan=PlanStep(step_description=f"Step {i} description"),
            code=f"# code {i}",
            result=SandboxResult(
                stdout=f"result_{i}: computed value",
                stderr="",
                return_code=0,
                execution_time_ms=100,
            ),
            step_index=i,
        )
        state = add_step(state, step, f"Found result {i}")
    return state


# --- JudgeDecision type ---


class TestJudgeDecision:
    def test_immutable(self):
        decision = JudgeDecision(
            sufficient=True,
            action="finish",
            reasoning="All parts answered",
        )
        assert decision.sufficient is True
        assert decision.action == "finish"
        with pytest.raises(AttributeError):
            decision.sufficient = False  # type: ignore[misc]

    def test_defaults(self):
        decision = JudgeDecision(sufficient=False, action="continue")
        assert decision.reasoning == ""
        assert decision.missing == ""
        assert decision.guidance_for_next_step == ""
        assert decision.truncate_to == 0

    def test_guidance_field(self):
        decision = JudgeDecision(
            sufficient=False,
            action="continue",
            guidance_for_next_step="Filter payments by merchant before computing average",
        )
        assert "Filter payments" in decision.guidance_for_next_step

    def test_backtrack_with_truncate(self):
        decision = JudgeDecision(
            sufficient=False,
            action="backtrack",
            truncate_to=2,
            reasoning="Step 3 used wrong filter",
        )
        assert decision.action == "backtrack"
        assert decision.truncate_to == 2


# --- render_for_agent: judge view ---


class TestJudgeView:
    def test_includes_question(self):
        state = _make_state_with_steps(1)
        rendered = render_for_agent(state, "judge")
        assert "What is the total amount?" in rendered

    def test_includes_data_sources(self):
        state = _make_state_with_steps(1)
        rendered = render_for_agent(state, "judge")
        assert "Data Sources" in rendered
        assert "payments.csv" in rendered

    def test_includes_key_findings(self):
        state = _make_state_with_steps(2)
        rendered = render_for_agent(state, "judge")
        assert "Key Findings" in rendered
        assert "Found result 0" in rendered
        assert "Found result 1" in rendered

    def test_includes_completed_steps(self):
        state = _make_state_with_steps(2)
        rendered = render_for_agent(state, "judge")
        assert "Completed Steps" in rendered

    def test_includes_latest_step_output(self):
        state = _make_state_with_steps(1)
        rendered = render_for_agent(state, "judge")
        assert "Step 0 Output" in rendered
        assert "result_0: computed value" in rendered

    def test_includes_judge_guidance(self):
        state = _make_state_with_steps(1)
        state = update_judge_guidance(state, "Total should be around 50000")
        rendered = render_for_agent(state, "judge")
        assert "Prior Guidance" in rendered
        assert "Total should be around 50000" in rendered

    def test_no_hypothesis_when_empty(self):
        state = _make_state_with_steps(1)
        rendered = render_for_agent(state, "judge")
        assert "Prior Guidance" not in rendered

    def test_includes_error_for_failed_step(self):
        manifest = _make_manifest()
        state = create_initial_state("t", "q", manifest, "p")
        step = StepRecord(
            plan=PlanStep(step_description="Compute sum"),
            code="raise ValueError('bad')",
            result=SandboxResult(
                stdout="",
                stderr="ValueError: bad",
                return_code=1,
                execution_time_ms=50,
            ),
            step_index=0,
        )
        state = add_step(state, step, "Error in computation")
        rendered = render_for_agent(state, "judge")
        assert "Step 0 Error" in rendered
        assert "ValueError: bad" in rendered

    def test_differs_from_verifier_view(self):
        state = _make_state_with_steps(2)
        judge_view = render_for_agent(state, "judge")
        verifier_view = render_for_agent(state, "verifier")
        # Judge gets Data Sources; verifier does not
        assert "Data Sources" in judge_view
        assert "Data Sources" not in verifier_view


# --- Guidance integration with hypothesis ---


class TestGuidanceHypothesisIntegration:
    def test_guidance_becomes_hypothesis(self):
        """Judge's guidance_for_next_step should be usable as the next hypothesis."""
        state = _make_state_with_steps(1)
        guidance = "Next: filter by card_scheme='GlobalCard' and compute weighted average"
        state = update_judge_guidance(state, guidance)
        assert state.judge_guidance == guidance

        # Planner should see this guidance
        rendered = render_for_agent(state, "planner")
        assert "GlobalCard" in rendered
        assert "Judge Guidance" in rendered
