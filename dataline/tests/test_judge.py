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


# --- Guidance integration ---


class TestGuidanceIntegration:
    def test_guidance_stored_in_state(self):
        """Judge's guidance_for_next_step is stored in state.judge_guidance."""
        state = _make_state_with_steps(1)
        guidance = "Next: filter by card_scheme='GlobalCard' and compute weighted average"
        state = update_judge_guidance(state, guidance)
        assert state.judge_guidance == guidance
