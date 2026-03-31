"""Tests for AnalysisState management (state.py)."""

import pytest

from dataline.core.state import (
    add_step,
    compress_manifest,
    create_initial_state,
    render_for_agent,
    summarize_step_output,
    truncate_to_step,
    update_hypothesis,
)
from dataline.core.types import (
    AnalysisState,
    Manifest,
    ManifestEntry,
    PlanStep,
    SandboxResult,
    StepRecord,
)


def _make_manifest() -> Manifest:
    """Create a minimal test manifest."""
    return Manifest(
        entries=(
            ManifestEntry(
                file_path="data.csv",
                file_type="csv",
                size_bytes=1000,
                summary={
                    "columns": [
                        {"name": "id", "dtype": "int64"},
                        {"name": "name", "dtype": "object"},
                        {"name": "value", "dtype": "float64"},
                    ],
                    "row_count": 100,
                },
            ),
        ),
        cross_source_relations=(),
        keyword_tags=("id", "name", "value"),
    )


def _make_step(index: int, description: str, stdout: str = "result") -> StepRecord:
    """Create a minimal test StepRecord."""
    return StepRecord(
        plan=PlanStep(step_description=description),
        code=f"# step {index}",
        result=SandboxResult(
            stdout=stdout,
            stderr="",
            return_code=0,
            execution_time_ms=100,
            step_id=f"step_{index}",
        ),
        step_index=index,
    )


# --- create_initial_state ---


class TestCreateInitialState:
    def test_creates_immutable_state(self):
        manifest = _make_manifest()
        state = create_initial_state("task_1", "What is the average?", manifest, "profile data")

        assert state.task_id == "task_1"
        assert state.question == "What is the average?"
        assert "id(int64)" in state.manifest_summary
        assert "name(object)" in state.manifest_summary
        assert state.key_findings == ()
        assert state.completed_steps == ()
        assert state.full_step_details == ()

    def test_profile_truncated(self):
        manifest = _make_manifest()
        long_profile = "x" * 10000
        state = create_initial_state("t", "q", manifest, long_profile)

        assert len(state.data_profile_summary) <= 10000


# --- compress_manifest ---


class TestCompressManifest:
    def test_csv_format(self):
        manifest = _make_manifest()
        compressed = compress_manifest(manifest)

        assert "data.csv" in compressed
        assert "id(int64)" in compressed
        assert "100 rows" in compressed

    def test_no_sample_values(self):
        """Compressed manifest should NOT include raw sample values."""
        manifest = Manifest(
            entries=(
                ManifestEntry(
                    file_path="test.csv",
                    file_type="csv",
                    size_bytes=100,
                    summary={
                        "columns": [{"name": "col", "dtype": "object", "sample": ["secret_value"]}],
                        "row_count": 10,
                    },
                ),
            ),
        )
        compressed = compress_manifest(manifest)

        assert "secret_value" not in compressed


# --- add_step ---


class TestAddStep:
    def test_immutable_original_unchanged(self):
        manifest = _make_manifest()
        state = create_initial_state("t", "q", manifest, "p")
        step = _make_step(0, "Load data", "Loaded 100 rows")

        new_state = add_step(state, step, "Loaded 100 rows from data.csv")

        # Original unchanged
        assert state.key_findings == ()
        assert state.completed_steps == ()
        assert state.full_step_details == ()

        # New state has the step
        assert len(new_state.key_findings) == 1
        assert len(new_state.completed_steps) == 1
        assert len(new_state.full_step_details) == 1
        assert "Loaded 100 rows" in new_state.key_findings[0]

    def test_multiple_steps_accumulate(self):
        manifest = _make_manifest()
        state = create_initial_state("t", "q", manifest, "p")

        state = add_step(state, _make_step(0, "Load", "loaded"), "Loaded data")
        state = add_step(state, _make_step(1, "Filter", "filtered"), "Filtered rows")
        state = add_step(state, _make_step(2, "Aggregate", "aggregated"), "Computed average")

        assert len(state.key_findings) == 3
        assert len(state.completed_steps) == 3
        assert len(state.full_step_details) == 3

    def test_pickle_detection(self):
        manifest = _make_manifest()
        state = create_initial_state("t", "q", manifest, "p")

        step = StepRecord(
            plan=PlanStep(step_description="Save intermediate"),
            code='import pickle\npickle.dump(df, open("step_0_result.pkl", "wb"))',
            result=SandboxResult(stdout="saved", stderr="", return_code=0, execution_time_ms=50),
            step_index=0,
        )

        new_state = add_step(state, step, "Saved intermediate results")
        assert any("step_0_result.pkl" in v[0] for v in new_state.variables_in_scope)


# --- truncate_to_step ---


class TestTruncateToStep:
    def test_backtrack(self):
        manifest = _make_manifest()
        state = create_initial_state("t", "q", manifest, "p")

        state = add_step(state, _make_step(0, "Step 0", "out0"), "Finding 0")
        state = add_step(state, _make_step(1, "Step 1", "out1"), "Finding 1")
        state = add_step(state, _make_step(2, "Step 2", "out2"), "Finding 2")

        truncated = truncate_to_step(state, 1)

        assert len(truncated.key_findings) == 1
        assert len(truncated.completed_steps) == 1
        assert len(truncated.full_step_details) == 1


# --- render_for_agent ---


class TestRenderForAgent:
    def _build_state(self) -> AnalysisState:
        manifest = _make_manifest()
        state = create_initial_state("t", "What is the total?", manifest, "profile text")
        state = add_step(state, _make_step(0, "Load data", "100 rows loaded"), "Loaded 100 rows")
        return state

    def test_planner_includes_question_and_manifest(self):
        state = self._build_state()
        rendered = render_for_agent(state, "planner")

        assert "What is the total?" in rendered
        assert "Data Sources" in rendered
        assert "Key Findings" in rendered

    def test_coder_includes_variables(self):
        state = self._build_state()
        rendered = render_for_agent(state, "coder")

        assert "Data Sources" in rendered

    def test_verifier_includes_findings_and_output(self):
        state = self._build_state()
        rendered = render_for_agent(state, "verifier")

        assert "Key Findings" in rendered
        assert "Latest Step Output" in rendered

    def test_finalizer_includes_full_details(self):
        state = self._build_state()
        rendered = render_for_agent(state, "finalizer")

        assert "All Step Results" in rendered
        assert "100 rows loaded" in rendered

    def test_debugger_minimal_context(self):
        state = self._build_state()
        rendered = render_for_agent(state, "debugger")

        assert "Data Sources" in rendered
        # Debugger should NOT get step history
        assert "Key Findings" not in rendered

    def test_different_agents_get_different_views(self):
        state = self._build_state()

        planner_view = render_for_agent(state, "planner")
        coder_view = render_for_agent(state, "coder")
        debugger_view = render_for_agent(state, "debugger")

        assert planner_view != coder_view
        assert coder_view != debugger_view


# --- summarize_step_output ---


class TestSummarizeStepOutput:
    def test_short_output(self):
        assert summarize_step_output("Hello world") == "Hello world"

    def test_long_output_truncated(self):
        long_text = "x" * 200
        result = summarize_step_output(long_text, max_len=50)
        assert len(result) <= 53  # 50 + "..."

    def test_empty_output(self):
        assert summarize_step_output("") == "no output"
        assert summarize_step_output("   ") == "no output"

    def test_multiline_takes_first(self):
        result = summarize_step_output("First line\nSecond line\nThird line")
        assert result == "First line"


# --- update_hypothesis ---


class TestUpdateHypothesis:
    def test_updates_immutably(self):
        manifest = _make_manifest()
        state = create_initial_state("t", "q", manifest, "p")
        state2 = update_hypothesis(state, "Values increase over time")

        assert state.current_hypothesis == ""
        assert state2.current_hypothesis == "Values increase over time"
