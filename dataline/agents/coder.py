"""Coder agent: convert plan step to executable Python code."""

from __future__ import annotations

import json
import re
from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.state import render_for_agent
from ..core.types import AnalysisState, Manifest, PlanStep, StepRecord
from ..core.workspace import Workspace


def generate(
    plan_step: PlanStep,
    manifest_json: str,
    steps_done: list[StepRecord],
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
    workspace: Workspace | None = None,
    manifest: Manifest | None = None,
) -> str:
    """Generate Python code for a plan step.

    If state is provided, uses structured context rendering.
    Otherwise falls back to legacy steps_done formatting.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "coder.md"
    template = prompt_path.read_text(encoding="utf-8")

    plan_dict: dict = {
        "step_description": plan_step.step_description,
        "data_sources": list(plan_step.data_sources),
        "depends_on_prior": plan_step.depends_on_prior,
        "expected_output": plan_step.expected_output,
    }
    if plan_step.approach_detail:
        plan_dict["approach_detail"] = plan_step.approach_detail
    plan_json = json.dumps(plan_dict, ensure_ascii=False)

    if state is not None:
        context = render_for_agent(state, "coder")

        # Inject lessons learned from prior iterations
        if workspace is not None:
            lessons = workspace.read_lessons_learned()
            if lessons:
                context += f"\n\n## Lessons Learned from Prior Iterations\n{lessons}"

        # Inject relevant column value distributions for referenced data sources
        if manifest is not None:
            col_context = _get_source_column_context(plan_step.data_sources, manifest)
            if col_context:
                context += f"\n\n## Column Value Context for This Step\n{col_context}"

        system_prompt = (
            template
            .replace("{plan_step}", plan_json)
            .replace("{manifest_json}", state.manifest_summary)
            .replace("{prior_results_summary}", context)  # includes data profile + recent results
        )
    else:
        prior_summary = _format_prior_results(steps_done)
        system_prompt = (
            template
            .replace("{plan_step}", plan_json)
            .replace("{manifest_json}", manifest_json)
            .replace("{prior_results_summary}", prior_summary)
        )

    response = llm.chat(system_prompt, "Generate the Python code now.")
    return _extract_code(response)


def _extract_code(response: str) -> str:
    """Extract Python code from markdown code blocks."""
    match = re.search(r"```python\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"```\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    return response.strip()


def _get_source_column_context(data_sources: tuple[str, ...], manifest: Manifest) -> str:
    """Extract column value distributions and sample rows for the data sources in this step.

    Injects both:
    - Column value distributions (cardinality, range, known values) — prevents wrong filter values
    - Actual sample rows formatted as DataFrame — prevents structural assumptions
    """
    if not data_sources:
        return ""

    parts: list[str] = []
    for entry in manifest.entries:
        entry_name = entry.file_path.rsplit("/", 1)[-1] if "/" in entry.file_path else entry.file_path
        if not any(entry_name in src or src in entry.file_path for src in data_sources):
            continue

        # --- Sample rows (actual data preview) ---
        sample_rows = entry.summary.get("sample_rows", [])
        # For SQLite: check per-table sample rows
        for table in entry.summary.get("tables", []):
            t_samples = table.get("sample_rows", [])
            if t_samples:
                sample_rows = t_samples  # use last non-empty table
        for sheet in entry.summary.get("sheets", []):
            s_samples = sheet.get("sample_rows", [])
            if s_samples:
                sample_rows = s_samples

        if sample_rows:
            parts.append(f"\n### {entry_name} — actual data sample (use this to understand real values/format):")
            parts.append(_format_sample_rows(sample_rows))

        # --- Column value distributions ---
        columns = entry.summary.get("columns", [])
        for table in entry.summary.get("tables", []):
            columns.extend(table.get("columns", []))
        for sheet in entry.summary.get("sheets", []):
            columns.extend(sheet.get("columns", []))

        col_lines: list[str] = []
        for col in columns:
            name = col.get("name", "")
            value_repr = col.get("value_repr", {})
            if not value_repr:
                continue
            vtype = value_repr.get("value_type", "")
            card = value_repr.get("cardinality", "")
            detail = ""
            if "all_values" in value_repr:
                detail = f"values: {value_repr['all_values']}"
            elif "range" in value_repr:
                detail = f"range: {value_repr['range']}"
            elif "sample" in value_repr:
                detail = f"sample: {value_repr['sample']}"
            col_lines.append(f"  - {name}: {vtype} (card={card}) {detail}")

        if col_lines:
            parts.append(f"\n### {entry_name} — column distributions:")
            parts.extend(col_lines[:50])

    return "\n".join(parts)


def _format_sample_rows(sample_rows: list[dict]) -> str:
    """Format sample rows as a readable table (like df.head(3).to_string())."""
    if not sample_rows:
        return ""
    try:
        import pandas as pd
        df = pd.DataFrame(sample_rows[:3])
        return df.to_string(index=False)
    except Exception:
        # Fallback: simple key-value format
        lines = []
        for i, row in enumerate(sample_rows[:3]):
            lines.append(f"  row {i}: {row}")
        return "\n".join(lines)


def _format_prior_results(steps: list[StepRecord]) -> str:
    if not steps:
        return "No prior steps."
    parts = []
    for s in steps:
        stdout = s.result.stdout[:100_000] if s.result.stdout else "(no output)"
        parts.append(f"Step {s.step_index} ({s.plan.step_description}):\n{stdout}")
    return "\n\n".join(parts)
