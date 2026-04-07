"""AnalysisState management: create, update, render for each agent role.

All functions are pure — they return new AnalysisState instances (immutable).

Information priority (high → low):
- Domain rules, formulas, definitions → full content
- Column statistics, distributions → full content (API has 200K context)
- Raw data samples → brief (like LIMIT 10)
- Recent step outputs → full content
- Older step outputs → 1-line summary (already in completed_steps)
"""

from __future__ import annotations

from ..core.types import AnalysisState, Manifest, StepRecord

# Safety cap for single stdout field to prevent extreme cases (e.g., printing 1M rows).
# 200K token context ≈ 800K chars. Each agent call is independent, so no sharing.
# This cap is generous — only catches pathological output.
_STDOUT_SAFETY_CAP = 100_000


def create_initial_state(
    task_id: str,
    question: str,
    manifest: Manifest,
    data_profile: str,
    domain_rules: str = "",
) -> AnalysisState:
    """Initialize state from profiler + analyzer outputs."""
    return AnalysisState(
        task_id=task_id,
        question=question,
        manifest_summary=compress_manifest(manifest),
        data_profile_summary=data_profile,
        domain_rules=domain_rules,
        question_analysis="",
        key_findings=(),
        variables_in_scope=(),
        judge_guidance="",
        completed_steps=(),
        full_step_details=(),
    )


def set_question_analysis(state: AnalysisState, analysis: str) -> AnalysisState:
    """Return new state with question_analysis set (called once after QuestionAnalyzer)."""
    return AnalysisState(
        task_id=state.task_id,
        question=state.question,
        manifest_summary=state.manifest_summary,
        data_profile_summary=state.data_profile_summary,
        domain_rules=state.domain_rules,
        question_analysis=analysis,
        key_findings=state.key_findings,
        variables_in_scope=state.variables_in_scope,
        judge_guidance=state.judge_guidance,
        completed_steps=state.completed_steps,
        full_step_details=state.full_step_details,
    )


def compress_manifest(manifest: Manifest) -> str:
    """Compress manifest to schema info. Full content for documentation files."""
    parts: list[str] = []

    for entry in manifest.entries:
        s = entry.summary
        file_label = f"{entry.file_type}: {entry.file_path}"

        # Flat columns (CSV, JSON, Parquet)
        if "columns" in s:
            cols = ", ".join(
                f"{c.get('name', '?')}({c.get('dtype', '?')})" for c in s["columns"]
            )
            rows = s.get("row_count", "?")
            parts.append(f"{file_label} [{rows} rows]: {cols}")

        # SQLite tables
        elif "tables" in s:
            for table in s["tables"]:
                cols = ", ".join(
                    f"{c.get('name', '?')}({c.get('dtype', '?')})"
                    for c in table.get("columns", [])
                )
                rows = table.get("row_count", "?")
                parts.append(f"{file_label}/{table.get('name', '?')} [{rows} rows]: {cols}")

        # Excel sheets
        elif "sheets" in s:
            for sheet in s["sheets"]:
                cols = ", ".join(
                    f"{c.get('name', '?')}({c.get('dtype', '?')})"
                    for c in sheet.get("columns", [])
                )
                rows = sheet.get("row_count", "?")
                parts.append(f"{file_label}/{sheet.get('name', '?')} [{rows} rows]: {cols}")

        # Text/image/other — full content (domain knowledge lives here)
        else:
            preview = s.get("text_preview", "") or ""
            parts.append(f"{file_label}:\n{preview}" if preview else file_label)

    # Cross-source relations
    for rel in manifest.cross_source_relations:
        parts.append(f"RELATION: {rel.source_a} <-> {rel.source_b}: {rel.relation} (conf={rel.confidence})")

    return "\n".join(parts)


def add_step(
    state: AnalysisState,
    step: StepRecord,
    finding_summary: str,
) -> AnalysisState:
    """Return new state with step appended. Stdout compressed to 1-line finding.

    Args:
        state: Current immutable state.
        step: The completed step record (with full stdout).
        finding_summary: 1-line summary of what this step discovered.
    """
    step_line = f"Step {step.step_index}: {step.plan.step_description} → {finding_summary}"

    # Detect variables saved to disk (pickle files)
    new_vars = state.variables_in_scope
    if "pickle.dump" in step.code or ".to_pickle" in step.code:
        import re
        pkl_matches = re.findall(r'["\']([^"\']+\.pkl)["\']', step.code)
        for pkl in pkl_matches:
            if not any(v[0] == pkl for v in new_vars):
                new_vars = new_vars + ((pkl, step.plan.step_description),)

    return AnalysisState(
        task_id=state.task_id,
        question=state.question,
        manifest_summary=state.manifest_summary,
        data_profile_summary=state.data_profile_summary,
        domain_rules=state.domain_rules,
        question_analysis=state.question_analysis,
        key_findings=state.key_findings + (finding_summary,),
        variables_in_scope=new_vars,
        judge_guidance=state.judge_guidance,
        completed_steps=state.completed_steps + (step_line,),
        full_step_details=state.full_step_details + (step,),
    )


def update_judge_guidance(state: AnalysisState, guidance: str) -> AnalysisState:
    """Return new state with updated judge guidance."""
    return AnalysisState(
        task_id=state.task_id,
        question=state.question,
        manifest_summary=state.manifest_summary,
        data_profile_summary=state.data_profile_summary,
        domain_rules=state.domain_rules,
        question_analysis=state.question_analysis,
        key_findings=state.key_findings,
        variables_in_scope=state.variables_in_scope,
        judge_guidance=guidance,
        completed_steps=state.completed_steps,
        full_step_details=state.full_step_details,
    )


def truncate_to_step(state: AnalysisState, step_index: int) -> AnalysisState:
    """Backtrack: truncate state to given step index. Keep findings up to that point."""
    return AnalysisState(
        task_id=state.task_id,
        question=state.question,
        manifest_summary=state.manifest_summary,
        data_profile_summary=state.data_profile_summary,
        domain_rules=state.domain_rules,
        question_analysis=state.question_analysis,
        key_findings=state.key_findings[:step_index],
        variables_in_scope=state.variables_in_scope,  # keep all — pickles still on disk
        judge_guidance=state.judge_guidance,
        completed_steps=state.completed_steps[:step_index],
        full_step_details=state.full_step_details[:step_index],
    )


def _cap(text: str) -> str:
    """Apply safety cap to prevent pathological output from blowing up context."""
    if len(text) > _STDOUT_SAFETY_CAP:
        return text[:_STDOUT_SAFETY_CAP] + f"\n... (truncated at {_STDOUT_SAFETY_CAP} chars)"
    return text


def summarize_step_output(stdout: str, max_len: int = 100) -> str:
    """Create a 1-line summary from step stdout. Deterministic extraction."""
    if not stdout or not stdout.strip():
        return "no output"

    lines = [line.strip() for line in stdout.strip().split("\n") if line.strip()]
    if not lines:
        return "empty output"

    first_line = lines[0]
    if len(first_line) <= max_len:
        return first_line
    return first_line[:max_len] + "..."
