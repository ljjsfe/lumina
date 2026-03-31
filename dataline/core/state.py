"""AnalysisState management: create, update, render for each agent role.

All functions are pure — they return new AnalysisState instances (immutable).
Inspired by DS-STAR (accumulative context), OpenClaw (3-tier memory),
and anchored iterative summarization (compress verified findings).
"""

from __future__ import annotations

from ..core.types import AnalysisState, Manifest, StepRecord


def create_initial_state(
    task_id: str,
    question: str,
    manifest: Manifest,
    data_profile: str,
) -> AnalysisState:
    """Initialize state from profiler + analyzer outputs."""
    return AnalysisState(
        task_id=task_id,
        question=question,
        manifest_summary=compress_manifest(manifest),
        data_profile_summary=data_profile[:10000],
        key_findings=(),
        variables_in_scope=(),
        current_hypothesis="",
        completed_steps=(),
        full_step_details=(),
    )


def compress_manifest(manifest: Manifest) -> str:
    """Compress manifest to column names + types only. No sample values."""
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

        # Text/image/other — include generous preview (domain knowledge lives here)
        else:
            preview = s.get("text_preview", "") if s.get("text_preview") else ""
            if preview and len(preview) > 8000:
                preview = preview[:8000] + f"\n... ({s.get('char_count', len(preview))} chars total)"
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
        # Extract pickle filename from code heuristically
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
        key_findings=state.key_findings + (finding_summary,),
        variables_in_scope=new_vars,
        current_hypothesis=state.current_hypothesis,
        completed_steps=state.completed_steps + (step_line,),
        full_step_details=state.full_step_details + (step,),
    )


def update_hypothesis(state: AnalysisState, hypothesis: str) -> AnalysisState:
    """Return new state with updated hypothesis."""
    return AnalysisState(
        task_id=state.task_id,
        question=state.question,
        manifest_summary=state.manifest_summary,
        data_profile_summary=state.data_profile_summary,
        key_findings=state.key_findings,
        variables_in_scope=state.variables_in_scope,
        current_hypothesis=hypothesis,
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
        key_findings=state.key_findings[:step_index],
        variables_in_scope=state.variables_in_scope,  # keep all — pickles still on disk
        current_hypothesis=state.current_hypothesis,
        completed_steps=state.completed_steps[:step_index],
        full_step_details=state.full_step_details[:step_index],
    )


def render_for_agent(state: AnalysisState, agent_role: str) -> str:
    """Render state into a context string tailored per agent role.

    Different agents need different views:
    - Planner: global view (findings + history + manifest + hypothesis)
    - Coder: narrow view (manifest + variables + last 2 steps full output)
    - Verifier: evaluation view (findings + history + last step output)
    - Router: decision view (same as verifier)
    - Debugger: error context (manifest + profile, no history)
    - Finalizer: everything (full step details with max output)
    """
    sections: list[str] = []

    if agent_role == "planner":
        sections.append(f"## Question\n{state.question}")
        sections.append(f"## Data Sources\n{state.manifest_summary}")
        if state.data_profile_summary:
            sections.append(f"## Data Profile\n{state.data_profile_summary[:10000]}")
        if state.key_findings:
            sections.append("## Key Findings So Far\n" + "\n".join(f"- {f}" for f in state.key_findings))
        if state.completed_steps:
            sections.append("## Completed Steps\n" + "\n".join(state.completed_steps))
        if state.current_hypothesis:
            sections.append(f"## Current Hypothesis\n{state.current_hypothesis}")

    elif agent_role == "coder":
        sections.append(f"## Data Sources\n{state.manifest_summary}")
        if state.data_profile_summary:
            sections.append(f"## Data Profile (sample rows, value distributions)\n{state.data_profile_summary[:10000]}")
        if state.variables_in_scope:
            vars_text = "\n".join(f"- {name}: {desc}" for name, desc in state.variables_in_scope)
            sections.append(f"## Available Variables (in TEMP_DIR)\n{vars_text}")
        # Last 2 steps with full output for coding context
        recent = state.full_step_details[-2:] if state.full_step_details else ()
        if recent:
            parts = []
            for s in recent:
                stdout = s.result.stdout[:4000] if s.result.stdout else "(no output)"
                parts.append(f"Step {s.step_index} ({s.plan.step_description}):\n{stdout}")
            sections.append("## Recent Results\n" + "\n\n".join(parts))

    elif agent_role == "judge":
        sections.append(f"## Question\n{state.question}")
        sections.append(f"## Data Sources\n{state.manifest_summary}")
        if state.key_findings:
            sections.append("## Key Findings\n" + "\n".join(f"- {f}" for f in state.key_findings))
        if state.completed_steps:
            sections.append("## Completed Steps\n" + "\n".join(state.completed_steps))
        # Last step with code + output for logic auditing
        if state.full_step_details:
            last = state.full_step_details[-1]
            code_preview = last.code[:2000] if last.code else "(no code)"
            stdout = last.result.stdout[:4000] if last.result.stdout else "(no output)"
            stderr = last.result.stderr[:500] if last.result.return_code != 0 else ""
            sections.append(f"## Latest Step Code\n```python\n{code_preview}\n```")
            sections.append(f"## Latest Step Output\n{stdout}")
            if stderr:
                sections.append(f"## Latest Step Error\n{stderr}")
        if state.current_hypothesis:
            sections.append(f"## Current Hypothesis\n{state.current_hypothesis}")

    elif agent_role in ("verifier", "router"):
        sections.append(f"## Question\n{state.question}")
        if state.key_findings:
            sections.append("## Key Findings\n" + "\n".join(f"- {f}" for f in state.key_findings))
        if state.completed_steps:
            sections.append("## Completed Steps\n" + "\n".join(state.completed_steps))
        # Last step with more output detail
        if state.full_step_details:
            last = state.full_step_details[-1]
            stdout = last.result.stdout[:4000] if last.result.stdout else "(no output)"
            sections.append(f"## Latest Step Output\n{stdout}")

    elif agent_role == "debugger":
        sections.append(f"## Data Sources\n{state.manifest_summary[:6000]}")
        if state.data_profile_summary:
            sections.append(f"## Data Profile (sample rows, value distributions)\n{state.data_profile_summary[:10000]}")

    elif agent_role == "finalizer":
        sections.append(f"## Question\n{state.question}")
        # Full step details with generous output truncation
        if state.full_step_details:
            parts = []
            for s in state.full_step_details:
                stdout = s.result.stdout[:8000] if s.result.stdout else "(no output)"
                parts.append(
                    f"Step {s.step_index}: {s.plan.step_description}\n"
                    f"  Output:\n{stdout}"
                )
            sections.append("## All Step Results\n" + "\n\n".join(parts))
        if state.key_findings:
            sections.append("## Key Findings\n" + "\n".join(f"- {f}" for f in state.key_findings))

    else:
        # Fallback: provide everything
        sections.append(f"## Question\n{state.question}")
        sections.append(f"## Data Sources\n{state.manifest_summary}")
        if state.completed_steps:
            sections.append("## Completed Steps\n" + "\n".join(state.completed_steps))

    return "\n\n".join(sections)


def summarize_step_output(stdout: str, max_len: int = 100) -> str:
    """Create a 1-line summary from step stdout. Deterministic extraction."""
    if not stdout or not stdout.strip():
        return "no output"

    # Take first non-empty line as summary
    lines = [line.strip() for line in stdout.strip().split("\n") if line.strip()]
    if not lines:
        return "empty output"

    first_line = lines[0]
    if len(first_line) <= max_len:
        return first_line
    return first_line[:max_len] + "..."
