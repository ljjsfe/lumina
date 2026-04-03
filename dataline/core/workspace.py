"""File-based analysis workspace: replaces in-memory context stuffing.

Inspired by GSD's spec-driven approach: use files as persistent state,
agents read from disk on demand instead of receiving everything in prompt.

Workspace files serve dual purpose:
1. Runtime: agents read relevant sections via get_context()
2. Post-run: developers inspect files for full observability

Directory structure:
    workspace/
    ├── DOMAIN_RULES.md              # Full documentation content (high priority)
    ├── DOMAIN_RULES_STRUCTURED.md   # LLM-extracted structured rules (when docs > 50K)
    ├── DATA_PROFILE.md              # Column stats, distributions (medium priority)
    ├── ANALYSIS_PLAN.md             # QuestionAnalyzer output (high priority)
    ├── PROGRESS.md                  # Accumulated findings per step
    ├── CONTEXT_SUMMARY.md           # LLM-compressed summary (replaces old step outputs)
    ├── LESSONS_LEARNED.md           # Cross-iteration debug lessons
    ├── JUDGE_GUIDANCE.md            # Current judge steering signal
    └── steps/
        ├── step_0_code.py           # Code executed
        ├── step_0_output.txt        # Full stdout
        └── ...
"""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .context_budget import ContextBudget


class Workspace:
    """File-based state management for a single analysis task.

    Lifecycle:
    1. Created in temp_dir (sandbox-accessible)
    2. Agents write state files during execution
    3. Agents read via get_context() — smart retrieval, not full dump
    4. On completion, persist() copies to output_dir/workspace/
    """

    def __init__(self, temp_dir: str, output_dir: str = ""):
        self.temp_dir = temp_dir
        self.output_dir = output_dir
        self._workspace_dir = os.path.join(temp_dir, "workspace")
        self._steps_dir = os.path.join(self._workspace_dir, "steps")
        os.makedirs(self._steps_dir, exist_ok=True)

    # --- Write methods (called during pipeline execution) ---

    def write_domain_rules(self, rules: str) -> None:
        """Write extracted documentation content to DOMAIN_RULES.md."""
        self._write("DOMAIN_RULES.md", rules)

    def write_data_profile(self, profile: str) -> None:
        """Write statistical profile to DATA_PROFILE.md."""
        self._write("DATA_PROFILE.md", profile)

    def write_analysis_plan(self, plan: str) -> None:
        """Write QuestionAnalyzer output to ANALYSIS_PLAN.md."""
        self._write("ANALYSIS_PLAN.md", plan)

    def append_progress(self, step_idx: int, description: str, finding: str) -> None:
        """Append a step's finding to PROGRESS.md."""
        line = f"### Step {step_idx}: {description}\n{finding}\n\n"
        path = os.path.join(self._workspace_dir, "PROGRESS.md")
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)

    def write_step(self, step_idx: int, code: str, output: str) -> None:
        """Write a step's code and output to individual files."""
        self._write(f"steps/step_{step_idx}_code.py", code)
        self._write(f"steps/step_{step_idx}_output.txt", output)

    def write_judge_guidance(self, guidance: str) -> None:
        """Write current judge guidance (overwritten each iteration)."""
        self._write("JUDGE_GUIDANCE.md", guidance)

    # --- Read methods (called by agents via get_context) ---

    def read_domain_rules(self) -> str:
        return self._read("DOMAIN_RULES.md")

    def read_data_profile(self) -> str:
        return self._read("DATA_PROFILE.md")

    def read_analysis_plan(self) -> str:
        return self._read("ANALYSIS_PLAN.md")

    def read_progress(self) -> str:
        return self._read("PROGRESS.md")

    def read_judge_guidance(self) -> str:
        return self._read("JUDGE_GUIDANCE.md")

    def read_step_output(self, step_idx: int) -> str:
        return self._read(f"steps/step_{step_idx}_output.txt")

    def read_step_code(self, step_idx: int) -> str:
        return self._read(f"steps/step_{step_idx}_code.py")

    # --- Smart context retrieval ---

    def get_context(
        self,
        agent_role: str,
        question: str,
        budget_chars: int = 200_000,
        budget: ContextBudget | None = None,
    ) -> str:
        """Assemble context for an agent, prioritized and budget-aware.

        Instead of stuffing everything into prompt, retrieves relevant
        sections from workspace files with priority ordering.

        If a ContextBudget is provided, uses its per-section ratios.
        Otherwise falls back to hardcoded fractions (backward compatible).

        Priority (high to low):
        1. Question (always included)
        2. Judge guidance (control signal, if exists)
        3. Analysis plan (strategy, if exists)
        4. Domain rules (documentation — smart truncation if too long)
        5. Data profile (statistics — smart truncation if too long)
        6. Progress / completed steps
        7. Recent step outputs (last 2 steps for coder, last 1 for judge)
        8. Manifest summary (schema info)
        """
        total = budget.total_chars if budget else budget_chars
        sections: list[tuple[str, str]] = []  # (heading, content)
        used = 0

        def add(heading: str, content: str, max_chars: int = 0) -> bool:
            """Add section if within budget. Returns True if added."""
            nonlocal used
            if not content or not content.strip():
                return False
            if max_chars and len(content) > max_chars:
                content = _smart_truncate(content, max_chars, question)
            if used + len(content) + len(heading) + 10 > total:
                return False
            sections.append((heading, content))
            used += len(content) + len(heading) + 10
            return True

        # Compute per-section limits
        if budget:
            plan_limit = int(total * budget.analysis_plan_pct)
            domain_limit = int(total * budget.domain_rules_pct)
            domain_limit_small = int(total * 0.10)  # finalizer gets less
            profile_limit = int(total * budget.data_profile_pct)
            progress_limit = int(total * budget.progress_pct)
        else:
            plan_limit = total // 5
            domain_limit = total * 3 // 10
            domain_limit_small = total // 10
            profile_limit = total // 4
            progress_limit = total // 5

        # 1. Question (always)
        add("Question", question)

        # 2. Judge guidance (control signal — highest priority after question)
        if agent_role == "planner":
            guidance = self.read_judge_guidance()
            if guidance:
                add("Judge Guidance (MUST ADDRESS in this step)", f"> **{guidance}**")

        # 3. Answer schema (guidance for planner and finalizer — NOT used for judge gates)
        schema = self._read("ANSWER_SCHEMA.md")
        if schema and agent_role in ("planner", "finalizer"):
            add("Answer Schema (from QuestionAnalyzer)", schema)

        # 4. Analysis plan (strategy context)
        plan = self.read_analysis_plan()
        if plan:
            add("Analysis Plan", plan, max_chars=plan_limit)

        # 5. Domain rules (prefer structured version when available)
        structured_rules = self._read("DOMAIN_RULES_STRUCTURED.md")
        rules = structured_rules or self.read_domain_rules()
        if rules and agent_role != "finalizer":
            add("Domain Rules (from documentation)", rules, max_chars=domain_limit)
        elif rules:
            add("Domain Rules", rules, max_chars=domain_limit_small)

        # 5. Manifest summary — injected by caller (not in workspace files)
        #    Caller adds this separately since it comes from AnalysisState

        # 6. Data profile
        profile = self.read_data_profile()
        if profile and agent_role in ("planner", "coder", "debugger"):
            add("Data Profile", profile, max_chars=profile_limit)

        # 7. Lessons learned (cross-iteration experience)
        lessons = self.read_lessons_learned()
        if lessons and agent_role in ("planner", "coder"):
            add("Lessons Learned from Prior Iterations", lessons)

        # 8. Progress — use context summary for older steps when available
        summary = self.read_context_summary()
        progress = self.read_progress()
        if agent_role in ("planner", "judge", "verifier", "router", "finalizer"):
            if summary:
                add("Analysis Progress Summary", summary, max_chars=progress_limit)
            elif progress:
                add("Progress So Far", progress, max_chars=progress_limit)

        # 9. Recent step outputs (agent-specific)
        if agent_role == "coder":
            self._add_recent_steps(sections, used, total, last_n=2)
        elif agent_role in ("judge", "verifier"):
            self._add_recent_steps(sections, used, total, last_n=1, include_code=True)
        elif agent_role == "finalizer":
            self._add_all_steps(sections, used, total)

        # Render
        parts = []
        for heading, content in sections:
            parts.append(f"## {heading}\n{content}")
        return "\n\n".join(parts)

    def _add_recent_steps(
        self,
        sections: list[tuple[str, str]],
        used: int,
        budget: int,
        last_n: int = 2,
        include_code: bool = False,
    ) -> None:
        """Add recent step outputs to sections."""
        step_files = sorted(
            [f for f in os.listdir(self._steps_dir)
             if f.endswith("_output.txt")],
        )
        recent = step_files[-last_n:] if step_files else []

        for fname in recent:
            idx = fname.split("_")[1]
            output = self._read(f"steps/{fname}")
            if not output:
                continue

            part = f"Step {idx} Output:\n{output}"
            if include_code:
                code = self._read(f"steps/step_{idx}_code.py")
                if code:
                    part = f"Step {idx} Code:\n```python\n{code}\n```\n\nStep {idx} Output:\n{output}"

            remaining = budget - used - 100
            if remaining <= 0:
                break
            if len(part) > remaining:
                part = part[:remaining] + "\n... (truncated)"
            sections.append((f"Step {idx} Result", part))
            used += len(part) + 50

    def _add_all_steps(
        self,
        sections: list[tuple[str, str]],
        used: int,
        budget: int,
    ) -> None:
        """Add all step outputs for finalizer."""
        step_files = sorted(
            [f for f in os.listdir(self._steps_dir)
             if f.endswith("_output.txt")],
        )
        for fname in step_files:
            idx = fname.split("_")[1]
            output = self._read(f"steps/{fname}")
            if not output:
                continue

            remaining = budget - used - 100
            if remaining <= 0:
                break
            if len(output) > remaining:
                output = output[:remaining] + "\n... (truncated)"
            sections.append((f"Step {idx} Result", output))
            used += len(output) + 50

    # --- Context summarization (compact) ---

    def compact(self, llm: object, question: str, budget: ContextBudget | None = None) -> str:
        """Summarize accumulated step outputs via a separate LLM call.

        Inspired by Claude Code's compact mechanism: when context grows too
        large, compress older step outputs into a structured summary.
        The original step files remain on disk for on-demand retrieval.

        Args:
            llm: LLMClient (typed as object to avoid circular import).
            question: The analysis question (for relevance-aware summarization).
            budget: Optional budget with compact_target_chars.

        Returns:
            The summary text.
        """
        from pathlib import Path

        # Collect all step outputs
        step_files = sorted(
            f for f in os.listdir(self._steps_dir) if f.endswith("_output.txt")
        )
        if not step_files:
            return ""

        parts: list[str] = []
        for fname in step_files:
            idx = fname.split("_")[1]
            output = self._read(f"steps/{fname}")
            code = self._read(f"steps/step_{idx}_code.py")
            part = f"### Step {idx}\n"
            if code:
                part += f"Code:\n```python\n{code[:2000]}\n```\n"
            part += f"Output:\n{output[:5000]}\n"
            parts.append(part)

        steps_text = "\n".join(parts)

        # Load compact prompt template
        prompt_path = Path(__file__).parent.parent / "prompts" / "compact.md"
        template = prompt_path.read_text(encoding="utf-8")
        system_prompt = (
            template
            .replace("{question}", question)
            .replace("{steps_text}", steps_text)
        )

        # Separate LLM call — does not pollute main agent context
        summary = llm.chat(system_prompt, "Summarize the analysis progress now.")

        # Track which step was last summarized
        last_step_idx = step_files[-1].split("_")[1]
        header = f"<!-- summarized_through: step_{last_step_idx} -->\n"
        summary = header + summary

        self._write("CONTEXT_SUMMARY.md", summary)
        return summary

    def read_context_summary(self) -> str:
        return self._read("CONTEXT_SUMMARY.md")

    def estimate_context_size(self) -> int:
        """Estimate the char count that get_context() would assemble.

        This mirrors the sections that get_context() actually reads,
        not the total disk usage. Used to decide when to trigger compact.
        """
        total = 0
        total += len(self.read_analysis_plan())
        total += len(self._read("DOMAIN_RULES_STRUCTURED.md") or self.read_domain_rules())
        total += len(self.read_data_profile())
        total += len(self.read_context_summary() or self.read_progress())
        total += len(self.read_lessons_learned())
        total += len(self.read_judge_guidance())
        # Recent step outputs (last 3 steps — worst case)
        step_files = sorted(
            f for f in os.listdir(self._steps_dir) if f.endswith("_output.txt")
        )
        for fname in step_files[-3:]:
            total += len(self._read(f"steps/{fname}"))
        return total

    # --- Lessons learned ---

    def append_lesson(self, step_idx: int, lesson: str) -> None:
        """Append a lesson learned to LESSONS_LEARNED.md."""
        line = f"- [step {step_idx}] {lesson}\n"
        path = os.path.join(self._workspace_dir, "LESSONS_LEARNED.md")
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)

    def read_lessons_learned(self) -> str:
        return self._read("LESSONS_LEARNED.md")

    # --- Persistence ---

    def persist(self) -> None:
        """Copy workspace to output_dir for post-run observability."""
        if not self.output_dir:
            return
        dest = os.path.join(self.output_dir, "workspace")
        if os.path.exists(dest):
            shutil.rmtree(dest)
        shutil.copytree(self._workspace_dir, dest)

    # --- Internal helpers ---

    def _write(self, rel_path: str, content: str) -> None:
        path = os.path.join(self._workspace_dir, rel_path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    def _read(self, rel_path: str) -> str:
        path = os.path.join(self._workspace_dir, rel_path)
        if not os.path.exists(path):
            return ""
        with open(path, "r", encoding="utf-8") as f:
            return f.read()


def _smart_truncate(text: str, max_chars: int, question: str) -> str:
    """Truncate long text intelligently, preserving most relevant sections.

    Strategy:
    1. If text fits in budget, return as-is
    2. Split into sections by headings
    3. Score each section by relevance to question (keyword overlap)
    4. Keep highest-scoring sections until budget filled
    5. Always keep first section (usually intro/overview)
    """
    if len(text) <= max_chars:
        return text

    # Split by markdown headings
    sections = re.split(r'(?=^#{1,3}\s)', text, flags=re.MULTILINE)
    if not sections:
        return text[:max_chars] + f"\n\n... (truncated from {len(text)} chars)"

    # Extract question keywords for relevance scoring
    q_words = set(re.findall(r'\w+', question.lower()))
    q_words -= {"the", "a", "an", "is", "are", "what", "which", "how", "for",
                "of", "in", "to", "and", "or", "with", "that", "this", "from"}

    # Score sections by keyword overlap with question
    scored: list[tuple[float, int, str]] = []
    for i, section in enumerate(sections):
        s_words = set(re.findall(r'\w+', section.lower()))
        overlap = len(q_words & s_words)
        # Boost first section (overview) and short sections (definitions)
        boost = 2.0 if i == 0 else (1.5 if len(section) < 500 else 1.0)
        scored.append((overlap * boost, i, section))

    # Sort by relevance (descending), then by position (ascending) for ties
    scored.sort(key=lambda x: (-x[0], x[1]))

    # Accumulate sections within budget
    kept: list[tuple[int, str]] = []
    total = 0
    for _score, idx, section in scored:
        if total + len(section) > max_chars:
            # Try to fit a truncated version
            remaining = max_chars - total
            if remaining > 200:
                kept.append((idx, section[:remaining] + "..."))
                total += remaining
            break
        kept.append((idx, section))
        total += len(section)

    # Re-sort by original position
    kept.sort(key=lambda x: x[0])

    result = "".join(s for _, s in kept)
    if total < len(text):
        result += f"\n\n... (showing {total}/{len(text)} chars, prioritized by relevance to question)"

    return result
