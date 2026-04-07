"""File-based analysis workspace: persistent state for a single task.

Agents write state files during execution. Files persist to output_dir
for post-run observability (inspect workspace/ to debug any task).

Directory structure:
    workspace/
    ├── DOMAIN_RULES.md     # Full documentation content
    ├── DATA_PROFILE.md     # Column stats, distributions
    ├── ANALYSIS_PLAN.md    # QuestionAnalyzer strategy output
    ├── PROGRESS.md         # Accumulated step findings
    ├── JUDGE_GUIDANCE.md   # Current judge steering signal
    └── steps/
        ├── step_0_code.py      # Code executed at step 0
        ├── step_0_output.txt   # Full stdout at step 0
        └── ...
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any


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


