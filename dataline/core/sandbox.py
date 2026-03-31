"""Python code execution sandbox with persistent state across steps."""

from __future__ import annotations

import os
import pickle
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

from .types import SandboxResult


class Sandbox:
    """Execute Python code in isolated subprocess with persistent temp directory."""

    def __init__(
        self,
        task_dir: str,
        timeout: int = 120,
        max_memory_mb: int = 1024,
    ):
        self._task_dir = os.path.abspath(task_dir)
        self._timeout = timeout
        self._max_memory_mb = max_memory_mb
        self._temp_dir = tempfile.mkdtemp(prefix="dataline_")
        self._step_count = 0
        self._install_helpers()

    @property
    def temp_dir(self) -> str:
        return self._temp_dir

    def execute(self, code: str, step_id: str | None = None) -> SandboxResult:
        """Execute Python code. Results persist in temp_dir."""
        if step_id is None:
            step_id = f"step_{self._step_count}"
        self._step_count += 1

        # Write code to temp file
        code_path = Path(self._temp_dir) / f"{step_id}.py"
        code_path.write_text(code, encoding="utf-8")

        env = os.environ.copy()
        env["TASK_DIR"] = self._task_dir
        env["TEMP_DIR"] = self._temp_dir
        env["PYTHONIOENCODING"] = "utf-8"

        start = time.time()
        try:
            proc = subprocess.run(
                ["python", str(code_path)],
                capture_output=True,
                text=True,
                timeout=self._timeout,
                env=env,
                cwd=self._temp_dir,
            )
            elapsed_ms = int((time.time() - start) * 1000)

            return SandboxResult(
                stdout=proc.stdout[:10000],  # cap output size
                stderr=proc.stderr[:5000],
                return_code=proc.returncode,
                execution_time_ms=elapsed_ms,
                step_id=step_id,
            )
        except subprocess.TimeoutExpired:
            elapsed_ms = int((time.time() - start) * 1000)
            return SandboxResult(
                stdout="",
                stderr=f"Timeout: execution exceeded {self._timeout}s limit",
                return_code=-1,
                execution_time_ms=elapsed_ms,
                step_id=step_id,
            )
        except Exception as e:
            elapsed_ms = int((time.time() - start) * 1000)
            return SandboxResult(
                stdout="",
                stderr=f"Sandbox error: {e}",
                return_code=-2,
                execution_time_ms=elapsed_ms,
                step_id=step_id,
            )

    def _install_helpers(self) -> None:
        """Copy data_helpers.py to TEMP_DIR so generated code can import it."""
        helpers_src = Path(__file__).parent.parent / "helpers" / "data_helpers.py"
        if helpers_src.exists():
            helpers_dst = Path(self._temp_dir) / "data_helpers.py"
            shutil.copy2(str(helpers_src), str(helpers_dst))

    def save_step_result(self, step_id: str, data: object) -> str:
        """Save step result as pickle for later steps to use."""
        path = Path(self._temp_dir) / f"{step_id}_result.pkl"
        with open(path, "wb") as f:
            pickle.dump(data, f)
        return str(path)

    def cleanup(self) -> None:
        """Remove temp directory."""
        if os.path.exists(self._temp_dir):
            shutil.rmtree(self._temp_dir, ignore_errors=True)

    def __del__(self) -> None:
        self.cleanup()
