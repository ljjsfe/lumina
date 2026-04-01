"""Debugger agent: fix failed code using traceback + data context."""

from __future__ import annotations

import re
from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.state import render_for_agent
from ..core.types import AnalysisState, SandboxResult


def fix(
    failed_code: str,
    result: SandboxResult,
    manifest_json: str,
    data_profile: str,
    llm: LLMClient,
    *,
    state: AnalysisState | None = None,
    retry_number: int = 0,
    previous_attempts: list[tuple[str, str]] | None = None,
) -> str:
    """Fix failed code using error info and data context.

    If state is provided, uses structured context for data context.
    Otherwise falls back to legacy manifest_json + data_profile.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "debugger.md"
    template = prompt_path.read_text(encoding="utf-8")

    error_type, error_message, full_traceback = _parse_error(result.stderr)
    error_category = classify_error(error_type)

    if state is not None:
        data_context = render_for_agent(state, "debugger")
    else:
        data_context = f"Manifest:\n{manifest_json[:3000]}\n\nProfile:\n{data_profile[:2000]}"

    retry_context = ""
    if previous_attempts:
        parts = []
        for i, (prev_code_snippet, prev_error) in enumerate(previous_attempts):
            parts.append(
                f"### Attempt {i + 1}\n"
                f"Code tried:\n```python\n{prev_code_snippet}\n```\n"
                f"Result: {prev_error}"
            )
        retry_context = "\n\n".join(parts)

    system_prompt = (
        template
        .replace("{failed_code}", failed_code)
        .replace("{full_traceback}", full_traceback)
        .replace("{error_type}", error_type)
        .replace("{error_message}", error_message)
        .replace("{error_category}", error_category)
        .replace("{retry_context}", retry_context)
        .replace("{data_context}", data_context)
    )

    response = llm.chat(system_prompt, "Fix the code now.")
    return _extract_code(response)


def _parse_error(stderr: str) -> tuple[str, str, str]:
    """Extract error type, message, and full traceback from stderr."""
    lines = stderr.strip().split("\n")
    if not lines:
        return "UnknownError", "No error output", ""

    last_line = lines[-1].strip()

    # Extract error type and message from last line
    if ": " in last_line:
        parts = last_line.split(": ", 1)
        error_type, error_msg = parts[0], parts[1]
    else:
        error_type, error_msg = "RuntimeError", last_line

    # Full traceback — no truncation (each API call has its own context window)
    full_tb = stderr.strip()
    if len(full_tb) > 100_000:
        full_tb = full_tb[-100_000:]

    return error_type, error_msg, full_tb


# Error categories for targeted fix strategies
_DATA_ERRORS = {"KeyError", "ColumnNotFoundError", "FileNotFoundError", "UnicodeDecodeError", "JSONDecodeError"}
_TYPE_ERRORS = {"TypeError", "ValueError", "AttributeError"}
_RESOURCE_ERRORS = {"MemoryError", "TimeoutError"}


def classify_error(error_type: str) -> str:
    """Classify error into category for targeted fix strategy."""
    if error_type in _DATA_ERRORS:
        return "data"
    if error_type in _TYPE_ERRORS:
        return "type"
    if error_type in _RESOURCE_ERRORS:
        return "resource"
    return "logic"


def _extract_code(response: str) -> str:
    match = re.search(r"```python\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"```\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    return response.strip()
