"""Analyzer agent: deep data profiling via code execution."""

from __future__ import annotations

import re
from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.sandbox import Sandbox
from ..core.types import Manifest
from ..profiler.manifest import manifest_to_json


def analyze(manifest: Manifest, llm: LLMClient, sandbox: Sandbox) -> str:
    """Generate and execute profiling code, return semantic data profile.

    Strategy:
    1. LLM generates profiling code → execute
    2. If execution fails, retry with a simpler prompt
    3. If all retries fail, use deterministic fallback (manifest summary)
    """
    manifest_json = manifest_to_json(manifest)

    # Attempt 1: Full LLM-generated profiling
    profile = _attempt_llm_profile(manifest_json, llm, sandbox)
    if profile:
        return profile

    # Attempt 2: Retry with simplified prompt (common failure: encoding, large files)
    profile = _attempt_simple_profile(manifest_json, llm, sandbox)
    if profile:
        return profile

    # Attempt 3: Deterministic fallback — always produces something useful
    return _deterministic_fallback(manifest_json)


def _attempt_llm_profile(manifest_json: str, llm: LLMClient, sandbox: Sandbox) -> str:
    """Full LLM-generated profiling code."""
    prompt_path = Path(__file__).parent.parent / "prompts" / "analyzer.md"
    system_prompt = prompt_path.read_text(encoding="utf-8")
    system_prompt = system_prompt.replace("{manifest_json}", manifest_json)

    response = llm.chat(system_prompt, "Generate the profiling code now.")

    code = _extract_code(response)
    if not code:
        code = response

    result = sandbox.execute(code, step_id="analyzer")

    if result.return_code == 0 and len(result.stdout.strip()) > 50:
        return result.stdout

    return ""


def _attempt_simple_profile(manifest_json: str, llm: LLMClient, sandbox: Sandbox) -> str:
    """Retry with a simpler prompt focused on robustness."""
    simple_prompt = f"""Write Python code to profile these data files. Keep it simple and robust.

## Files
{manifest_json}

## Rules
1. TASK_DIR environment variable has the data path.
2. For CSV files: use pandas with encoding='utf-8' first, then 'latin-1' as fallback. Print: shape, column names with dtypes, null counts, and for each column: unique count, min, max, and top 5 most frequent values.
3. For JSON files: load with json module. Print: type (list/dict), length, and if list of dicts, print all keys and for each key show unique value count and top 5 values.
4. For SQLite: print table names, schemas, row counts, and 3 sample rows per table.
5. For text files (md/txt): print first 500 characters.
6. Wrap each file in try/except to ensure one file failure doesn't stop the whole script.
7. Print "=== <filename> ===" header before each file summary.
"""

    response = llm.chat(simple_prompt, "Generate the profiling code now. Keep it simple.")

    code = _extract_code(response)
    if not code:
        code = response

    result = sandbox.execute(code, step_id="analyzer_retry")

    if result.return_code == 0 and len(result.stdout.strip()) > 50:
        return result.stdout

    return ""


def _deterministic_fallback(manifest_json: str) -> str:
    """Always-works fallback using manifest metadata. No LLM needed."""
    return (
        "Data profile (from manifest metadata — profiling code failed, "
        "so this is schema-level only. You MUST explore the actual data "
        "in your first step before making assumptions about values):\n\n"
        f"{manifest_json}"
    )


def _extract_code(response: str) -> str:
    """Extract Python code from markdown code blocks."""
    match = re.search(r"```python\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"```\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    return ""
