"""Analyzer agent: deep data profiling via code execution.

Returns two separate outputs:
- data_profile: column statistics, distributions, sample rows
- domain_rules: extracted from documentation files (manual, README, knowledge)
"""

from __future__ import annotations

import re
from pathlib import Path

from ..core.llm_client import LLMClient
from ..core.sandbox import Sandbox
from ..core.types import Manifest
from ..profiler.manifest import manifest_to_json


def analyze(manifest: Manifest, llm: LLMClient, sandbox: Sandbox) -> tuple[str, str]:
    """Generate and execute profiling code, return (data_profile, domain_rules).

    Strategy:
    1. Extract domain rules from documentation files (deterministic, no LLM)
    2. LLM generates profiling code for structured data → execute
    3. If execution fails, retry with a simpler prompt
    4. If all retries fail, use deterministic fallback (manifest summary)
    """
    # Step 1: Extract domain rules from documentation files (independent channel)
    domain_rules = _extract_domain_rules(manifest)

    # Step 2: Profile structured data
    manifest_json = manifest_to_json(manifest)

    profile = _attempt_llm_profile(manifest_json, llm, sandbox)
    if profile:
        return profile, domain_rules

    profile = _attempt_simple_profile(manifest_json, llm, sandbox)
    if profile:
        return profile, domain_rules

    return _deterministic_fallback(manifest_json), domain_rules


def _extract_domain_rules(manifest: Manifest) -> str:
    """Extract full content from documentation files as domain rules.

    Documentation files (manual.md, README, knowledge.md, etc.) contain
    formulas, definitions, and business rules that agents need to follow.
    These are preserved in full — they are HIGH PRIORITY information.

    This is deterministic (no LLM cost) and creates an independent channel
    so domain knowledge is never squeezed out by statistical profiles.
    """
    doc_extensions = {".md", ".txt", ".rst"}
    doc_parts: list[str] = []

    for entry in manifest.entries:
        if entry.file_type not in ("markdown", "pdf", "docx"):
            continue

        # Get text content from profiler's text_preview
        text = entry.summary.get("text_preview", "")
        if not text:
            continue

        # Read full file if text_preview was truncated
        file_path = entry.file_path
        if entry.summary.get("char_count", 0) > len(text):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    text = f.read()
            except (OSError, UnicodeDecodeError):
                pass  # fall back to text_preview

        doc_parts.append(f"=== {entry.file_path} ===\n{text}")

    return "\n\n".join(doc_parts)


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
5. For text files (md/txt): skip — already handled separately.
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


def extract_structured_rules(raw_rules: str, llm: LLMClient) -> str:
    """LLM-powered extraction of structured rules from long domain docs.

    Only called when len(raw_rules) > 50_000. Produces a compact, structured
    representation that preserves all rules while reducing token footprint.
    """
    prompt_path = Path(__file__).parent.parent / "prompts" / "domain_extractor.md"
    template = prompt_path.read_text(encoding="utf-8")

    # If extremely long, take first 100K chars (safety)
    rules_text = raw_rules[:100_000] if len(raw_rules) > 100_000 else raw_rules
    system_prompt = template.replace("{domain_rules_text}", rules_text)

    return llm.chat(system_prompt, "Extract all structured rules now.")


def _extract_code(response: str) -> str:
    """Extract Python code from markdown code blocks."""
    match = re.search(r"```python\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"```\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    return ""
