#!/usr/bin/env python3
"""Quick eval report from status.json files — no gold data needed.

Usage:
    python eval_report.py <results_dir>
    python eval_report.py results/eval_ctx_20260405_2354
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path


def _classify_error(message: str) -> str:
    """Map a failure message to a short error category."""
    msg = message.lower()
    if "token limit" in msg or "exceeded model token" in msg:
        return "token_limit"
    if "timeout" in msg or "timed out" in msg:
        return "timeout"
    if "rate limit" in msg or "429" in msg:
        return "rate_limit"
    if "connection" in msg or "network" in msg:
        return "network"
    if "syntax" in msg or "nameerror" in msg or "typeerror" in msg or "valueerror" in msg:
        return "code_error"
    if "401" in msg or "403" in msg or "unauthorized" in msg:
        return "auth_error"
    if "500" in msg or "502" in msg or "503" in msg:
        return "server_error"
    if "failed" in msg or "error" in msg:
        return "other_error"
    return "unknown"


def load_status_files(results_dir: Path) -> list[dict]:
    """Recursively find and load all status.json files under results_dir."""
    statuses = []
    for path in sorted(results_dir.rglob("status.json")):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
                data["_path"] = str(path)
                statuses.append(data)
        except Exception as e:
            print(f"  [warn] Could not read {path}: {e}", file=sys.stderr)
    return statuses


def generate_report(results_dir: str) -> str:
    root = Path(results_dir)
    if not root.exists():
        return f"[error] Directory not found: {results_dir}"

    statuses = load_status_files(root)
    if not statuses:
        return f"[error] No status.json files found under {results_dir}"

    total = len(statuses)
    completed = [s for s in statuses if s.get("status") == "completed"]
    failed = [s for s in statuses if s.get("status") == "failed"]
    other = [s for s in statuses if s.get("status") not in ("completed", "failed")]

    # Tokens / cost / time
    total_tokens = sum(s.get("tokens_used", 0) for s in statuses)
    total_cost = sum(s.get("cost_usd", 0.0) for s in statuses)
    total_time = sum(s.get("elapsed_seconds", 0.0) for s in statuses)

    # Iteration stats (completed only)
    iters = [s.get("current_iteration", 0) for s in completed]
    avg_iter = sum(iters) / len(iters) if iters else 0.0
    max_iter_task = max(completed, key=lambda s: s.get("current_iteration", 0)) if completed else None

    # Error categories
    error_cats: Counter = Counter()
    for s in failed:
        msg = s.get("message", "")
        error_cats[_classify_error(msg)] += 1

    lines: list[str] = []
    lines.append(f"=== EVAL REPORT: {root.name} ===")
    lines.append(f"")
    lines.append(f"Total tasks  : {total}")
    lines.append(f"  completed  : {len(completed)}  ({len(completed)/total:.1%})")
    lines.append(f"  failed     : {len(failed)}  ({len(failed)/total:.1%})")
    if other:
        lines.append(f"  other      : {len(other)}")

    lines.append(f"")
    lines.append(f"=== TOKEN ECONOMICS ===")
    lines.append(f"  Total tokens : {total_tokens:,}")
    lines.append(f"  Total cost   : ${total_cost:.3f}")
    lines.append(f"  Total time   : {total_time/60:.1f} min")
    if total > 0:
        lines.append(f"  Avg / task   : {total_tokens // total:,} tok  ${total_cost / total:.3f}  {total_time / total:.0f}s")

    if completed:
        lines.append(f"")
        lines.append(f"=== ITERATION STATS (completed) ===")
        lines.append(f"  Avg iterations : {avg_iter:.1f}")
        if max_iter_task:
            lines.append(
                f"  Max iterations : {max_iter_task.get('current_iteration')} "
                f"({max_iter_task.get('task_id')})"
            )
        steps = [s.get("steps_completed", 0) for s in completed]
        lines.append(f"  Avg steps      : {sum(steps)/len(steps):.1f}")

    if failed:
        lines.append(f"")
        lines.append(f"=== FAILURE BREAKDOWN ({len(failed)} tasks) ===")
        for cat, count in error_cats.most_common():
            pct = count / len(failed) * 100
            lines.append(f"  {cat:>15} : {count} ({pct:.0f}%)")

        lines.append(f"")
        lines.append(f"=== FAILED TASKS ===")
        for s in sorted(failed, key=lambda x: x.get("task_id", "")):
            tid = s.get("task_id", "?")
            msg = s.get("message", "")
            # Trim long messages
            display_msg = msg[:100] + "..." if len(msg) > 100 else msg
            lines.append(f"  [{tid}] {display_msg}")

    if other:
        lines.append(f"")
        lines.append(f"=== OTHER STATUS ===")
        for s in other:
            lines.append(f"  [{s.get('task_id', '?')}] status={s.get('status')} msg={s.get('message', '')[:80]}")

    return "\n".join(lines)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python eval_report.py <results_dir>")
        print("Example: python eval_report.py results/eval_ctx_20260405_2354")
        sys.exit(1)

    print(generate_report(sys.argv[1]))
