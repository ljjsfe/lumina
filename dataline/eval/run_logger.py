"""Central run logger — auto-appends every eval run to a persistent JSONL log."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path

from ..core.types import EvalReport


LOG_FILE = "results/runs_log.jsonl"


def log_run(
    report: EvalReport,
    benchmark: str,            # "kdd" | "dabstep"
    run_label: str,            # e.g. "baseline", "fix-coder-prompt-v2"
    model: str,
    provider: str,
    sample_size: int,
    config_notes: str = "",
) -> str:
    """Append a run to the central log. Returns the run_id."""
    run_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{run_label.replace(' ', '-')}"

    entry = {
        "run_id": run_id,
        "timestamp": datetime.now().isoformat(),
        "benchmark": benchmark,
        "label": run_label,
        "model": model,
        "provider": provider,
        "sample_size": sample_size,
        "config_notes": config_notes,
        # Scores
        "overall_accuracy": round(report.overall_accuracy, 4),
        "per_difficulty": report.per_difficulty,
        # Cost
        "total_tokens": report.total_tokens,
        "total_cost_usd": round(report.total_cost_usd, 4),
        "avg_tokens_per_task": report.total_tokens // max(sample_size, 1),
        # Failures
        "failure_breakdown": report.failure_breakdown,
        "agent_bottlenecks": report.agent_bottlenecks,
        # Top suggestion
        "top_suggestion": report.suggestions[0] if report.suggestions else "",
    }

    os.makedirs("results", exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    return run_id


def load_history() -> list[dict]:
    """Load all logged runs, newest first."""
    if not os.path.exists(LOG_FILE):
        return []
    entries = []
    with open(LOG_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return list(reversed(entries))


def format_history(benchmark_filter: str | None = None, last_n: int = 20) -> str:
    """Format run history as a progress table."""
    history = load_history()
    if benchmark_filter:
        history = [h for h in history if h.get("benchmark") == benchmark_filter]
    history = history[:last_n]

    if not history:
        return "No runs logged yet."

    lines = [
        "=== RUN HISTORY ===",
        f"{'Run ID':<30} {'Benchmark':<12} {'Model':<20} {'Overall':>8} {'Easy':>6} {'Med':>6} {'Hard':>6} {'Tokens':>10} {'Cost':>8} {'Top Bottleneck'}",
        "-" * 130,
    ]

    for h in history:
        pd = h.get("per_difficulty", {})
        bottlenecks = h.get("agent_bottlenecks", {})
        top_agent = max(bottlenecks, key=bottlenecks.get) if bottlenecks else "-"
        top_agent_count = bottlenecks.get(top_agent, 0) if bottlenecks else 0

        lines.append(
            f"{h['run_id']:<30} "
            f"{h.get('benchmark','?'):<12} "
            f"{h.get('model','?'):<20} "
            f"{h.get('overall_accuracy', 0):>7.1%} "
            f"{pd.get('easy', 0):>6.1%} "
            f"{pd.get('medium', pd.get('medium', 0)):>6.1%} "
            f"{pd.get('hard', 0):>6.1%} "
            f"{h.get('total_tokens', 0):>10,} "
            f"${h.get('total_cost_usd', 0):>6.3f} "
            f"  {top_agent}({top_agent_count})"
        )

    # Show trend if ≥2 runs
    same_bench = [h for h in history if h.get("benchmark") == history[0].get("benchmark")]
    if len(same_bench) >= 2:
        latest = same_bench[0]["overall_accuracy"]
        prev = same_bench[1]["overall_accuracy"]
        delta = latest - prev
        trend = f"+{delta:.1%}" if delta >= 0 else f"{delta:.1%}"
        lines.append("-" * 130)
        lines.append(f"Trend (latest vs previous): {trend}")

    return "\n".join(lines)


def format_best_runs() -> str:
    """Show best run per benchmark."""
    history = load_history()
    if not history:
        return "No runs yet."

    best: dict[str, dict] = {}
    for h in history:
        bench = h.get("benchmark", "?")
        if bench not in best or h["overall_accuracy"] > best[bench]["overall_accuracy"]:
            best[bench] = h

    lines = ["=== BEST RUNS ==="]
    for bench, h in best.items():
        lines.append(
            f"{bench}: {h['overall_accuracy']:.1%} | {h['model']} | {h['run_id']} | ${h['total_cost_usd']:.3f}"
        )
    return "\n".join(lines)
