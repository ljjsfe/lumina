"""Compare two eval runs."""

from __future__ import annotations

from ..core.types import CompareReport, EvalReport


def compare_runs(run_a: EvalReport, run_b: EvalReport) -> CompareReport:
    """Compare two runs. Positive delta = B is better."""
    scores_a = {s.task_id: s.score for s in run_a.task_scores}
    scores_b = {s.task_id: s.score for s in run_b.task_scores}

    all_tasks = set(scores_a.keys()) | set(scores_b.keys())

    improved = []
    regressed = []
    for task_id in sorted(all_tasks):
        a = scores_a.get(task_id, 0)
        b = scores_b.get(task_id, 0)
        if a == 0 and b == 1:
            improved.append(task_id)
        elif a == 1 and b == 0:
            regressed.append(task_id)

    per_diff_delta = {}
    for diff in ["easy", "medium", "hard", "extreme"]:
        a_val = run_a.per_difficulty.get(diff, 0.0)
        b_val = run_b.per_difficulty.get(diff, 0.0)
        per_diff_delta[diff] = b_val - a_val

    return CompareReport(
        accuracy_delta=run_b.overall_accuracy - run_a.overall_accuracy,
        improved_tasks=tuple(improved),
        regressed_tasks=tuple(regressed),
        per_difficulty_delta=per_diff_delta,
    )


def format_comparison(report: CompareReport) -> str:
    """Format comparison as readable text."""
    lines = [
        f"=== RUN COMPARISON ===",
        f"Accuracy delta: {report.accuracy_delta:+.1%}",
        f"",
        f"Per-difficulty delta:",
    ]
    for diff, delta in report.per_difficulty_delta.items():
        lines.append(f"  {diff:>8}: {delta:+.1%}")

    lines.append(f"\nImproved ({len(report.improved_tasks)} tasks): {', '.join(report.improved_tasks[:10])}")
    lines.append(f"Regressed ({len(report.regressed_tasks)} tasks): {', '.join(report.regressed_tasks[:10])}")

    if report.regressed_tasks:
        lines.append(f"\n⚠ REGRESSIONS DETECTED — investigate before committing")

    return "\n".join(lines)
