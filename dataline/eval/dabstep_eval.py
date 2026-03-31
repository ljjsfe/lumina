"""DABstep evaluation: scalar answer matching against the official dev set."""

from __future__ import annotations

import json
import os
from collections import Counter
from pathlib import Path

from ..core.types import EvalReport, TaskScore
from .dabstep_scorer import score_answer
from .failure_analysis import categorize_failure, identify_failed_agent


def load_dabstep_tasks(dabstep_dir: str) -> dict[str, dict]:
    """Load dev tasks from local JSON file.

    Returns:
        {task_id: {"question": ..., "answer": ..., "level": ..., "guidelines": ...}}
    """
    tasks_file = Path(dabstep_dir) / "dev_tasks.json"
    if not tasks_file.exists():
        raise FileNotFoundError(
            f"DABstep dev tasks not found at {tasks_file}. "
            "Run: python -c \"from datasets import load_dataset; import json; "
            "ds = load_dataset('adyen/DABstep', name='tasks', split='dev'); "
            "json.dump([dict(r) for r in ds], open('data/dabstep/dev_tasks.json','w'))\""
        )
    with open(tasks_file, encoding="utf-8") as f:
        tasks_list = json.load(f)
    return {str(t["task_id"]): t for t in tasks_list}


def run_dabstep_eval(
    results_dir: str,
    dabstep_dir: str,
    task_ids: list[str] | None = None,
) -> EvalReport:
    """Evaluate DABstep tasks.

    Args:
        results_dir: Directory with task_id/prediction.csv and task_id/trace.json
        dabstep_dir: DABstep root directory (data/dabstep/)
    """
    tasks = load_dabstep_tasks(dabstep_dir)

    if task_ids is not None:
        tasks = {k: v for k, v in tasks.items() if k in task_ids}

    scores: list[TaskScore] = []
    failure_counts: Counter = Counter()
    agent_failures: Counter = Counter()
    total_tokens = 0
    total_cost = 0.0

    for task_id, task_meta in sorted(tasks.items()):
        gold_answer = str(task_meta.get("answer", ""))
        level = task_meta.get("level", "")

        pred_path = os.path.join(results_dir, task_id, "prediction.csv")
        trace_path = os.path.join(results_dir, task_id, "trace.json")

        if not os.path.exists(pred_path):
            ts = TaskScore(
                task_id=task_id, score=0, difficulty=level,
                failure_category="empty_answer",
                failed_at_agent="orchestrator",
                error_detail="No prediction.csv found",
                suggestion="Task was not executed or crashed before producing output",
            )
            scores.append(ts)
            failure_counts["empty_answer"] += 1
            agent_failures["orchestrator"] += 1
            continue

        # Extract predicted answer from prediction.csv
        pred_answer = _extract_scalar_from_csv(pred_path)

        # Load trace for diagnostics
        trace = []
        tokens = 0
        cost = 0.0
        time_s = 0.0
        steps = 0
        if os.path.exists(trace_path):
            with open(trace_path, encoding="utf-8") as f:
                trace_data = json.load(f)
                trace = trace_data.get("trace", [])
                tokens = trace_data.get("total_tokens", 0)
                cost = trace_data.get("total_cost_usd", 0.0)
                time_s = trace_data.get("time_seconds", 0.0)
                steps = trace_data.get("steps_executed", 0)

        total_tokens += tokens
        total_cost += cost

        task_score_val = score_answer(pred_answer, gold_answer)

        ts = TaskScore(
            task_id=task_id,
            score=task_score_val,
            difficulty=level,
            tokens_used=tokens,
            cost_usd=cost,
            time_seconds=time_s,
            steps_executed=steps,
        )

        if task_score_val == 0:
            category = categorize_failure(ts, trace)
            failed_agent = identify_failed_agent(trace)
            ts = TaskScore(
                task_id=task_id, score=0, difficulty=level,
                failure_category=category,
                failed_at_agent=failed_agent,
                tokens_used=tokens,
                cost_usd=cost,
                time_seconds=time_s,
                steps_executed=steps,
                error_detail=f"pred='{pred_answer[:60]}' gold='{gold_answer[:60]}'",
            )
            failure_counts[category] += 1
            agent_failures[failed_agent] += 1

        scores.append(ts)

    total = len(scores)
    correct = sum(1 for s in scores if s.score == 1)
    overall = correct / total if total > 0 else 0.0

    per_diff: dict[str, float] = {}
    for diff in ["easy", "hard"]:
        diff_tasks = [s for s in scores if s.difficulty == diff]
        if diff_tasks:
            per_diff[diff] = sum(1 for s in diff_tasks if s.score == 1) / len(diff_tasks)

    suggestions = _generate_suggestions(failure_counts, agent_failures, scores)

    return EvalReport(
        overall_accuracy=overall,
        per_difficulty=per_diff,
        task_scores=tuple(scores),
        failure_breakdown=dict(failure_counts),
        agent_bottlenecks=dict(agent_failures),
        total_tokens=total_tokens,
        total_cost_usd=total_cost,
        suggestions=tuple(suggestions),
    )


def _extract_scalar_from_csv(pred_path: str) -> str:
    """Extract the predicted scalar answer from prediction.csv.

    DABstep answers are scalars. We take the first non-header cell value,
    trying to find the most likely answer column.
    """
    try:
        import pandas as pd
        df = pd.read_csv(pred_path)
        if df.empty:
            return ""
        # If there's an "answer" column, use that
        for col in df.columns:
            if col.lower() in ("answer", "result", "value"):
                val = df[col].iloc[0]
                return str(val).strip() if val is not None else ""
        # Otherwise take the first value of the first column
        val = df.iloc[0, 0]
        return str(val).strip() if val is not None else ""
    except Exception:
        return ""


def _generate_suggestions(
    failures: Counter,
    agent_fails: Counter,
    scores: list[TaskScore],
) -> list[str]:
    suggestions = []
    total_failed = sum(failures.values())
    if total_failed == 0:
        return ["All tasks passed! Consider running on the full test set."]

    for category, count in failures.most_common(3):
        pct = count / total_failed * 100
        if category == "code_error":
            suggestions.append(
                f"[HIGH] Fix code generation ({count} tasks, {pct:.0f}%). "
                "Improve coder prompt with payments schema context."
            )
        elif category == "wrong_direction":
            suggestions.append(
                f"[HIGH] Improve planning ({count} tasks, {pct:.0f}%). "
                "Planner needs better manual.md utilization."
            )
        elif category == "format_error":
            suggestions.append(
                f"[MED] Fix scalar extraction ({count} tasks, {pct:.0f}%). "
                "finalizer needs to output a clean scalar, not a table."
            )
        elif category == "empty_answer":
            suggestions.append(
                f"[HIGH] Fix empty outputs ({count} tasks, {pct:.0f}%). "
                "Tasks not producing any prediction."
            )
        else:
            suggestions.append(
                f"[MED] Address '{category}' failures ({count} tasks, {pct:.0f}%)."
            )

    if agent_fails:
        worst_agent, worst_count = agent_fails.most_common(1)[0]
        suggestions.append(
            f"[INFO] Most failures at: {worst_agent} ({worst_count} tasks)."
        )

    return suggestions


def format_dabstep_report(report: EvalReport) -> str:
    """Format DABstep EvalReport as human-readable dashboard."""
    total = len(report.task_scores)
    correct = sum(1 for s in report.task_scores if s.score == 1)

    lines = [
        f"=== DABSTEP EVAL: {total} tasks ===",
        "",
        f"Overall: {correct}/{total} ({report.overall_accuracy:.1%})",
    ]

    for diff in ["easy", "hard"]:
        if diff in report.per_difficulty:
            diff_tasks = [s for s in report.task_scores if s.difficulty == diff]
            diff_correct = sum(1 for s in diff_tasks if s.score == 1)
            lines.append(f"  {diff:>6}: {diff_correct}/{len(diff_tasks)} ({report.per_difficulty[diff]:.1%})")

    # Per-task breakdown
    lines.append("\n=== PER-TASK ===")
    for s in sorted(report.task_scores, key=lambda x: x.task_id):
        status = "✓" if s.score == 1 else "✗"
        detail = s.error_detail[:60] if s.error_detail else ""
        lines.append(f"  {status} [{s.difficulty:5}] task {s.task_id:<6} {detail}")

    if report.failure_breakdown:
        lines.append("\n=== FAILURE BREAKDOWN ===")
        for cat, count in sorted(report.failure_breakdown.items(), key=lambda x: -x[1]):
            total_failed = sum(report.failure_breakdown.values())
            pct = count / total_failed * 100 if total_failed > 0 else 0
            lines.append(f"  {cat:>20}: {count} ({pct:.0f}%)")

    lines.append("\n=== TOKEN ECONOMICS ===")
    lines.append(f"  Total: {report.total_tokens:,} tokens / ${report.total_cost_usd:.2f}")
    if total > 0:
        lines.append(f"  Avg per task: {report.total_tokens // max(total, 1):,} tokens / ${report.total_cost_usd / total:.3f}")

    if report.suggestions:
        lines.append("\n=== TOP IMPROVEMENTS ===")
        for i, s in enumerate(report.suggestions, 1):
            lines.append(f"  {i}. {s}")

    return "\n".join(lines)
