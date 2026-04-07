#!/usr/bin/env python3
"""Launch KDD dev set (10 stratified tasks) with monitoring."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime

from dataline.eval.dev_sets import KDD_DEV, describe_dev_set


def launch_task(task_id: str, results_base: str) -> dict:
    """Launch a task in the background using Popen (non-blocking)."""
    task_data_dir = f"data/demo/input/{task_id}"
    task_output_dir = f"{results_base}/{task_id}"

    if not os.path.exists(task_data_dir):
        return {"task_id": task_id, "status": "skipped", "reason": f"No data dir"}

    print(f"  Launching {task_id:>10}...", end=" ", flush=True)

    try:
        # Ensure output dir exists
        os.makedirs(task_output_dir, exist_ok=True)

        # Use Popen for non-blocking launch (let main.py handle async)
        with open(f"{task_output_dir}/launch.log", "w") as logf:
            subprocess.Popen(
                ["python", "main.py", "run", "--task", task_data_dir, "--output", task_output_dir],
                stdout=logf,
                stderr=subprocess.STDOUT,
                text=True,
            )
        print(f"✓")
        return {"task_id": task_id, "status": "launched"}
    except Exception as e:
        print(f"✗ ({type(e).__name__})")
        return {"task_id": task_id, "status": "failed_to_launch", "detail": str(e)}


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run KDD dev set eval")
    parser.add_argument(
        "--output",
        default=f"results/kdd_dev_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        help="Output directory for results",
    )
    parser.add_argument("--limit", type=int, default=None, help="Run only first N tasks")
    args = parser.parse_args()

    results_base = args.output
    os.makedirs(results_base, exist_ok=True)

    print(f"KDD Dev Eval")
    print(f"============")
    print(describe_dev_set("kdd"))
    print(f"\nOutput: {results_base}")
    print(f"Tasks:  {len(KDD_DEV)}")
    print()

    tasks_to_run = KDD_DEV[: args.limit] if args.limit else KDD_DEV
    results = []

    for i, task_id in enumerate(tasks_to_run, 1):
        print(f"[{i}/{len(tasks_to_run)}]", end=" ")
        result = launch_task(task_id, results_base)
        results.append(result)

    print()

    # Summary
    print("=== LAUNCH SUMMARY ===")
    launched = [r for r in results if r["status"] == "launched"]
    failed_launch = [r for r in results if r["status"] == "failed_to_launch"]
    skipped = [r for r in results if r["status"] == "skipped"]

    print(f"Launched: {len(launched)}/{len(results)}")
    if failed_launch:
        print(f"Failed  : {len(failed_launch)}")
        for r in failed_launch:
            print(f"  - {r['task_id']}: {r.get('detail', '?')}")
    if skipped:
        print(f"Skipped : {len(skipped)}")

    print()
    print("ℹ️  Tasks are running in background. Monitor progress with:")
    print(f"    python eval_report.py {results_base}")
    print(f"    python monitor.py {results_base}")
    print()
    print("Or check individual task status:")

    # Save launch manifest
    manifest = {
        "benchmark": "kdd",
        "launched_at": datetime.now().isoformat(),
        "dev_set": list(tasks_to_run),
        "launched": len(launched),
        "failed_to_launch": len(failed_launch),
        "skipped": len(skipped),
        "results": results,
    }
    with open(f"{results_base}/launch_manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

    for r in results:
        if r["status"] == "launched":
            task_id = r["task_id"]
            print(f"    ls {results_base}/{task_id}/status.json")


if __name__ == "__main__":
    main()
