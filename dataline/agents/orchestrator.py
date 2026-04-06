"""Orchestrator: main loop that coordinates all agents.

Pipeline:
  Profiler → Analyzer → QuestionAnalyzer → Loop(Planner → Coder → Sandbox → Judge) → Finalizer

Workspace files provide observability and smart context retrieval.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from ..core.context_manager import ContextManager
from ..core.llm_client import LLMClient
from ..core.sandbox import Sandbox
from ..core.state import (
    add_step,
    create_initial_state,
    summarize_step_output,
    truncate_to_step,
    update_judge_guidance,
)
from ..core.tracer import TaskTracer
from ..core.tracing_llm import TracingLLMClient
from ..core.types import (
    AnalysisState,
    JudgeDecision,
    Manifest,
    PlanStep,
    SandboxResult,
    StepRecord,
)
from ..core.workspace import Workspace
from ..profiler import manifest as profiler
from ..profiler.manifest import manifest_to_json
from . import analyzer, planner, coder, judge, debugger, finalizer, question_analyzer
from .code_validator import validate_column_references


@dataclass
class TaskResult:
    """Complete result from running a task."""
    task_id: str
    question: str
    answer: dict[str, Any]         # {"col_name": [values]}
    steps: list[StepRecord]
    trace: list[dict]              # Full trace for diagnostics
    observations: dict[str, Any]   # Structured observation points
    total_tokens: int = 0
    total_cost_usd: float = 0.0
    time_seconds: float = 0.0
    success: bool = True
    error: str = ""
    benchmark: str = "kdd"


def run_task(
    task_dir: str,
    question: str,
    llm: LLMClient,
    config: dict,
    task_id: str = "",
    output_dir: str = "",
    benchmark: str = "kdd",
    guidelines: str = "",
    session_id: str = "",
) -> TaskResult:
    """Run the full agent pipeline on a single task."""
    start_time = time.time()
    trace: list[dict] = []
    max_iterations = config.get("agent", {}).get("max_iterations", 8)
    min_iterations = config.get("agent", {}).get("min_iterations", 2)
    max_retries = config.get("agent", {}).get("max_retries", 2)
    backtrack_limit = config.get("agent", {}).get("backtrack_limit", 3)
    stagnation_threshold = config.get("agent", {}).get("stagnation_threshold", 2)

    # Initialize tracer for real-time progress + LLM I/O logging
    tracer = TaskTracer(task_id, output_dir, session_id=session_id)
    traced_llm = TracingLLMClient(llm, tracer)

    sandbox = Sandbox(
        task_dir=task_dir,
        timeout=config.get("sandbox", {}).get("timeout_seconds", 120),
        max_memory_mb=config.get("sandbox", {}).get("max_memory_mb", 1024),
    )

    # Workspace: file-based state, persisted to output_dir/workspace/
    workspace = Workspace(temp_dir=sandbox.temp_dir, output_dir=output_dir)

    # Context manager: enforces token budget across all agent calls
    context_window = config.get("llm", {}).get("context_window", 262_144)
    cm = ContextManager(token_limit=context_window)

    obs: dict[str, Any] = {
        "profiler": {},
        "analyzer": {},
        "question_analyzer": {},
        "iterations": [],
        "final": {},
    }

    try:
        # 1. Profile (deterministic, no LLM)
        with tracer.span("profiler"):
            _log(trace, "profiler", "Scanning task directory")
            manifest = profiler.scan(task_dir)
            manifest_json = manifest_to_json(manifest)
            _log(trace, "profiler", f"Found {len(manifest.entries)} files, {len(manifest.cross_source_relations)} relations")

        obs["profiler"] = {
            "files_found": len(manifest.entries),
            "file_types": sorted({e.file_type for e in manifest.entries}),
            "cross_source_relations": len(manifest.cross_source_relations),
            "total_size_bytes": sum(e.size_bytes for e in manifest.entries),
            "coverage_signal": "complete" if manifest.entries else "empty",
        }

        # 2. Analyze (deep profiling via code + domain rule extraction)
        with tracer.span("analyzer"):
            _log(trace, "analyzer", "Running deep data analysis")
            data_profile, domain_rules_raw = analyzer.analyze(manifest, traced_llm, sandbox)
            _log(trace, "analyzer", f"Profile: {len(data_profile)} chars, domain rules: {len(domain_rules_raw)} chars")

        # 2b. Compile domain rules if they exceed budget fraction
        # Layer 1 compilation: recall-priority, question-agnostic
        with tracer.span("domain_compiler"):
            domain_rules = analyzer.compile_domain_rules(
                domain_rules_raw, traced_llm, cm.budget_tokens,
            )
            if len(domain_rules) < len(domain_rules_raw):
                _log(trace, "domain_compiler",
                     f"Compiled: {len(domain_rules_raw)} → {len(domain_rules)} chars")
            else:
                _log(trace, "domain_compiler", "No compilation needed")

        # Write to workspace for file-based access
        workspace.write_domain_rules(domain_rules)
        workspace.write_data_profile(data_profile)

        obs["analyzer"] = {
            "profile_length_chars": len(data_profile),
            "domain_rules_length_chars": len(domain_rules),
            "domain_rules_raw_chars": len(domain_rules_raw),
            "domain_rules_compiled": len(domain_rules) < len(domain_rules_raw),
            "analysis_success": len(data_profile) > 50,
            "profile_quality": "rich" if len(data_profile) > 500 else ("sparse" if len(data_profile) > 50 else "failed"),
        }

        # 3. Initialize AnalysisState (still used for compatibility)
        state = create_initial_state(task_id, question, manifest, data_profile, domain_rules)

        # 4. QuestionAnalyzer: pre-execution strategic analysis (GSD discuss-phase)
        with tracer.span("question_analyzer"):
            _log(trace, "question_analyzer", "Analyzing question strategy")
            analysis_plan = question_analyzer.analyze_question(
                question, state.manifest_summary, workspace, traced_llm,
            )
            _log(trace, "question_analyzer", f"Analysis plan: {len(analysis_plan)} chars")

        obs["question_analyzer"] = {
            "plan_length_chars": len(analysis_plan),
            "plan_generated": len(analysis_plan) > 50,
        }

        # Keep legacy steps_done for TaskResult output
        steps_done: list[StepRecord] = []
        backtracks_used = 0
        stagnation_count = 0

        # Track judge guidance for planner
        judge_guidance = ""

        # 5. Incremental plan-code-verify loop
        for iteration in range(max_iterations):
            tracer.set_iteration(iteration, max_iterations)
            _log(trace, "iteration", f"--- Iteration {iteration} ---")
            iter_obs: dict[str, Any] = {"iteration": iteration}

            # Update judge guidance from prior iteration
            if judge_guidance:
                state = update_judge_guidance(state, judge_guidance)
                workspace.write_judge_guidance(judge_guidance)

            # Plan next step
            with tracer.span("planner", metadata={"iteration": iteration}):
                _log(trace, "planner", "Planning next step")
                plan_step = planner.plan_next(
                    question, manifest_json, data_profile, steps_done, traced_llm,
                    state=state, cm=cm,
                )
            _log(trace, "planner", f"Plan: {plan_step.step_description}")
            iter_obs["plan_description"] = plan_step.step_description
            iter_obs["plan_sources"] = list(plan_step.data_sources)

            # Generate code
            with tracer.span("coder", metadata={"iteration": iteration}):
                _log(trace, "coder", "Generating code")
                code = coder.generate(plan_step, manifest_json, steps_done, traced_llm, state=state, cm=cm)
            _log(trace, "coder", f"Code length: {len(code)} chars")

            # Pre-execution validation: check column references against manifest
            annotated_code, col_warnings = validate_column_references(code, manifest)
            if col_warnings:
                _log(trace, "code_validator", f"Column warnings: {col_warnings}")
                code = annotated_code
                iter_obs["column_warnings"] = col_warnings

            # Execute
            step_id = f"step_{iteration}"
            with tracer.span("sandbox", metadata={"step_id": step_id}):
                _log(trace, "sandbox", f"Executing {step_id}")
                result = sandbox.execute(code, step_id=step_id)
            _log(trace, "sandbox", f"rc={result.return_code}, stdout={len(result.stdout)} chars")

            debug_retries = 0
            if result.return_code != 0:
                _log(trace, "debugger", f"Code failed: {result.stderr[:200]}")
                previous_attempts: list[tuple[str, str]] = []
                for retry in range(max_retries):
                    debug_retries += 1
                    with tracer.span("debugger", metadata={"retry": retry}):
                        fixed_code = debugger.fix(
                            code, result, manifest_json, data_profile, traced_llm,
                            state=state, cm=cm,
                            retry_number=retry,
                            previous_attempts=previous_attempts,
                        )
                    new_result = sandbox.execute(fixed_code, step_id=f"{step_id}_retry_{retry}")
                    _log(trace, "debugger", f"Retry {retry}: rc={new_result.return_code}")
                    previous_attempts.append((fixed_code[:500], new_result.stderr[:300]))
                    result = new_result
                    if result.return_code == 0:
                        code = fixed_code
                        break

            # Write step to workspace (observability)
            workspace.write_step(iteration, code, result.stdout)

            iter_obs["code_success"] = result.return_code == 0
            iter_obs["debug_retries"] = debug_retries
            iter_obs["exec_time_ms"] = result.execution_time_ms
            iter_obs["stdout_preview"] = result.stdout[:200].strip()
            iter_obs["stderr_preview"] = result.stderr[:200].strip() if result.return_code != 0 else ""

            step_record = StepRecord(
                plan=plan_step,
                code=code,
                result=result,
                step_index=iteration,
            )
            steps_done.append(step_record)

            # Update state with compressed finding
            finding = summarize_step_output(result.stdout)
            state = add_step(state, step_record, finding)
            workspace.append_progress(iteration, plan_step.step_description, finding)


            # Judge: combined sufficiency check + routing + guidance (single LLM call)
            with tracer.span("judge", metadata={"iteration": iteration}):
                _log(trace, "judge", "Evaluating progress")
                verdict = judge.evaluate(question, steps_done, traced_llm, state=state, cm=cm)
            _log(trace, "judge", f"sufficient={verdict.sufficient}, action={verdict.action}, missing={verdict.missing}")

            iter_obs["judge_sufficient"] = verdict.sufficient
            iter_obs["judge_action"] = verdict.action
            iter_obs["judge_reasoning"] = verdict.reasoning
            iter_obs["judge_missing"] = verdict.missing
            iter_obs["judge_guidance"] = verdict.guidance_for_next_step
            obs["iterations"].append(iter_obs)

            # Store guidance for next iteration's planner
            prev_guidance = judge_guidance
            judge_guidance = verdict.guidance_for_next_step

            if verdict.action == "finish":
                if iteration < min_iterations - 1:
                    _log(trace, "orchestrator",
                         f"Overriding premature finish at iteration {iteration} "
                         f"(min_iterations={min_iterations}). Forcing continue.")
                    verdict = JudgeDecision(
                        sufficient=False,
                        action="continue",
                        reasoning="Overridden: must verify results before concluding",
                        guidance_for_next_step=(
                            verdict.missing
                            or "Verify the results: cross-check against the data, "
                            "confirm row counts, and validate any assumptions made."
                        ),
                    )
                    judge_guidance = verdict.guidance_for_next_step
                else:
                    break
            elif verdict.action == "backtrack" and backtracks_used < backtrack_limit:
                truncate_to = max(0, min(verdict.truncate_to, len(steps_done) - 1))
                steps_done = steps_done[:truncate_to]
                state = truncate_to_step(state, truncate_to)
                backtracks_used += 1
                stagnation_count = 0
                judge_guidance = ""
                _log(trace, "judge", f"Backtracked to step {truncate_to}")
            else:
                # Stagnation detection
                step_failed = result.return_code != 0
                guidance_repeated = (
                    prev_guidance
                    and verdict.guidance_for_next_step
                    and prev_guidance.strip()[:80] == verdict.guidance_for_next_step.strip()[:80]
                )
                if step_failed or guidance_repeated:
                    stagnation_count += 1
                    reason = "code_failed" if step_failed else "guidance_repeated"
                    _log(trace, "orchestrator", f"Stagnation signal: {reason} (count={stagnation_count})")
                else:
                    stagnation_count = 0

                if stagnation_count >= stagnation_threshold:
                    # Instead of giving up, force the planner to change strategy.
                    # Only truly stop if we've already forced a strategy change
                    # and it still didn't help (stagnation_count doubles the threshold).
                    if stagnation_count >= stagnation_threshold * 2:
                        _log(trace, "orchestrator", f"Early stop: {stagnation_count} stagnation signals despite strategy change")
                        break
                    _log(trace, "orchestrator",
                         f"Stagnation detected ({stagnation_count} signals). "
                         f"Forcing planner to change strategy.")
                    judge_guidance = (
                        f"MANDATORY STRATEGY CHANGE: The previous approach has failed "
                        f"{stagnation_count} consecutive times. You MUST use a completely "
                        f"different strategy. Do NOT repeat the same type of code, query, "
                        f"or filtering logic. Try: different column names, different join "
                        f"strategy, alternative data loading method, or re-read the raw "
                        f"data from scratch."
                    )

        # 6. Finalize
        with tracer.span("finalizer"):
            _log(trace, "finalizer", "Formatting answer")
            answer = finalizer.format_answer(question, steps_done, traced_llm, state=state, cm=cm, benchmark=benchmark, guidelines=guidelines)
        _log(trace, "finalizer", f"Answer columns: {list(answer.keys())}")

        elapsed = time.time() - start_time
        usage = llm.total_usage

        obs["final"] = {
            "total_iterations": len(obs["iterations"]),
            "backtracks_used": backtracks_used,
            "answer_columns": list(answer.keys()),
            "answer_rows": len(next(iter(answer.values()), [])) if answer else 0,
            "success": True,
            "total_debug_retries": sum(i.get("debug_retries", 0) for i in obs["iterations"]),
            "code_failures": sum(1 for i in obs["iterations"] if not i.get("code_success", True)),
            "early_judge_exit": (
                len(obs["iterations"]) <= 2
                and any(i.get("judge_sufficient") for i in obs["iterations"])
            ),
            "stagnation_stops": stagnation_count >= stagnation_threshold,
        }

        # Persist workspace for post-run observability
        workspace.persist()
        tracer.set_observations(obs)
        tracer.finish(success=True)

        return TaskResult(
            task_id=task_id,
            question=question,
            answer=answer,
            steps=steps_done,
            trace=trace,
            observations=obs,
            total_tokens=usage.get("input_tokens", 0) + usage.get("output_tokens", 0),
            total_cost_usd=usage.get("cost_usd", 0.0),
            time_seconds=elapsed,
            benchmark=benchmark,
        )

    except Exception as e:
        elapsed = time.time() - start_time
        _log(trace, "error", str(e))
        obs["final"] = {"success": False, "error": str(e)}
        workspace.persist()  # persist even on failure for debugging
        tracer.set_observations(obs)
        tracer.finish(success=False, error=str(e))
        return TaskResult(
            task_id=task_id,
            question=question,
            answer={},
            steps=[],
            trace=trace,
            observations=obs,
            time_seconds=elapsed,
            success=False,
            error=str(e),
            benchmark=benchmark,
        )
    finally:
        sandbox.cleanup()


def _log(trace: list[dict], agent: str, message: str) -> None:
    trace.append({
        "timestamp": time.time(),
        "agent": agent,
        "message": message,
    })
