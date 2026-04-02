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

from ..core.context_budget import ContextBudget, compute_budget, estimate_complexity
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
) -> TaskResult:
    """Run the full agent pipeline on a single task."""
    start_time = time.time()
    trace: list[dict] = []
    max_iterations = config.get("agent", {}).get("max_iterations", 8)
    min_iterations = config.get("agent", {}).get("min_iterations", 1)
    max_retries = config.get("agent", {}).get("max_retries", 2)
    backtrack_limit = config.get("agent", {}).get("backtrack_limit", 3)
    stagnation_threshold = config.get("agent", {}).get("stagnation_threshold", 2)

    # Initialize tracer for real-time progress + LLM I/O logging
    tracer = TaskTracer(task_id, output_dir)
    traced_llm = TracingLLMClient(llm, tracer)

    sandbox = Sandbox(
        task_dir=task_dir,
        timeout=config.get("sandbox", {}).get("timeout_seconds", 120),
        max_memory_mb=config.get("sandbox", {}).get("max_memory_mb", 1024),
    )

    # Workspace: file-based state, persisted to output_dir/workspace/
    workspace = Workspace(temp_dir=sandbox.temp_dir, output_dir=output_dir)

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
            data_profile, domain_rules = analyzer.analyze(manifest, traced_llm, sandbox)
            _log(trace, "analyzer", f"Profile: {len(data_profile)} chars, domain rules: {len(domain_rules)} chars")

        # Write to workspace for file-based access
        workspace.write_domain_rules(domain_rules)
        workspace.write_data_profile(data_profile)

        # Extract structured rules for long domain docs (> 50K chars)
        if len(domain_rules) > 50_000:
            with tracer.span("domain_extractor"):
                _log(trace, "domain_extractor", f"Extracting structured rules from {len(domain_rules)} chars")
                structured_rules = analyzer.extract_structured_rules(domain_rules, traced_llm)
                workspace._write("DOMAIN_RULES_STRUCTURED.md", structured_rules)
                _log(trace, "domain_extractor", f"Structured rules: {len(structured_rules)} chars")

        obs["analyzer"] = {
            "profile_length_chars": len(data_profile),
            "domain_rules_length_chars": len(domain_rules),
            "analysis_success": len(data_profile) > 50,
            "profile_quality": "rich" if len(data_profile) > 500 else ("sparse" if len(data_profile) > 50 else "failed"),
        }

        # 2b. Compute dynamic context budget
        complexity = estimate_complexity(question, manifest, bool(domain_rules))
        ctx_budget = compute_budget(complexity, bool(domain_rules))
        _log(trace, "budget", f"complexity={complexity}, total_chars={ctx_budget.total_chars}, compact_trigger={ctx_budget.compact_trigger_chars}")
        obs["budget"] = {
            "complexity": complexity,
            "total_chars": ctx_budget.total_chars,
            "compact_trigger_chars": ctx_budget.compact_trigger_chars,
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
        replans_used = 0
        max_replans = 1  # limit replanning to avoid infinite loops
        max_verifications_per_iter = 1  # limit verification rounds per iteration

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
                    state=state,
                    workspace=workspace,
                )
            _log(trace, "planner", f"Plan: {plan_step.step_description}")
            if plan_step.approach_detail:
                _log(trace, "planner", f"Approach: {plan_step.approach_detail[:300]}")
            iter_obs["plan_description"] = plan_step.step_description
            iter_obs["plan_sources"] = list(plan_step.data_sources)
            iter_obs["approach_detail"] = plan_step.approach_detail

            # Generate code
            with tracer.span("coder", metadata={"iteration": iteration}):
                _log(trace, "coder", "Generating code")
                code = coder.generate(
                    plan_step, manifest_json, steps_done, traced_llm,
                    state=state, workspace=workspace, manifest=manifest,
                )
            _log(trace, "coder", f"Code length: {len(code)} chars")

            # Pre-execution validation: check column references
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
            debug_exhausted = False  # True when all retries failed — trigger planner switch
            if result.return_code != 0:
                _log(trace, "debugger", f"Code failed: {result.stderr[:200]}")
                initial_error_type = debugger.classify_error(
                    debugger._parse_error(result.stderr)[0]
                )
                previous_attempts: list[tuple[str, str]] = []
                for retry in range(max_retries):
                    debug_retries += 1
                    with tracer.span("debugger", metadata={"retry": retry}):
                        fixed_code = debugger.fix(
                            code, result, manifest_json, data_profile, traced_llm,
                            state=state,
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

                # All retries exhausted and still failing — switch strategy
                if result.return_code != 0:
                    debug_exhausted = True
                    final_error = result.stderr[-800:].strip()
                    _log(trace, "debugger", f"Debug exhausted after {debug_retries} retries. Forcing replan via planner.")
                    workspace.append_lesson(
                        iteration,
                        f"Approach '{plan_step.step_description[:100]}' is unrecoverable "
                        f"({initial_error_type}): {final_error[:200]}. Try a completely different approach."
                    )

            # Write step to workspace (observability)
            workspace.write_step(iteration, code, result.stdout)

            iter_obs["code_success"] = result.return_code == 0
            iter_obs["debug_retries"] = debug_retries
            iter_obs["debug_exhausted"] = debug_exhausted
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

            # Extract lessons from successful debug fixes
            if debug_retries > 0 and result.return_code == 0:
                lesson = _extract_debug_lesson(step_record.result.stderr)
                if lesson:
                    workspace.append_lesson(iteration, lesson)

            # Debug exhausted: skip judge, force planner to switch approach
            if debug_exhausted:
                final_error = result.stderr[-400:].strip()
                judge_guidance = (
                    f"CRITICAL: The approach '{plan_step.step_description[:120]}' failed after "
                    f"{debug_retries} debug attempts and could not be fixed. "
                    f"Last error: {final_error[:300]}. "
                    f"You MUST try a completely different approach — different method, different library, "
                    f"or different data source."
                )
                state = update_judge_guidance(state, judge_guidance)
                workspace.write_judge_guidance(judge_guidance)
                obs["iterations"].append(iter_obs)
                stagnation_count = 0  # reset — new approach incoming
                continue  # skip judge, go straight to next planner iteration

            # Compact: summarize older steps when context grows too large
            ctx_size = workspace.estimate_context_size()
            if ctx_size > ctx_budget.compact_trigger_chars:
                _log(trace, "compact", f"Context size {ctx_size} exceeds trigger {ctx_budget.compact_trigger_chars}, compacting")
                workspace.compact(traced_llm, question, ctx_budget)

            # Judge: combined sufficiency check + routing + guidance (single LLM call)
            with tracer.span("judge", metadata={"iteration": iteration}):
                _log(trace, "judge", "Evaluating progress")
                verdict = judge.evaluate(question, steps_done, traced_llm, state=state)
            _log(trace, "judge", f"sufficient={verdict.sufficient}, action={verdict.action}, missing={verdict.missing}")

            # Handle "verify" action: run verification code, then re-evaluate
            if verdict.action == "verify" and verdict.verification_code:
                verifications_done = 0
                while (verdict.action == "verify"
                       and verdict.verification_code
                       and verifications_done < max_verifications_per_iter):
                    verifications_done += 1
                    _log(trace, "judge", f"Running verification code ({verifications_done})")
                    with tracer.span("verification", metadata={"iteration": iteration}):
                        v_result = sandbox.execute(
                            verdict.verification_code,
                            step_id=f"verify_{iteration}_{verifications_done}",
                        )
                    _log(trace, "judge", f"Verification rc={v_result.return_code}, stdout={v_result.stdout[:200]}")
                    iter_obs[f"verification_{verifications_done}_stdout"] = v_result.stdout[:500]
                    iter_obs[f"verification_{verifications_done}_stderr"] = v_result.stderr[:200] if v_result.return_code != 0 else ""

                    # Inject verification result into state for re-evaluation
                    verification_finding = f"Verification output: {v_result.stdout[:2000]}"
                    if v_result.return_code != 0:
                        verification_finding += f"\nVerification error: {v_result.stderr[:500]}"

                    # Create a synthetic step record with verification result
                    v_step = StepRecord(
                        plan=PlanStep(step_description=f"Judge verification (iteration {iteration})"),
                        code=verdict.verification_code,
                        result=v_result,
                        step_index=len(steps_done),
                    )
                    # Temporarily add to state for re-evaluation (don't persist)
                    temp_state = add_step(state, v_step, verification_finding)

                    # Re-evaluate with verification result
                    with tracer.span("judge_post_verify", metadata={"iteration": iteration}):
                        verdict = judge.evaluate(question, steps_done + [v_step], traced_llm, state=temp_state)
                    _log(trace, "judge", f"Post-verify: sufficient={verdict.sufficient}, action={verdict.action}")

            iter_obs["judge_sufficient"] = verdict.sufficient
            iter_obs["judge_action"] = verdict.action
            iter_obs["judge_reasoning"] = verdict.reasoning
            iter_obs["judge_missing"] = verdict.missing
            iter_obs["judge_guidance"] = verdict.guidance_for_next_step
            obs["iterations"].append(iter_obs)

            # Store guidance for next iteration's planner
            prev_guidance = judge_guidance
            judge_guidance = verdict.guidance_for_next_step

            # Record lesson when approach pivots significantly
            if (prev_guidance and judge_guidance
                    and _guidance_similarity(prev_guidance, judge_guidance) < 0.5):
                workspace.append_lesson(iteration, f"Approach changed: {judge_guidance[:150]}")

            # Handle "replan" action: re-run question_analyzer with accumulated findings
            if verdict.action == "replan" and replans_used < max_replans:
                replans_used += 1
                _log(trace, "judge", f"Triggering strategic replan (used={replans_used}/{max_replans})")
                with tracer.span("replan", metadata={"iteration": iteration}):
                    # Pass accumulated findings to question_analyzer for context
                    replan_context = workspace.read_progress()
                    if replan_context:
                        workspace.append_lesson(
                            iteration,
                            f"Replan triggered: {verdict.reasoning[:200]}"
                        )
                    analysis_plan = question_analyzer.analyze_question(
                        question, state.manifest_summary, workspace, traced_llm,
                    )
                _log(trace, "replan", f"New analysis plan: {len(analysis_plan)} chars")
                # Reset stagnation since we changed direction
                stagnation_count = 0
                judge_guidance = verdict.guidance_for_next_step or "Follow the new analysis plan."
                continue

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
                # Stagnation detection (word-level Jaccard similarity)
                step_failed = result.return_code != 0
                guidance_repeated = (
                    prev_guidance
                    and verdict.guidance_for_next_step
                    and _guidance_similarity(prev_guidance, verdict.guidance_for_next_step) > 0.7
                )
                if step_failed or guidance_repeated:
                    stagnation_count += 1
                    reason = "code_failed" if step_failed else "guidance_repeated"
                    _log(trace, "orchestrator", f"Stagnation signal: {reason} (count={stagnation_count})")
                else:
                    stagnation_count = 0

                if stagnation_count >= stagnation_threshold:
                    _log(trace, "orchestrator", f"Early stop: {stagnation_count} stagnation signals")
                    break

        # 6. Finalize
        with tracer.span("finalizer"):
            _log(trace, "finalizer", "Formatting answer")
            answer = finalizer.format_answer(question, steps_done, traced_llm, state=state, benchmark=benchmark, guidelines=guidelines)
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


def _guidance_similarity(a: str, b: str) -> float:
    """Word-level Jaccard similarity between two guidance strings."""
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    if not words_a or not words_b:
        return 0.0
    intersection = len(words_a & words_b)
    union = len(words_a | words_b)
    return intersection / union if union > 0 else 0.0


def _extract_debug_lesson(stderr: str) -> str:
    """Extract a specific lesson from a debug error, including actual names/values.

    The lesson must contain concrete details (column names, file names, actual
    values) so downstream agents can avoid the exact same mistake — not generic
    advice that the prompt already contains.
    """
    import re

    if not stderr:
        return ""

    # KeyError: extract the actual column name
    m = re.search(r"KeyError:\s*['\"]([^'\"]+)['\"]", stderr)
    if m:
        col = m.group(1)
        return f"Column '{col}' does not exist — check actual column names with df.columns"

    # FileNotFoundError: extract the file path
    m = re.search(r"FileNotFoundError:.*?['\"]([^'\"]+)['\"]", stderr)
    if m:
        path = m.group(1)
        return f"File '{path}' not found — use os.path.join(TASK_DIR, filename)"

    # UnicodeDecodeError: extract the encoding
    m = re.search(r"UnicodeDecodeError:\s*['\"](\w+)['\"]", stderr)
    if m:
        enc = m.group(1)
        return f"Encoding '{enc}' failed — use encoding='latin-1' as fallback"

    # ValueError with specific message
    m = re.search(r"ValueError:\s*(.{10,80})", stderr)
    if m:
        msg = m.group(1).strip()
        return f"ValueError: {msg}"

    # TypeError with specific message
    m = re.search(r"TypeError:\s*(.{10,80})", stderr)
    if m:
        msg = m.group(1).strip()
        return f"TypeError: {msg}"

    # sqlite3 errors: extract table/column name
    m = re.search(r"(?:no such table|no such column):\s*(\S+)", stderr)
    if m:
        name = m.group(1)
        return f"SQLite: '{name}' does not exist — check with PRAGMA table_info()"

    # Generic: take the last meaningful line of the traceback
    lines = [l.strip() for l in stderr.strip().splitlines() if l.strip()]
    if lines:
        last = lines[-1]
        if len(last) > 150:
            last = last[:150]
        return f"Error fixed: {last}"

    return ""


def _log(trace: list[dict], agent: str, message: str) -> None:
    trace.append({
        "timestamp": time.time(),
        "agent": agent,
        "message": message,
    })
