"""Immutable data types for dataline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# --- Profiler types ---

@dataclass(frozen=True)
class ManifestEntry:
    """Single file's metadata from profiler."""
    file_path: str
    file_type: str  # csv | sqlite | json | markdown | pdf | docx | excel | image | parquet
    size_bytes: int
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CrossSourceRelation:
    """Auto-discovered relation between two data sources."""
    source_a: str
    source_b: str
    relation: str
    confidence: float


@dataclass(frozen=True)
class Manifest:
    """Complete profiling result for a task directory."""
    entries: tuple[ManifestEntry, ...]
    cross_source_relations: tuple[CrossSourceRelation, ...] = ()
    keyword_tags: tuple[str, ...] = ()


# --- Sandbox types ---

@dataclass(frozen=True)
class SandboxResult:
    """Result from executing code in sandbox."""
    stdout: str
    stderr: str
    return_code: int
    execution_time_ms: int
    step_id: str = ""


# --- Agent types ---

@dataclass(frozen=True)
class PlanStep:
    """A single planned step from the planner."""
    step_description: str
    data_sources: tuple[str, ...] = ()
    depends_on_prior: bool = False
    expected_output: str = ""
    approach_detail: str = ""  # analytical blueprint: columns, filters, formulas, edge cases


@dataclass(frozen=True)
class StepRecord:
    """Record of a completed step (plan + code + result)."""
    plan: PlanStep
    code: str
    result: SandboxResult
    step_index: int = 0


@dataclass(frozen=True)
class VerifierVerdict:
    """Verifier's judgment on sufficiency."""
    sufficient: bool
    reasoning: str = ""
    missing: str = ""


@dataclass(frozen=True)
class RouterDecision:
    """Router's decision on next action."""
    action: str  # continue | backtrack | finish
    truncate_to: int = 0
    reasoning: str = ""


@dataclass(frozen=True)
class JudgeDecision:
    """Judge's combined verdict: sufficiency + routing + guidance.

    Replaces separate VerifierVerdict + RouterDecision with a single LLM call.
    """
    sufficient: bool
    action: str  # continue | backtrack | finish | verify | replan
    reasoning: str = ""
    missing: str = ""
    guidance_for_next_step: str = ""  # passed to Planner to steer next iteration
    truncate_to: int = 0
    verification_code: str = ""  # Python code for independent verification (action="verify")


@dataclass(frozen=True)
class LLMUsage:
    """Token and cost tracking for a single LLM call."""
    input_tokens: int
    output_tokens: int
    cost_usd: float
    latency_ms: int
    provider: str = ""
    model: str = ""


# --- Eval types ---

@dataclass(frozen=True)
class TaskScore:
    """Eval result for a single task."""
    task_id: str
    score: int  # 0 or 1 for KDD
    difficulty: str = ""
    failure_category: str = ""  # code_error | wrong_direction | format_error | ...
    failed_at_agent: str = ""   # which agent failed
    error_type: str = ""
    error_detail: str = ""
    tokens_used: int = 0
    cost_usd: float = 0.0
    time_seconds: float = 0.0
    steps_executed: int = 0
    max_steps: int = 13
    suggestion: str = ""


# --- Memory / State types ---

@dataclass(frozen=True)
class AnalysisState:
    """Structured state passed between agents. Replaces raw list[StepRecord].

    Information layers (by priority):
    - Layer 1 (task definition): question, manifest_summary — never changes
    - Layer 2 (domain knowledge): domain_rules — extracted from manual/README, independent channel
    - Layer 3 (data understanding): data_profile_summary — column stats, distributions
    - Layer 4 (execution state): key_findings, completed_steps, variables — updated each step
    - Layer 5 (control signal): judge_guidance — steering from judge to planner
    - Layer 6 (raw detail): full_step_details — complete step records, disk-backed via pickles
    """
    task_id: str
    question: str
    manifest_summary: str                           # column names + types (compressed schema)
    data_profile_summary: str                       # analyzer output: stats, distributions
    domain_rules: str = ""                          # extracted from manual/README/knowledge files
    key_findings: tuple[str, ...] = ()              # 1-line verified findings
    variables_in_scope: tuple[tuple[str, str], ...] = ()  # (pickle_name, description)
    judge_guidance: str = ""                        # steering instruction from judge for next iteration
    completed_steps: tuple[str, ...] = ()           # 1-line per step
    full_step_details: tuple[StepRecord, ...] = ()  # raw data for Finalizer + Judge


@dataclass(frozen=True)
class EvalReport:
    """Aggregate evaluation report with diagnostics."""
    overall_accuracy: float
    per_difficulty: dict[str, float] = field(default_factory=dict)
    task_scores: tuple[TaskScore, ...] = ()
    failure_breakdown: dict[str, int] = field(default_factory=dict)
    agent_bottlenecks: dict[str, int] = field(default_factory=dict)
    total_tokens: int = 0
    total_cost_usd: float = 0.0
    suggestions: tuple[str, ...] = ()


@dataclass(frozen=True)
class CompareReport:
    """Comparison between two eval runs."""
    accuracy_delta: float
    improved_tasks: tuple[str, ...] = ()
    regressed_tasks: tuple[str, ...] = ()
    per_difficulty_delta: dict[str, float] = field(default_factory=dict)
