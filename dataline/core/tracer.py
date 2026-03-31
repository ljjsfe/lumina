"""Structured tracing with optional Arize Phoenix integration.

Two purposes:
1. **Real-time progress**: writes status.json to output dir for live monitoring
2. **Post-hoc analysis**: exports OTEL-compatible spans (to Phoenix or JSON)

Phoenix is an optional dependency. Without it, traces are saved as JSON.
With it, spans are automatically pushed to Phoenix UI (localhost:6006).

Usage in orchestrator:
    tracer = TaskTracer(task_id, output_dir)
    with tracer.span("planner", metadata={"iteration": 0}):
        result = planner.plan_next(...)
    tracer.finish()
"""

from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Generator

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Span:
    """A single traced operation (agent call, LLM call, etc.)."""
    name: str
    agent: str
    start_time: float
    end_time: float
    duration_ms: int
    metadata: dict[str, Any] = field(default_factory=dict)
    # LLM I/O (only for LLM call spans)
    llm_input: str = ""           # system + user prompt (truncated)
    llm_output: str = ""          # model response (truncated)
    llm_thinking: str = ""        # extended thinking (Claude only)
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    error: str = ""


@dataclass
class SpanBuilder:
    """Mutable builder for constructing a Span during execution."""
    name: str
    agent: str
    start_time: float
    metadata: dict[str, Any] = field(default_factory=dict)
    llm_input: str = ""
    llm_output: str = ""
    llm_thinking: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    error: str = ""

    def set_llm_io(
        self,
        system_prompt: str,
        user_prompt: str,
        response: str,
        thinking: str = "",
        input_tokens: int = 0,
        output_tokens: int = 0,
        cost_usd: float = 0.0,
    ) -> None:
        """Record LLM input/output on this span."""
        max_prompt = 8000
        max_response = 4000
        self.llm_input = _truncate(f"[SYSTEM]\n{system_prompt}\n\n[USER]\n{user_prompt}", max_prompt)
        self.llm_output = _truncate(response, max_response)
        self.llm_thinking = _truncate(thinking, 2000) if thinking else ""
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.cost_usd = cost_usd

    def build(self) -> Span:
        """Finalize into an immutable Span."""
        end_time = time.time()
        return Span(
            name=self.name,
            agent=self.agent,
            start_time=self.start_time,
            end_time=end_time,
            duration_ms=int((end_time - self.start_time) * 1000),
            metadata=dict(self.metadata),
            llm_input=self.llm_input,
            llm_output=self.llm_output,
            llm_thinking=self.llm_thinking,
            input_tokens=self.input_tokens,
            output_tokens=self.output_tokens,
            cost_usd=self.cost_usd,
            error=self.error,
        )


class TaskTracer:
    """Traces a single task execution with real-time progress updates."""

    def __init__(self, task_id: str, output_dir: str = ""):
        self._task_id = task_id
        self._output_dir = output_dir
        self._spans: list[Span] = []
        self._start_time = time.time()
        self._current_span: SpanBuilder | None = None
        self._phoenix_tracer = _try_init_phoenix()

        # Progress state
        self._progress = Progress(
            task_id=task_id,
            status="running",
            current_agent="",
            current_iteration=0,
            total_iterations=0,
            steps_completed=0,
            tokens_used=0,
            cost_usd=0.0,
            elapsed_seconds=0.0,
        )

    @contextmanager
    def span(
        self,
        agent: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> Generator[SpanBuilder, None, None]:
        """Context manager that traces an agent operation.

        Usage:
            with tracer.span("planner", metadata={"iteration": 0}) as s:
                result = planner.plan_next(...)
                s.set_llm_io(system, user, response)
        """
        builder = SpanBuilder(
            name=f"{self._task_id}/{agent}",
            agent=agent,
            start_time=time.time(),
            metadata=metadata or {},
        )
        self._current_span = builder

        # Update progress
        self._progress = Progress(
            task_id=self._task_id,
            status="running",
            current_agent=agent,
            current_iteration=self._progress.current_iteration,
            total_iterations=self._progress.total_iterations,
            steps_completed=self._progress.steps_completed,
            tokens_used=self._progress.tokens_used,
            cost_usd=self._progress.cost_usd,
            elapsed_seconds=time.time() - self._start_time,
            message=f"Running {agent}...",
        )
        self._write_progress()

        try:
            yield builder
        except Exception as exc:
            builder.error = str(exc)
            raise
        finally:
            span = builder.build()
            self._spans.append(span)
            self._current_span = None

            # Accumulate token usage
            self._progress = Progress(
                task_id=self._task_id,
                status="running",
                current_agent=agent,
                current_iteration=self._progress.current_iteration,
                total_iterations=self._progress.total_iterations,
                steps_completed=self._progress.steps_completed,
                tokens_used=self._progress.tokens_used + span.input_tokens + span.output_tokens,
                cost_usd=self._progress.cost_usd + span.cost_usd,
                elapsed_seconds=time.time() - self._start_time,
                message=f"{agent} done ({span.duration_ms}ms)",
            )
            self._write_progress()

            # Push to Phoenix if available
            if self._phoenix_tracer:
                _push_span_to_phoenix(self._phoenix_tracer, span)

    def set_iteration(self, iteration: int, total: int) -> None:
        """Update current iteration progress."""
        self._progress = Progress(
            task_id=self._task_id,
            status="running",
            current_agent=self._progress.current_agent,
            current_iteration=iteration,
            total_iterations=total,
            steps_completed=iteration,
            tokens_used=self._progress.tokens_used,
            cost_usd=self._progress.cost_usd,
            elapsed_seconds=time.time() - self._start_time,
            message=f"Iteration {iteration}/{total}",
        )
        self._write_progress()

    def finish(self, success: bool = True, error: str = "") -> None:
        """Mark task as complete and write final trace."""
        self._progress = Progress(
            task_id=self._task_id,
            status="completed" if success else "failed",
            current_agent="",
            current_iteration=self._progress.current_iteration,
            total_iterations=self._progress.total_iterations,
            steps_completed=self._progress.steps_completed,
            tokens_used=self._progress.tokens_used,
            cost_usd=self._progress.cost_usd,
            elapsed_seconds=time.time() - self._start_time,
            message="Done" if success else f"Failed: {error}",
        )
        self._write_progress()
        self._write_trace()

    def _write_progress(self) -> None:
        """Write real-time progress to status.json in output dir."""
        if not self._output_dir:
            return
        try:
            os.makedirs(self._output_dir, exist_ok=True)
            path = os.path.join(self._output_dir, "status.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self._progress.to_dict(), f, indent=2)
        except OSError as exc:
            logger.debug("Could not write progress: %s", exc)

    def _write_trace(self) -> None:
        """Write full trace with LLM I/O to trace_detailed.json."""
        if not self._output_dir:
            return
        try:
            path = os.path.join(self._output_dir, "trace_detailed.json")
            data = {
                "task_id": self._task_id,
                "total_spans": len(self._spans),
                "total_duration_ms": int((time.time() - self._start_time) * 1000),
                "total_tokens": self._progress.tokens_used,
                "total_cost_usd": round(self._progress.cost_usd, 4),
                "spans": [_span_to_dict(s) for s in self._spans],
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except OSError as exc:
            logger.debug("Could not write trace: %s", exc)

    @property
    def spans(self) -> tuple[Span, ...]:
        return tuple(self._spans)

    @property
    def progress(self) -> "Progress":
        return self._progress


@dataclass(frozen=True)
class Progress:
    """Real-time task execution progress."""
    task_id: str
    status: str  # running | completed | failed
    current_agent: str
    current_iteration: int
    total_iterations: int
    steps_completed: int
    tokens_used: int
    cost_usd: float
    elapsed_seconds: float
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "current_agent": self.current_agent,
            "current_iteration": self.current_iteration,
            "total_iterations": self.total_iterations,
            "steps_completed": self.steps_completed,
            "tokens_used": self.tokens_used,
            "cost_usd": round(self.cost_usd, 4),
            "elapsed_seconds": round(self.elapsed_seconds, 1),
            "message": self.message,
        }


# --- Phoenix integration (optional) ---


def _try_init_phoenix() -> Any | None:
    """Try to initialize Phoenix tracer. Returns None if not installed."""
    try:
        import os as _os

        from opentelemetry import trace as otel_trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from openinference.semconv.resource import ResourceAttributes

        project_name = _os.environ.get("PHOENIX_PROJECT", "dataline")

        resource = Resource.create({
            ResourceAttributes.PROJECT_NAME: project_name,
            "service.name": project_name,
        })
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint="http://localhost:6006/v1/traces")
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        otel_trace.set_tracer_provider(provider)

        logger.info("Phoenix tracing enabled (http://localhost:6006) project=%s", project_name)
        return otel_trace.get_tracer("dataline")
    except ImportError:
        logger.debug("Phoenix not installed — traces saved to JSON only")
        return None
    except Exception as exc:
        logger.debug("Phoenix init failed: %s — traces saved to JSON only", exc)
        return None


def _push_span_to_phoenix(tracer: Any, span: Span) -> None:
    """Push a completed span to Phoenix via OTEL."""
    try:
        with tracer.start_as_current_span(span.name) as otel_span:
            otel_span.set_attribute("agent", span.agent)
            otel_span.set_attribute("duration_ms", span.duration_ms)
            otel_span.set_attribute("input_tokens", span.input_tokens)
            otel_span.set_attribute("output_tokens", span.output_tokens)
            otel_span.set_attribute("cost_usd", span.cost_usd)
            if span.llm_input:
                otel_span.set_attribute("llm.input", span.llm_input)
            if span.llm_output:
                otel_span.set_attribute("llm.output", span.llm_output)
            if span.llm_thinking:
                otel_span.set_attribute("llm.thinking", span.llm_thinking)
            if span.error:
                otel_span.set_attribute("error", span.error)
            for k, v in span.metadata.items():
                otel_span.set_attribute(f"meta.{k}", str(v))
    except Exception as exc:
        logger.debug("Failed to push span to Phoenix: %s", exc)


# --- Helpers ---


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len] + f"\n... [truncated, {len(text)} total chars]"


def _span_to_dict(span: Span) -> dict[str, Any]:
    """Convert Span to JSON-serializable dict."""
    d: dict[str, Any] = {
        "name": span.name,
        "agent": span.agent,
        "duration_ms": span.duration_ms,
        "metadata": span.metadata,
    }
    if span.input_tokens > 0:
        d["input_tokens"] = span.input_tokens
        d["output_tokens"] = span.output_tokens
        d["cost_usd"] = round(span.cost_usd, 6)
    if span.llm_input:
        d["llm_input"] = span.llm_input
    if span.llm_output:
        d["llm_output"] = span.llm_output
    if span.llm_thinking:
        d["llm_thinking"] = span.llm_thinking
    if span.error:
        d["error"] = span.error
    return d
