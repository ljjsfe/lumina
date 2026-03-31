"""LLM client wrapper that automatically logs I/O to the active tracer span.

This is a thin decorator around LLMClient. Instead of modifying LLMClient
(which would couple it to tracing), we wrap it so that every `chat()` call
automatically records the full prompt and response on the current span.

Usage in orchestrator:
    tracer = TaskTracer(task_id, output_dir)
    traced_llm = TracingLLMClient(llm, tracer)
    # Now pass traced_llm to agents instead of llm
"""

from __future__ import annotations

from .llm_client import LLMClient
from .tracer import TaskTracer
from .types import LLMUsage


class TracingLLMClient:
    """Wraps LLMClient to auto-log LLM I/O to the active tracer span."""

    def __init__(self, inner: LLMClient, tracer: TaskTracer):
        self._inner = inner
        self._tracer = tracer

    def chat(self, system: str, user: str) -> str:
        """Single-turn chat with automatic tracing."""
        response, usage = self.chat_with_usage(system, user)
        return response

    def chat_with_usage(self, system: str, user: str) -> tuple[str, LLMUsage]:
        """Chat and return (response_text, usage_info) with tracing."""
        response, usage = self._inner.chat_with_usage(system, user)

        # Record on current span if one is active
        current_span = self._tracer._current_span
        if current_span is not None:
            current_span.set_llm_io(
                system_prompt=system,
                user_prompt=user,
                response=response,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cost_usd=usage.cost_usd,
            )

        return response, usage

    @property
    def total_usage(self) -> dict:
        return self._inner.total_usage

    # Forward any other attribute access to inner client
    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)
