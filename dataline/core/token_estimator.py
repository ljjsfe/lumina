"""Lightweight token estimation using tiktoken.

Uses cl100k_base encoding (GPT-4 / most BPE-based LLMs) with a configurable
safety margin to account for tokenizer differences across providers.

Industry standard approach — same as LiteLLM and LangChain.
"""

from __future__ import annotations

import tiktoken

# Module-level singleton — encoding is thread-safe and reusable.
_ENCODING = tiktoken.get_encoding("cl100k_base")

# Default safety margin: tiktoken underestimates Moonshot/Kimi by ~8%.
# 1.15x covers this gap with room to spare.
_DEFAULT_SAFETY_MARGIN = 1.15


# Safety cap for individual text fields (e.g., stdout) before token estimation.
# Prevents pathological output from blowing up context.
_STDOUT_SAFETY_CAP = 100_000


def cap_text(text: str, max_chars: int = _STDOUT_SAFETY_CAP) -> str:
    """Apply safety cap to prevent pathological output from blowing up context."""
    if len(text) > max_chars:
        return text[:max_chars] + f"\n... (truncated at {max_chars} chars)"
    return text


def estimate_tokens(text: str, safety_margin: float = _DEFAULT_SAFETY_MARGIN) -> int:
    """Estimate token count for a text string.

    Args:
        text: The text to estimate.
        safety_margin: Multiplier to account for tokenizer differences.
            1.15 means "assume 15% more tokens than tiktoken reports."

    Returns:
        Estimated token count (always ≥ 0).
    """
    if not text:
        return 0
    return int(len(_ENCODING.encode(text)) * safety_margin)
