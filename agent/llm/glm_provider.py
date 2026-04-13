"""ZAI LLM provider — Anthropic-compatible endpoint for GLM-4.7."""

from __future__ import annotations

import os

from phi.model.anthropic import Claude

ZAI_BASE_URL = "https://api.z.ai/api/anthropic"
DEFAULT_MODEL = "claude-sonnet-4-5-20250929"  # Maps to GLM-4.7


def create_glm_model(
    model_id: str | None = None,
    temperature: float = 0.7,
    max_tokens: int = 2048,
) -> Claude:
    """
    Create ZAI LLM model via Anthropic-compatible API.

    Requires Doppler secrets:
        ANTHROPIC_API_KEY   — ZAI key
        ANTHROPIC_BASE_URL  — https://api.z.ai/api/anthropic
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set. Configure in Doppler.")

    # Phidata ignores base_url param — must set via env var
    os.environ.setdefault("ANTHROPIC_BASE_URL", ZAI_BASE_URL)

    return Claude(
        id=model_id or DEFAULT_MODEL,
        api_key=api_key,
        temperature=temperature,
        max_tokens=max_tokens,
    )
