"""Ollama LLM provider — supports Ollama Cloud and local instances."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from phi.model.base import Model


def create_ollama_model(
    model_name: str | None = None,
    temperature: float = 0.7,
    max_tokens: int = 2048,
) -> Model:
    """
    Create Ollama model for Phidata agent.

    Supports:
    - Ollama Cloud: https://ollama.com (set OLLAMA_BASE_URL)
    - Local Ollama: http://localhost:11434 (default)

    Environment variables:
        OLLAMA_MODEL      — model name (default: llama3.2)
        OLLAMA_BASE_URL   — Ollama endpoint (default: http://localhost:11434)
        OLLAMA_API_KEY   — API key for Ollama Cloud
    """
    from phi.model.ollama import Ollama

    model = model_name or os.getenv("OLLAMA_MODEL", "llama3.2")
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    api_key = os.getenv("OLLAMA_API_KEY", None)

    options = {
        "temperature": temperature,
        "num_predict": max_tokens,
    }
    request_params = {"api_key": api_key} if api_key else None

    return Ollama(
        id=model,
        host=base_url,
        options=options,
        request_params=request_params,
    )
