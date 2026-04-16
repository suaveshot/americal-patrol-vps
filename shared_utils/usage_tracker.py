"""
Anthropic API Usage Tracker

Drop-in wrapper for anthropic.Anthropic().messages.create() that logs
token usage per call to JSONL files. Every pipeline uses this instead
of calling the Anthropic SDK directly.

Usage:
    from shared_utils.usage_tracker import tracked_create

    response = tracked_create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        messages=[{"role": "user", "content": "Hello"}],
        pipeline="blog",
        client_id="garcia",
    )
    # response is the normal Anthropic Message object — unchanged
"""

import json
import os
from datetime import datetime
from pathlib import Path

import anthropic

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
USAGE_LOGS_DIR = _PROJECT_ROOT / "usage_logs"

# Pricing per 1M tokens (USD). Update when Anthropic changes pricing.
MODEL_PRICING = {
    "claude-sonnet-4-20250514": {"input": 3.00, "output": 15.00},
    "claude-haiku-4-5-20251001": {"input": 0.80, "output": 4.00},
    "claude-opus-4-20250514": {"input": 15.00, "output": 75.00},
}

# Fallback for unknown models
_DEFAULT_PRICING = {"input": 3.00, "output": 15.00}


def _estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculate estimated cost in USD."""
    pricing = MODEL_PRICING.get(model, _DEFAULT_PRICING)
    cost = (input_tokens * pricing["input"] / 1_000_000) + \
           (output_tokens * pricing["output"] / 1_000_000)
    return round(cost, 6)


def _log_usage(
    pipeline: str,
    client_id: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    est_cost: float,
):
    """Append a usage record to the client's monthly JSONL log."""
    USAGE_LOGS_DIR.mkdir(exist_ok=True)

    month = datetime.now().strftime("%Y%m")
    log_file = USAGE_LOGS_DIR / f"{client_id}_{month}.jsonl"

    record = {
        "ts": datetime.now().isoformat(),
        "pipeline": pipeline,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "est_cost_usd": est_cost,
    }

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")


def tracked_create(
    *,
    model: str,
    max_tokens: int,
    messages: list,
    pipeline: str,
    client_id: str,
    api_key: str | None = None,
    system: str | None = None,
    **kwargs,
) -> anthropic.types.Message:
    """
    Call anthropic.Anthropic().messages.create() and log token usage.

    Accepts all the same parameters as messages.create(), plus:
        pipeline:  Pipeline ID (e.g., "blog", "email", "sales")
        client_id: Client identifier from tenant_config.json

    Returns the Anthropic Message response unchanged.
    """
    client_kwargs = {}
    if api_key:
        client_kwargs["api_key"] = api_key

    client = anthropic.Anthropic(**client_kwargs)

    create_kwargs = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": messages,
        **kwargs,
    }
    if system is not None:
        create_kwargs["system"] = system

    response = client.messages.create(**create_kwargs)

    # Extract usage from response
    input_tokens = response.usage.input_tokens
    output_tokens = response.usage.output_tokens
    est_cost = _estimate_cost(model, input_tokens, output_tokens)

    _log_usage(pipeline, client_id, model, input_tokens, output_tokens, est_cost)

    return response
