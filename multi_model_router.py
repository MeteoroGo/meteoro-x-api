#!/usr/bin/env python3
"""
METEORO X v13 — MULTI-MODEL ROUTER + RATE LIMITER
Multi-Model Router | Dynamic Agent Routing | Per-Provider Semaphores | Retry + Exponential Backoff

ARCHITECTURE:
  Dynamic provider detection: Automatically detects available API keys at startup
  Ideal routing: Routes each agent to its optimal model based on specialization
  Graceful fallback: Routes to any available provider if primary is unavailable

SUPPORTED PROVIDERS (availability depends on configured API keys):
  - Anthropic models (synthesis, coordination, risk analysis)
  - DeepSeek V3 (quantitative analysis, deep reasoning)
  - Kimi/Moonshot (Asian markets, multi-language support)
  - Google Gemini (web search, real-time grounding)

RATE LIMITING:
  Each provider enforces request-level rate limits.
  Swarm orchestrates agent sequencing with inter-request delays.
  Fallback chains provide resilience when primary provider is unavailable.

Target cost per analysis cycle (agentic system): $0.03-0.08
"""

import os
import json
import time
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any
from enum import Enum

logger = logging.getLogger("meteoro.router")

try:
    import httpx
except ImportError:
    httpx = None

# ═══════════════════════════════════════════════════════════════
# API KEY VALIDATION
# Filters out placeholder values and keys that are too short
# to be real. Prevents wasted time on failed auth attempts.
# ═══════════════════════════════════════════════════════════════

_PLACEHOLDER_VALUES = {"PENDING", "NONE", "TODO", "XXX", "PLACEHOLDER", "CHANGEME", "YOUR_KEY_HERE", ""}

def _is_valid_api_key(key: str) -> bool:
    """Check if an API key is real (not a placeholder and long enough)."""
    if not key:
        return False
    cleaned = key.strip().upper()
    if cleaned in _PLACEHOLDER_VALUES:
        return False
    # Real API keys are at least 20 chars
    if len(key.strip()) < 20:
        return False
    return True

# ═══════════════════════════════════════════════════════════════
# RATE LIMITING — Per-provider semaphores + retry with backoff.
# Prevents concurrent calls to same provider (which triggers 429).
# Each provider gets at most 1 in-flight request at a time.
# On 429: exponential backoff retry (3s, 6s, 12s).
# ═══════════════════════════════════════════════════════════════

MAX_RETRIES = 3           # Retry per provider on 429
RETRY_BASE_DELAY = 3.0    # Base delay for exponential backoff (seconds)
INTER_PROVIDER_DELAY = 0.5  # Small delay between trying different providers

# Per-provider semaphores: serialize access to each provider
# This prevents 3 parallel agents from all hitting Anthropic at once
_provider_semaphores: Dict[str, asyncio.Semaphore] = {}

def _get_provider_semaphore(provider_value: str) -> asyncio.Semaphore:
    """Get or create a semaphore for a provider (1 concurrent call max)."""
    if provider_value not in _provider_semaphores:
        _provider_semaphores[provider_value] = asyncio.Semaphore(1)
    return _provider_semaphores[provider_value]


class ModelProvider(Enum):
    ANTHROPIC = "anthropic"
    DEEPSEEK = "deepseek"
    KIMI = "kimi"
    GEMINI = "gemini"


@dataclass
class ModelProfile:
    name: str
    model_id: str
    provider: ModelProvider
    base_url: str
    api_key_env: str
    cost_input_per_m: float
    cost_output_per_m: float
    max_tokens: int = 4096
    context_window: int = 128000
    specialization: List[str] = field(default_factory=list)
    supports_tools: bool = True
    timeout_s: float = 15.0


MODELS: Dict[str, ModelProfile] = {
    "claude-haiku": ModelProfile(
        name="Claude Haiku 4.5",
        model_id="claude-haiku-4-5-20251001",
        provider=ModelProvider.ANTHROPIC,
        base_url="https://api.anthropic.com/v1/messages",
        api_key_env="ANTHROPIC_API_KEY",
        cost_input_per_m=0.80,
        cost_output_per_m=4.00,
        specialization=["synthesis", "coordination", "risk", "spanish"],
        timeout_s=15.0,
    ),
    "deepseek-v3": ModelProfile(
        name="DeepSeek V3",
        model_id="deepseek-chat",
        provider=ModelProvider.DEEPSEEK,
        base_url="https://api.deepseek.com/v1/chat/completions",
        api_key_env="DEEPSEEK_API_KEY",
        cost_input_per_m=0.14,
        cost_output_per_m=0.28,
        specialization=["quantitative", "reasoning", "math", "backtesting"],
        supports_tools=False,
        timeout_s=10.0,
    ),
    "kimi-v1": ModelProfile(
        name="Kimi Moonshot v1",
        model_id="moonshot-v1-8k",
        provider=ModelProvider.KIMI,
        base_url="https://api.moonshot.cn/v1/chat/completions",
        api_key_env="KIMI_API_KEY",
        cost_input_per_m=0.60,
        cost_output_per_m=0.60,
        context_window=8192,
        specialization=["mandarin", "china", "asia", "demand"],
        supports_tools=False,
        timeout_s=10.0,
    ),
    "gemini-flash": ModelProfile(
        name="Gemini 2.0 Flash",
        model_id="gemini-2.0-flash",
        provider=ModelProvider.GEMINI,
        base_url="https://generativelanguage.googleapis.com/v1beta/models",
        api_key_env="GEMINI_API_KEY",
        cost_input_per_m=0.0,
        cost_output_per_m=0.0,
        specialization=["web_search", "grounding", "realtime", "news"],
        supports_tools=False,
        timeout_s=8.0,
    ),
}

# ═══════════════════════════════════════════════════════════════
# AGENT → MODEL ROUTING
# Ideal routing map: Each agent routed to its optimal model.
# Availability determined by configured API keys at runtime.
# Automatic fallback ensures graceful degradation if primary model unavailable.
# ═══════════════════════════════════════════════════════════════
AGENT_MODEL_MAP_IDEAL = {
    "satellite_recon":       "claude-haiku",
    "maritime_intel":        "claude-haiku",
    "supply_chain_mapper":   "deepseek-v3",
    "latam_osint":           "claude-haiku",
    "china_demand_oracle":   "kimi-v1",       # Kimi for China/Asia intelligence
    "geopolitical_risk":     "gemini-flash",
    "macro_regime":          "deepseek-v3",
    "quant_alpha":           "deepseek-v3",
    "sentiment_flow":        "kimi-v1",       # Kimi for Asian sentiment
    "risk_guardian":         "claude-haiku",
    "execution_engine":      "claude-haiku",
    "counterintelligence":   "deepseek-v3",
    "commander":             "claude-haiku",
}

FALLBACK_CHAIN: Dict[str, List[str]] = {
    "claude-haiku":  ["deepseek-v3", "gemini-flash", "kimi-v1"],
    "deepseek-v3":   ["claude-haiku", "gemini-flash", "kimi-v1"],
    "kimi-v1":       ["deepseek-v3", "claude-haiku", "gemini-flash"],
    "gemini-flash":  ["claude-haiku", "deepseek-v3", "kimi-v1"],
}


# ═══════════════════════════════════════════════════════════════
# PROVIDER DETECTION & DYNAMIC ROUTING
# ═══════════════════════════════════════════════════════════════

def _get_available_providers() -> set:
    """
    Detect which providers are available based on VALID API keys.
    Filters out placeholder values like "PENDING", "NONE", etc.

    Returns:
        set: Provider enum values for which REAL API keys are configured.
    """
    available = set()
    for model_key, profile in MODELS.items():
        api_key = os.getenv(profile.api_key_env, "").strip()
        if _is_valid_api_key(api_key):
            available.add(profile.provider.value)
            logger.info(f"[ROUTER] Provider {profile.provider.value} ACTIVE ({model_key})")
        else:
            if api_key:
                logger.warning(f"[ROUTER] {profile.api_key_env} has placeholder value — SKIPPED")
    return available


def _get_available_models() -> set:
    """
    Detect which models are available based on VALID API keys.
    Filters out placeholder values.

    Returns:
        set: Model keys for which REAL API keys are configured.
    """
    available = set()
    for model_key, profile in MODELS.items():
        api_key = os.getenv(profile.api_key_env, "").strip()
        if _is_valid_api_key(api_key):
            available.add(model_key)
    return available


def get_active_routing() -> Dict[str, str]:
    """
    Build active routing map based on available providers.

    For each agent, attempts to route to its ideal model.
    If ideal model is unavailable, falls back to any available model.
    If no models available, defaults to "gemini-flash" for graceful degradation.

    Returns:
        dict: Agent name -> model key mapping reflecting current availability.
    """
    available_models = _get_available_models()
    active_map = {}

    for agent_name, ideal_model in AGENT_MODEL_MAP_IDEAL.items():
        # Use ideal model if available
        if ideal_model in available_models:
            active_map[agent_name] = ideal_model
        else:
            # Fall back to first available model in fallback chain
            fallback_chain = FALLBACK_CHAIN.get(ideal_model, [])
            fallback_found = None
            for fallback_model in fallback_chain:
                if fallback_model in available_models:
                    fallback_found = fallback_model
                    break

            # Final fallback: use any available model (prefer gemini-flash for stability)
            if fallback_found:
                active_map[agent_name] = fallback_found
            elif "gemini-flash" in available_models:
                active_map[agent_name] = "gemini-flash"
            elif available_models:
                # Last resort: pick first available model
                active_map[agent_name] = next(iter(available_models))
            else:
                # No models available: default to gemini-flash (may fail gracefully)
                active_map[agent_name] = "gemini-flash"

    return active_map


@dataclass
class LLMResponse:
    content: str
    model_used: str
    provider: str
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    latency_ms: int = 0
    fallback_used: bool = False


class CostTracker:
    def __init__(self):
        self.total_cost = 0.0
        self.calls = 0
        self.by_model: Dict[str, float] = {}

    def record(self, model: str, cost: float):
        self.total_cost += cost
        self.calls += 1
        self.by_model[model] = self.by_model.get(model, 0.0) + cost

    def summary(self) -> dict:
        return {
            "total_cost_usd": round(self.total_cost, 6),
            "total_calls": self.calls,
            "by_model": {k: round(v, 6) for k, v in self.by_model.items()},
        }


cost_tracker = CostTracker()


async def _call_anthropic(profile: ModelProfile, system: str, user_msg: str) -> dict:
    api_key = os.getenv(profile.api_key_env, "")
    if not api_key:
        raise ValueError(f"Missing {profile.api_key_env}")
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": profile.model_id,
        "max_tokens": profile.max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user_msg}],
    }
    async with httpx.AsyncClient(timeout=profile.timeout_s) as client:
        r = await client.post(profile.base_url, json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
    return {
        "content": data.get("content", [{}])[0].get("text", ""),
        "input_tokens": data.get("usage", {}).get("input_tokens", 0),
        "output_tokens": data.get("usage", {}).get("output_tokens", 0),
    }


async def _call_openai_compat(profile: ModelProfile, system: str, user_msg: str) -> dict:
    api_key = os.getenv(profile.api_key_env, "")
    if not api_key:
        raise ValueError(f"Missing {profile.api_key_env}")
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": profile.model_id,
        "max_tokens": profile.max_tokens,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ],
    }
    async with httpx.AsyncClient(timeout=profile.timeout_s) as client:
        r = await client.post(profile.base_url, json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
    choice = data.get("choices", [{}])[0]
    usage = data.get("usage", {})
    return {
        "content": choice.get("message", {}).get("content", ""),
        "input_tokens": usage.get("prompt_tokens", 0),
        "output_tokens": usage.get("completion_tokens", 0),
    }


async def _call_gemini(profile: ModelProfile, system: str, user_msg: str) -> dict:
    api_key = os.getenv(profile.api_key_env, "")
    if not api_key:
        raise ValueError(f"Missing {profile.api_key_env}")
    url = f"{profile.base_url}/{profile.model_id}:generateContent?key={api_key}"
    payload = {
        "system_instruction": {"parts": [{"text": system}]},
        "contents": [{"parts": [{"text": user_msg}]}],
        "generationConfig": {"maxOutputTokens": profile.max_tokens},
    }
    async with httpx.AsyncClient(timeout=profile.timeout_s) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
    candidates = data.get("candidates", [{}])
    text = ""
    if candidates:
        parts = candidates[0].get("content", {}).get("parts", [])
        text = parts[0].get("text", "") if parts else ""
    usage = data.get("usageMetadata", {})
    return {
        "content": text,
        "input_tokens": usage.get("promptTokenCount", 0),
        "output_tokens": usage.get("candidatesTokenCount", 0),
    }


PROVIDER_CALLERS = {
    ModelProvider.ANTHROPIC: _call_anthropic,
    ModelProvider.DEEPSEEK: _call_openai_compat,
    ModelProvider.KIMI: _call_openai_compat,
    ModelProvider.GEMINI: _call_gemini,
}


async def call_llm(
    agent_name: str,
    system_prompt: str,
    user_message: str,
    model_override: Optional[str] = None,
) -> LLMResponse:
    """
    Call LLM with automatic routing, fallback chain, and error handling.

    Uses active routing map (which detects available providers) as the primary
    routing decision. Supports model override for testing or special cases.
    Falls back through provider chain if primary model is unavailable.

    Args:
        agent_name: Name of the calling agent
        system_prompt: System-level instructions for the model
        user_message: User message to process
        model_override: Optional model key to override routing (for testing)

    Returns:
        LLMResponse: Model response with usage metrics and fallback tracking
    """
    # Get active routing based on available providers
    active_routing = get_active_routing()
    primary_key = model_override or active_routing.get(agent_name, "gemini-flash")
    chain = [primary_key] + FALLBACK_CHAIN.get(primary_key, [])

    last_error = None
    for chain_idx, model_key in enumerate(chain):
        profile = MODELS.get(model_key)
        if not profile:
            continue
        api_key = os.getenv(profile.api_key_env, "").strip()
        if not _is_valid_api_key(api_key):
            continue
        caller = PROVIDER_CALLERS.get(profile.provider)
        if not caller:
            continue

        # Small delay between trying different providers in fallback chain
        if chain_idx > 0:
            await asyncio.sleep(INTER_PROVIDER_DELAY)

        # Acquire per-provider semaphore (serialize calls to same provider)
        sem = _get_provider_semaphore(profile.provider.value)

        for retry in range(MAX_RETRIES):
            async with sem:
                t0 = time.time()
                try:
                    result = await caller(profile, system_prompt, user_message)
                    latency = int((time.time() - t0) * 1000)
                    inp = result.get("input_tokens", 0)
                    out = result.get("output_tokens", 0)
                    cost = (inp * profile.cost_input_per_m + out * profile.cost_output_per_m) / 1_000_000
                    cost_tracker.record(model_key, cost)
                    return LLMResponse(
                        content=result["content"],
                        model_used=model_key,
                        provider=profile.provider.value,
                        input_tokens=inp,
                        output_tokens=out,
                        cost_usd=cost,
                        latency_ms=latency,
                        fallback_used=(model_key != primary_key),
                    )
                except Exception as e:
                    last_error = e
                    err_str = str(e)
                    is_429 = "429" in err_str or "rate" in err_str.lower()

                    if is_429 and retry < MAX_RETRIES - 1:
                        wait = RETRY_BASE_DELAY * (2 ** retry)  # 3s, 6s, 12s
                        logger.info(f"[{agent_name}] {model_key} rate-limited — retry {retry+1}/{MAX_RETRIES} in {wait:.0f}s")
                        await asyncio.sleep(wait)
                        continue  # Retry same provider
                    else:
                        logger.warning(f"[{agent_name}] {model_key} failed: {err_str[:80]}")
                        break  # Move to next provider in chain

    return LLMResponse(
        content=json.dumps({
            "signal": "HOLD", "confidence": 0,
            "reasoning": f"All LLM providers unavailable: {last_error}",
            "sources_analyzed": 0,
        }),
        model_used="none", provider="fallback", latency_ms=0,
    )


def get_model_for_agent(agent_name: str) -> str:
    """
    Get the active model assignment for an agent based on current provider availability.

    Args:
        agent_name: Name of the agent

    Returns:
        str: Model key (e.g., "claude-haiku", "deepseek-v3") currently assigned to this agent
    """
    active_routing = get_active_routing()
    return active_routing.get(agent_name, "gemini-flash")

def get_cost_summary() -> dict:
    return cost_tracker.summary()
