#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  METEORO X v14 — 6-PROVIDER INTELLIGENCE BACKBONE               ║
║  Multi-Model Router | Cost-Optimized | Per-Provider Semaphores  ║
║  Retry + Exponential Backoff | Provider Health Cache            ║
║                                                                  ║
║  6 PROVIDERS:                                                    ║
║    1. Anthropic Claude Haiku 4.5  — synthesis, risk, Spanish    ║
║    2. DeepSeek V3                 — quant, reasoning ($0.28/M)  ║
║    3. Kimi K2 (Moonshot)          — Asia, multi-language        ║
║    4. Google Gemini 2.5 Flash     — real-time, news, grounding  ║
║    5. OpenAI GPT-4o mini          — general analysis ($0.15/M)  ║
║    6. Groq (Llama 3.3 70B)       — ultra-fast, FREE tier       ║
║                                                                  ║
║  COST-OPTIMIZED ROUTING:                                        ║
║    Primary: cheapest provider per agent specialization           ║
║    Fallback: 5-deep chain ensures no single point of failure    ║
║    Target: $0.02-0.05 per analysis cycle                        ║
║                                                                  ║
║  RATE LIMITING:                                                  ║
║    Per-provider asyncio.Semaphore (1 concurrent max)            ║
║    Exponential backoff on 429 (3s, 6s, 12s)                     ║
║    Provider health cache — auto-disable on auth/credit failure  ║
╚══════════════════════════════════════════════════════════════════╝
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
# ═══════════════════════════════════════════════════════════════

_PLACEHOLDER_VALUES = {"PENDING", "NONE", "TODO", "XXX", "PLACEHOLDER", "CHANGEME", "YOUR_KEY_HERE", ""}

def _is_valid_api_key(key: str) -> bool:
    """Check if an API key is real (not a placeholder and long enough)."""
    if not key:
        return False
    cleaned = key.strip().upper()
    if cleaned in _PLACEHOLDER_VALUES:
        return False
    if len(key.strip()) < 20:
        return False
    return True

# ═══════════════════════════════════════════════════════════════
# RATE LIMITING — Per-provider semaphores + retry with backoff.
# Each provider gets at most 1 in-flight request at a time.
# On 429: exponential backoff retry (3s, 6s, 12s).
# On auth/credit failure: disable provider for 5-min cooldown.
# ═══════════════════════════════════════════════════════════════

MAX_RETRIES = 3
RETRY_BASE_DELAY = 3.0        # 3s, 6s, 12s backoff — gives Groq time to reset
INTER_PROVIDER_DELAY = 0.3
PROVIDER_COOLDOWN_S = 300

_provider_semaphores: Dict[str, asyncio.Semaphore] = {}
_provider_failures: Dict[str, float] = {}

# Per-provider concurrency limits (based on rate limits):
# Groq: 30 RPM → can handle 3 concurrent calls safely
# Gemini: 10 RPM free tier → 2 concurrent calls
# Others: 1 concurrent call (conservative)
PROVIDER_CONCURRENCY = {
    "groq": 2,       # Free tier: 30 RPM — 2 concurrent is safe
    "gemini": 2,     # Free tier: 10 RPM
    "deepseek": 2,
    "openai": 2,
    "kimi": 1,
    "anthropic": 1,
}

def _get_provider_semaphore(provider_value: str) -> asyncio.Semaphore:
    """Get or create a semaphore for a provider (concurrency based on rate limits)."""
    if provider_value not in _provider_semaphores:
        limit = PROVIDER_CONCURRENCY.get(provider_value, 1)
        _provider_semaphores[provider_value] = asyncio.Semaphore(limit)
    return _provider_semaphores[provider_value]

def _is_provider_healthy(provider_value: str) -> bool:
    """Check if provider is healthy (not in cooldown from auth/credit failure)."""
    fail_time = _provider_failures.get(provider_value)
    if fail_time is None:
        return True
    if time.time() - fail_time > PROVIDER_COOLDOWN_S:
        del _provider_failures[provider_value]
        logger.info(f"[ROUTER] Provider {provider_value} cooldown expired — re-enabling")
        return True
    return False

def _mark_provider_failed(provider_value: str, reason: str):
    """Mark a provider as failed — skip for cooldown period."""
    _provider_failures[provider_value] = time.time()
    logger.warning(f"[ROUTER] Provider {provider_value} DISABLED for {PROVIDER_COOLDOWN_S}s: {reason}")


# ═══════════════════════════════════════════════════════════════
# 6 PROVIDERS
# ═══════════════════════════════════════════════════════════════

class ModelProvider(Enum):
    ANTHROPIC = "anthropic"
    DEEPSEEK = "deepseek"
    KIMI = "kimi"
    GEMINI = "gemini"
    OPENAI = "openai"
    GROQ = "groq"


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


# ═══════════════════════════════════════════════════════════════
# MODEL REGISTRY — 6 providers, cost-optimized
# Ordered by cost (cheapest first for reference):
#   Groq Llama 3.3 70B:   FREE (500K tok/day)
#   DeepSeek V3:           $0.28/$0.42 per M tokens
#   GPT-4o mini:           $0.15/$0.60 per M tokens
#   Gemini 2.5 Flash:      $0.30/$2.50 per M tokens (free tier: 10 RPM)
#   Kimi K2:               $0.60/$2.50 per M tokens
#   Claude Haiku 4.5:      $1.00/$5.00 per M tokens
# ═══════════════════════════════════════════════════════════════

MODELS: Dict[str, ModelProfile] = {
    # ── TIER 1: ULTRA-LOW COST ─────────────────────────────────
    "groq-llama": ModelProfile(
        name="Groq Llama 3.3 70B",
        model_id="llama-3.3-70b-versatile",
        provider=ModelProvider.GROQ,
        base_url="https://api.groq.com/openai/v1/chat/completions",
        api_key_env="GROQ_API_KEY",
        cost_input_per_m=0.06,      # Effectively free on free tier
        cost_output_per_m=0.06,
        specialization=["speed", "volume", "sentiment", "general"],
        supports_tools=False,
        timeout_s=10.0,
    ),
    "deepseek-v3": ModelProfile(
        name="DeepSeek V3",
        model_id="deepseek-chat",
        provider=ModelProvider.DEEPSEEK,
        base_url="https://api.deepseek.com/v1/chat/completions",
        api_key_env="DEEPSEEK_API_KEY",
        cost_input_per_m=0.28,
        cost_output_per_m=0.42,
        specialization=["quantitative", "reasoning", "math", "backtesting"],
        supports_tools=False,
        timeout_s=10.0,
    ),
    "gpt4o-mini": ModelProfile(
        name="GPT-4o mini",
        model_id="gpt-4o-mini",
        provider=ModelProvider.OPENAI,
        base_url="https://api.openai.com/v1/chat/completions",
        api_key_env="OPENAI_API_KEY",
        cost_input_per_m=0.15,
        cost_output_per_m=0.60,
        specialization=["general", "analysis", "structured_output", "geopolitical"],
        supports_tools=True,
        timeout_s=12.0,
    ),

    # ── TIER 2: MID-COST, SPECIALIZED ─────────────────────────
    "gemini-flash": ModelProfile(
        name="Gemini 2.5 Flash",
        model_id="gemini-2.5-flash",
        provider=ModelProvider.GEMINI,
        base_url="https://generativelanguage.googleapis.com/v1beta/models",
        api_key_env="GEMINI_API_KEY",
        cost_input_per_m=0.15,      # Free tier: 10 RPM, 500 RPD
        cost_output_per_m=0.60,
        specialization=["web_search", "grounding", "realtime", "news", "multimodal"],
        supports_tools=False,
        timeout_s=10.0,
    ),
    "kimi-k2": ModelProfile(
        name="Kimi K2",
        model_id="kimi-k2-0711",
        provider=ModelProvider.KIMI,
        base_url="https://api.moonshot.ai/v1/chat/completions",
        api_key_env="KIMI_API_KEY",
        cost_input_per_m=0.60,
        cost_output_per_m=2.50,
        context_window=262144,
        specialization=["mandarin", "china", "asia", "demand", "multimodal"],
        supports_tools=False,
        timeout_s=12.0,
    ),

    # ── TIER 3: PREMIUM ────────────────────────────────────────
    "claude-haiku": ModelProfile(
        name="Claude Haiku 4.5",
        model_id="claude-haiku-4-5-20251001",
        provider=ModelProvider.ANTHROPIC,
        base_url="https://api.anthropic.com/v1/messages",
        api_key_env="ANTHROPIC_API_KEY",
        cost_input_per_m=1.00,
        cost_output_per_m=5.00,
        specialization=["synthesis", "coordination", "risk", "spanish", "structured"],
        timeout_s=10.0,
    ),
}


# ═══════════════════════════════════════════════════════════════
# AGENT → MODEL ROUTING (Cost-Optimized)
# Strategy: Cheapest capable provider as primary.
# Premium models (Claude) reserved for synthesis/risk only.
# Each agent has 5-deep fallback chain.
# ═══════════════════════════════════════════════════════════════

AGENT_MODEL_MAP_IDEAL = {
    # Physical Intelligence — Groq (free, fast) + DeepSeek fallback
    "satellite_recon":       "groq-llama",
    "maritime_intel":        "groq-llama",
    "supply_chain_mapper":   "deepseek-v3",

    # Regional Intelligence — GPT-4o mini (cheap, good at geopolitics)
    "latam_osint":           "gpt4o-mini",
    "china_demand_oracle":   "kimi-k2",           # Kimi for China/Asia
    "geopolitical_risk":     "gpt4o-mini",

    # Quantitative — DeepSeek (best at math/quant, ultra-cheap)
    "macro_regime":          "deepseek-v3",
    "quant_alpha":           "deepseek-v3",
    "sentiment_flow":        "groq-llama",         # Fast sentiment

    # Critical — Claude Haiku (premium, but only 3 calls)
    "risk_guardian":         "claude-haiku",        # Risk needs best model
    "execution_engine":      "gemini-flash",        # Gemini for real-time
    "counterintelligence":   "gpt4o-mini",          # GPT for adversarial

    # Commander (synthesis) — Claude
    "commander":             "claude-haiku",
}

# 5-deep fallback chains — prioritized by cost
FALLBACK_CHAIN: Dict[str, List[str]] = {
    "groq-llama":    ["deepseek-v3", "gpt4o-mini", "gemini-flash", "kimi-k2", "claude-haiku"],
    "deepseek-v3":   ["groq-llama", "gpt4o-mini", "gemini-flash", "kimi-k2", "claude-haiku"],
    "gpt4o-mini":    ["deepseek-v3", "groq-llama", "gemini-flash", "kimi-k2", "claude-haiku"],
    "gemini-flash":  ["groq-llama", "deepseek-v3", "gpt4o-mini", "kimi-k2", "claude-haiku"],
    "kimi-k2":       ["deepseek-v3", "groq-llama", "gpt4o-mini", "gemini-flash", "claude-haiku"],
    "claude-haiku":  ["deepseek-v3", "gpt4o-mini", "groq-llama", "gemini-flash", "kimi-k2"],
}


# ═══════════════════════════════════════════════════════════════
# PROVIDER DETECTION & DYNAMIC ROUTING
# ═══════════════════════════════════════════════════════════════

def _get_available_providers() -> set:
    """Detect which providers have valid API keys configured."""
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
    """Detect which models have valid API keys configured."""
    available = set()
    for model_key, profile in MODELS.items():
        api_key = os.getenv(profile.api_key_env, "").strip()
        if _is_valid_api_key(api_key):
            available.add(model_key)
    return available


def get_active_routing() -> Dict[str, str]:
    """
    Build active routing map based on available providers.
    Falls back through chain if primary model unavailable.
    """
    available_models = _get_available_models()
    active_map = {}

    for agent_name, ideal_model in AGENT_MODEL_MAP_IDEAL.items():
        if ideal_model in available_models:
            active_map[agent_name] = ideal_model
        else:
            fallback_chain = FALLBACK_CHAIN.get(ideal_model, [])
            fallback_found = None
            for fallback_model in fallback_chain:
                if fallback_model in available_models:
                    fallback_found = fallback_model
                    break

            if fallback_found:
                active_map[agent_name] = fallback_found
            elif available_models:
                active_map[agent_name] = next(iter(available_models))
            else:
                active_map[agent_name] = "groq-llama"  # Default to free provider

    return active_map


def get_provider_status() -> Dict[str, Any]:
    """Return detailed provider status for diagnostics."""
    status = {}
    for model_key, profile in MODELS.items():
        api_key = os.getenv(profile.api_key_env, "").strip()
        has_key = _is_valid_api_key(api_key)
        healthy = _is_provider_healthy(profile.provider.value)
        status[model_key] = {
            "provider": profile.provider.value,
            "name": profile.name,
            "key_configured": has_key,
            "healthy": healthy,
            "cost_per_m_input": profile.cost_input_per_m,
            "cost_per_m_output": profile.cost_output_per_m,
            "specialization": profile.specialization,
        }
    return status


# ═══════════════════════════════════════════════════════════════
# RESPONSE + COST TRACKING
# ═══════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════
# PROVIDER CALLERS
# ═══════════════════════════════════════════════════════════════

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
        # Check for credit/auth errors in response body BEFORE raise_for_status
        if r.status_code >= 400:
            try:
                err_body = r.json()
                err_msg = err_body.get("error", {}).get("message", "")
                if any(kw in err_msg.lower() for kw in ["credit balance", "billing", "insufficient"]):
                    raise ValueError(f"Anthropic credit balance error: {err_msg}")
            except (json.JSONDecodeError, ValueError) as ve:
                if "credit" in str(ve).lower():
                    raise
        r.raise_for_status()
        data = r.json()
    return {
        "content": data.get("content", [{}])[0].get("text", ""),
        "input_tokens": data.get("usage", {}).get("input_tokens", 0),
        "output_tokens": data.get("usage", {}).get("output_tokens", 0),
    }


async def _call_openai_compat(profile: ModelProfile, system: str, user_msg: str) -> dict:
    """OpenAI-compatible caller — works for OpenAI, DeepSeek, Kimi, and Groq."""
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
    ModelProvider.OPENAI: _call_openai_compat,
    ModelProvider.GROQ: _call_openai_compat,
}


# ═══════════════════════════════════════════════════════════════
# MAIN LLM CALL — with rate limiting + fallback + health cache
# ═══════════════════════════════════════════════════════════════

async def call_llm(
    agent_name: str,
    system_prompt: str,
    user_message: str,
    model_override: Optional[str] = None,
) -> LLMResponse:
    """
    Call LLM with automatic routing, 5-deep fallback chain, rate limiting,
    and provider health caching. Routes to cheapest available provider first.
    """
    active_routing = get_active_routing()
    primary_key = model_override or active_routing.get(agent_name, "groq-llama")
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

        # Skip providers in cooldown (auth/credit failures)
        if not _is_provider_healthy(profile.provider.value):
            logger.debug(f"[{agent_name}] Skipping {model_key} — provider in cooldown")
            continue

        # Small delay between trying different providers
        if chain_idx > 0:
            await asyncio.sleep(INTER_PROVIDER_DELAY)

        # Acquire per-provider semaphore
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
                    err_str = str(e).lower()

                    # Auth/credit failures — disable provider
                    is_auth_fail = any(kw in err_str for kw in [
                        "credit balance", "credit", "insufficient", "billing",
                        "authentication", "invalid api key", "unauthorized",
                        "invalid_api_key", "permission",
                        "401", "402", "403",
                    ])
                    if is_auth_fail:
                        _mark_provider_failed(profile.provider.value, str(e)[:100])
                        break

                    # Rate limit — DON'T retry same provider, move to next in chain
                    # This is critical: retrying 429 wastes time; the next provider
                    # in the fallback chain is likely available immediately.
                    is_429 = "429" in err_str or "rate" in err_str or "too many" in err_str
                    if is_429:
                        logger.info(f"[{agent_name}] {model_key} rate-limited — skipping to next provider")
                        break  # Move to next provider in fallback chain
                    else:
                        logger.warning(f"[{agent_name}] {model_key} failed: {str(e)[:80]}")
                        break

    return LLMResponse(
        content=json.dumps({
            "signal": "HOLD", "confidence": 0,
            "reasoning": f"All LLM providers unavailable: {last_error}",
            "sources_analyzed": 0,
        }),
        model_used="none", provider="fallback", latency_ms=0,
    )


def get_model_for_agent(agent_name: str) -> str:
    active_routing = get_active_routing()
    return active_routing.get(agent_name, "groq-llama")

def get_cost_summary() -> dict:
    return cost_tracker.summary()


# ═══════════════════════════════════════════════════════════════
# PROVIDER WARMUP — Test each provider at startup
# Runs a tiny "say hi" call to each configured provider.
# Marks failed providers in health cache immediately so the
# first real analysis doesn't waste time on dead providers.
# ═══════════════════════════════════════════════════════════════

_warmup_done = False

async def warmup_providers():
    """
    Test each configured provider with a minimal LLM call.
    Marks failing providers (auth errors, credit issues) in health cache.
    Called once during FastAPI startup.
    """
    global _warmup_done
    if _warmup_done:
        return
    _warmup_done = True

    logger.info("[WARMUP] ═══ Testing all configured providers... ═══")
    results = {}

    for model_key, profile in MODELS.items():
        api_key = os.getenv(profile.api_key_env, "").strip()
        if not _is_valid_api_key(api_key):
            results[model_key] = "NO_KEY"
            continue

        caller = PROVIDER_CALLERS.get(profile.provider)
        if not caller:
            results[model_key] = "NO_CALLER"
            continue

        try:
            # Minimal test call — short prompt, tiny response
            result = await asyncio.wait_for(
                caller(profile, "You are a test.", "Reply with only: OK"),
                timeout=8.0,
            )
            content = result.get("content", "").strip()
            if content:
                results[model_key] = "OK"
                logger.info(f"[WARMUP] {model_key} ({profile.provider.value}): ✅ ALIVE")
            else:
                results[model_key] = "EMPTY_RESPONSE"
                logger.warning(f"[WARMUP] {model_key}: empty response")
        except Exception as e:
            err_str = str(e).lower()
            is_auth = any(kw in err_str for kw in [
                "credit balance", "insufficient", "billing",
                "authentication", "invalid api key", "unauthorized",
                "401", "402", "403",
            ])
            if is_auth:
                _mark_provider_failed(profile.provider.value, f"warmup: {str(e)[:80]}")
                results[model_key] = f"AUTH_FAIL: {str(e)[:60]}"
            else:
                # Timeout or transient error — don't permanently disable, but log
                results[model_key] = f"ERROR: {str(e)[:60]}"
            logger.warning(f"[WARMUP] {model_key}: ❌ {results[model_key]}")

    working = sum(1 for v in results.values() if v == "OK")
    logger.info(f"[WARMUP] ═══ Results: {working}/{len(MODELS)} providers ALIVE ═══")
    for k, v in results.items():
        logger.info(f"[WARMUP]   {k:15s}: {v}")

    return results
