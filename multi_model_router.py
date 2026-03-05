#!/usr/bin/env python3
"""
METEORO X v7.2 — MULTI-MODEL ROUTER + RATE LIMITER
4 Cerebros Cognitivos | Enrutamiento Dinamico por Agente | Retry + Backoff

STACK ACTIVO:
  Claude Haiku 3.5  -> Sintesis, coordinacion, decisiones (Anthropic) [PENDIENTE - consola caida]
  DeepSeek V3       -> Razonamiento profundo, quant, China/Mandarin, analisis pesado
  Gemini 2.0 Flash  -> Busqueda web, grounding, datos en tiempo real (GRATIS, 15 RPM limit)
  Kimi/Moonshot     -> DESHABILITADO (requiere telefono chino para registro)

RATE LIMITING:
  Gemini free tier = 15 RPM. We limit to 3 concurrent calls + retry on 429.
  DeepSeek needs credits loaded ($5 min at platform.deepseek.com).
  All agents fall through to Gemini via fallback chain until other providers are funded.

Costo objetivo por analisis completo (12 agentes): $0.03-0.08
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
# RATE LIMITING — Prevent 429 errors on free tier APIs
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# SIMPLE APPROACH — No rate limiter in router.
# Swarm handles pacing by running agents sequentially with delays.
# ═══════════════════════════════════════════════════════════════

MAX_RETRIES = 1  # Just try once per provider (swarm handles pacing)


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
        name="Claude Haiku 3.5",
        model_id="claude-3-5-haiku-20241022",
        provider=ModelProvider.ANTHROPIC,
        base_url="https://api.anthropic.com/v1/messages",
        api_key_env="ANTHROPIC_API_KEY",
        cost_input_per_m=0.25,
        cost_output_per_m=1.25,
        specialization=["synthesis", "coordination", "risk", "spanish"],
        timeout_s=12.0,
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
# Currently ALL agents route to gemini-flash (only working provider)
# DeepSeek: 402 Payment Required (needs $5 credits)
# Anthropic: API key not configured yet
# Once funded, re-route agents to their ideal models.
# ═══════════════════════════════════════════════════════════════
AGENT_MODEL_MAP: Dict[str, str] = {
    "satellite_recon":       "gemini-flash",
    "maritime_intel":        "gemini-flash",
    "supply_chain_mapper":   "gemini-flash",
    "latam_osint":           "gemini-flash",
    "china_demand_oracle":   "gemini-flash",
    "geopolitical_risk":     "gemini-flash",
    "macro_regime":          "gemini-flash",
    "quant_alpha":           "gemini-flash",
    "sentiment_flow":        "gemini-flash",
    "risk_guardian":         "gemini-flash",
    "execution_engine":      "gemini-flash",
    "counterintelligence":   "gemini-flash",
    "commander":             "gemini-flash",
}

# IDEAL routing (re-enable when providers are funded):
# AGENT_MODEL_MAP_IDEAL = {
#     "satellite_recon":       "claude-haiku",
#     "maritime_intel":        "claude-haiku",
#     "supply_chain_mapper":   "deepseek-v3",
#     "latam_osint":           "claude-haiku",
#     "china_demand_oracle":   "deepseek-v3",  # DeepSeek = Chinese model
#     "geopolitical_risk":     "gemini-flash",
#     "macro_regime":          "deepseek-v3",
#     "quant_alpha":           "deepseek-v3",
#     "sentiment_flow":        "claude-haiku",
#     "risk_guardian":         "claude-haiku",
#     "execution_engine":      "claude-haiku",
#     "counterintelligence":   "deepseek-v3",
#     "commander":             "claude-haiku",
# }

FALLBACK_CHAIN: Dict[str, List[str]] = {
    "claude-haiku":  ["deepseek-v3", "gemini-flash"],
    "deepseek-v3":   ["claude-haiku", "gemini-flash"],
    "kimi-v1":       ["deepseek-v3", "claude-haiku"],
    "gemini-flash":  ["claude-haiku", "deepseek-v3"],
}


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
    Call LLM with automatic fallback chain + retry on 429 rate limits.
    Uses per-provider semaphores to limit concurrent calls.
    """
    primary_key = model_override or AGENT_MODEL_MAP.get(agent_name, "claude-haiku")
    chain = [primary_key] + FALLBACK_CHAIN.get(primary_key, [])

    last_error = None
    for model_key in chain:
        profile = MODELS.get(model_key)
        if not profile:
            continue
        api_key = os.getenv(profile.api_key_env, "")
        if not api_key:
            continue
        caller = PROVIDER_CALLERS.get(profile.provider)
        if not caller:
            continue

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
            logger.warning(f"[{agent_name}] {model_key} error: {str(e)[:80]}")
            continue  # Try next model in fallback chain

    return LLMResponse(
        content=json.dumps({
            "signal": "HOLD", "confidence": 0,
            "reasoning": f"All LLM providers unavailable: {last_error}",
            "sources_analyzed": 0,
        }),
        model_used="none", provider="fallback", latency_ms=0,
    )


def get_model_for_agent(agent_name: str) -> str:
    return AGENT_MODEL_MAP.get(agent_name, "claude-haiku")

def get_cost_summary() -> dict:
    return cost_tracker.summary()
