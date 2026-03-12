"""
METEORO X v14 — 6-Provider Proactive Intelligence Platform
===========================================================
FastAPI server: 6 LLM Providers | Proactive Monitoring | Memory | Knowledge Graph

Endpoints:
  POST /api/analyze         - Full agentic system analysis
  POST /api/swarm/analyze   - Direct swarm endpoint
  GET  /api/health          - Server heartbeat + capabilities
  GET  /api/macro           - Current macro snapshot
  GET  /api/signals         - Recent signals history
  GET  /api/swarm/config    - Swarm configuration info
  GET  /api/diagnostics     - Provider status
  === KNOWLEDGE GRAPH (10 entity types + correspondents) ===
  GET  /api/knowledge/exchanges    - Global commodity exchanges
  GET  /api/knowledge/traders      - Major commodity traders
  GET  /api/knowledge/mines        - Major mines worldwide
  GET  /api/knowledge/plants       - Refineries & smelters
  GET  /api/knowledge/ports        - Major ports & terminals
  GET  /api/knowledge/shipping     - Shipping companies (navieras)
  GET  /api/knowledge/logistics    - Rail, road, pipeline operators
  GET  /api/knowledge/qa           - Inspection & QA companies
  GET  /api/knowledge/clients      - End consumers / buyers
  GET  /api/knowledge/correspondents - Autonomous correspondent network
  GET  /api/knowledge/stats        - Knowledge graph statistics
  GET  /api/knowledge/{commodity}  - Full industry context for a commodity
  === AUTONOMOUS MEMORY (learning + calibration) ===
  GET  /api/memory/dashboard          - Performance dashboard
  GET  /api/memory/track-record       - Accuracy statistics
  GET  /api/memory/leaderboard        - Agent performance ranking
  GET  /api/memory/calibration/{c}    - Auto-calibration weights
  GET  /api/memory/context/{c}        - Historical memory context
  POST /api/memory/update-outcomes    - Trigger outcome evaluation
  ===
  GET  /api/ping            - Lightweight keep-alive endpoint
  WS   /ws/analyze          - Real-time streaming analysis
"""

import asyncio
import json
import time
import os
import sys
import traceback
from datetime import datetime, timezone
from dataclasses import asdict
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

import math

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _sanitize_nan(obj):
    """Replace NaN/Inf with None recursively — required for valid JSON."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize_nan(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_nan(v) for v in obj]
    return obj

# Import old pipeline as fallback
try:
    from meteoro_pipeline import analyze as legacy_analyze, classify_command, EvidencePack
    HAS_LEGACY = True
except Exception:
    HAS_LEGACY = False

# Import new swarm
try:
    from meteoro_swarm import MeteorSwarm
    HAS_SWARM = True
except Exception as e:
    HAS_SWARM = False
    print(f"[WARN] Swarm import failed: {e}")

# Data sources
try:
    from data_sources.fred_api import get_macro_snapshot
    HAS_MACRO = True
except Exception:
    HAS_MACRO = False

try:
    from latency_benchmark import LatencyBenchmark
    HAS_BENCHMARK = True
except Exception:
    HAS_BENCHMARK = False

# Industry Knowledge Graph
try:
    from data_sources.industry_knowledge import (
        get_all_exchanges_summary, get_all_traders_summary,
        get_all_mines_summary, get_all_plants_summary,
        get_all_ports_summary, get_all_shipping_summary,
        get_all_logistics_summary, get_all_qa_summary,
        get_all_clients_summary, get_knowledge_graph_stats,
        get_commodity_context, build_agent_context_prompt,
        EXCHANGES, MAJOR_TRADERS, MAJOR_MINES, SUPPLY_CHAINS,
        MAJOR_PORTS, SHIPPING_COMPANIES, LOGISTICS_COMPANIES,
        INSPECTION_QA, END_CLIENTS,
    )
    HAS_KNOWLEDGE = True
except Exception as e:
    HAS_KNOWLEDGE = False
    print(f"[WARN] Industry knowledge import failed: {e}")

# Autonomous Correspondents Network
try:
    from data_sources.correspondents import (
        get_all_correspondents_summary,
        get_correspondents_for_commodity,
        build_correspondent_prompt,
        CORRESPONDENTS,
    )
    HAS_CORRESPONDENTS = True
except Exception as e:
    HAS_CORRESPONDENTS = False
    print(f"[WARN] Correspondents import failed: {e}")

# Autonomous Memory System
try:
    from memory.autonomous_memory import AutonomousMemory
    HAS_MEMORY = True
except Exception as e:
    HAS_MEMORY = False
    print(f"[WARN] Autonomous memory import failed: {e}")

# Proactive Monitor Engine
try:
    from proactive_monitor import ProactiveMonitor
    HAS_PROACTIVE = True
except Exception as e:
    HAS_PROACTIVE = False
    print(f"[WARN] Proactive monitor import failed: {e}")

# Provider status (v14)
try:
    from multi_model_router import get_provider_status
    HAS_PROVIDER_STATUS = True
except Exception:
    HAS_PROVIDER_STATUS = False

# ═══════════════════════════════════════════════════════════════
# APP SETUP
# ═══════════════════════════════════════════════════════════════

app = FastAPI(
    title="Meteoro X — Autonomous Intelligence",
    description="AI-Native Autonomous Commodity Intelligence | Agentic System | Industry Knowledge Graph",
    version="14.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory stores
signal_history = []
swarm_instance = None
benchmark = None
memory_instance = None

# Initialize autonomous memory
if HAS_MEMORY:
    try:
        memory_instance = AutonomousMemory()
        print("[MEMORY] Autonomous memory system ready")
    except Exception as me:
        print(f"[WARN] Memory initialization failed: {me}")

if HAS_BENCHMARK:
    benchmark = LatencyBenchmark(db_path=os.environ.get("BENCHMARK_DB", "/tmp/latency_benchmark.db"))


def get_swarm():
    global swarm_instance
    if swarm_instance is None and HAS_SWARM:
        swarm_instance = MeteorSwarm()
    return swarm_instance


# ═══════════════════════════════════════════════════════════════
# PROACTIVE ENGINE STARTUP
# Auto-start the proactive monitoring engine when the server boots.
# This makes the system autonomous — it doesn't wait for commands.
# ═══════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup_proactive_engine():
    """Auto-start proactive intelligence engine on server boot."""
    # TEMPORARILY DISABLED: Proactive engine consumes Groq free tier rate limit
    # (30 RPM). With only 2 active providers, the proactive engine eats all
    # the tokens before user-triggered analyses can run.
    # Re-enable once we have 3+ paid providers with higher rate limits.
    print("[PROACTIVE] ═══ Engine PAUSED — rate limit conservation mode ═══")
    print("[PROACTIVE] Will re-enable with 3+ paid providers")


# ═══════════════════════════════════════════════════════════════
# COMMODITY CLASSIFICATION
# ═══════════════════════════════════════════════════════════════

COMMODITY_KEYWORDS = {
    # Oil & Energy
    "oil": "OIL", "petroleo": "OIL", "petróleo": "OIL", "crude": "OIL",
    "wti": "OIL", "brent": "OIL", "crudo": "OIL", "opec": "OIL",
    "gas": "NATURAL_GAS", "natural gas": "NATURAL_GAS", "gas natural": "NATURAL_GAS",
    "lng": "NATURAL_GAS",
    # Precious Metals
    "gold": "GOLD", "oro": "GOLD", "bullion": "GOLD",
    "silver": "SILVER", "plata": "SILVER",
    # Base Metals
    "copper": "COPPER", "cobre": "COPPER", "escondida": "COPPER",
    "las bambas": "COPPER", "antamina": "COPPER", "codelco": "COPPER",
    "coal": "COAL", "carbon": "COAL", "carbón": "COAL", "cerrejon": "COAL",
    "cerrejón": "COAL", "newcastle": "COAL",
    "nickel": "NICKEL", "niquel": "NICKEL", "níquel": "NICKEL", "norilsk": "NICKEL",
    "lithium": "LITHIUM", "litio": "LITHIUM", "sqm": "LITHIUM",
    "iron": "IRON", "hierro": "IRON", "vale": "IRON", "pilbara": "IRON",
    # Agriculture
    "coffee": "COFFEE", "cafe": "COFFEE", "café": "COFFEE", "arabica": "COFFEE",
    "wheat": "WHEAT", "trigo": "WHEAT",
    "corn": "CORN", "maiz": "CORN", "maíz": "CORN",
    "soy": "SOY", "soja": "SOY", "soybean": "SOY",
    "sugar": "SUGAR", "azucar": "SUGAR", "azúcar": "SUGAR",
    # Strategic / EV Metals
    "cobalt": "COBALT", "cobalto": "COBALT",
    "platinum": "PLATINUM", "platino": "PLATINUM",
    "palladium": "PALLADIUM", "paladio": "PALLADIUM",
    "uranium": "URANIUM", "uranio": "URANIUM",
    "tin": "TIN", "estaño": "TIN",
    "zinc": "ZINC",
    "aluminum": "ALUMINUM", "aluminio": "ALUMINUM", "aluminium": "ALUMINUM",
    # Agriculture — LatAm extended
    "cacao": "COCOA", "cocoa": "COCOA", "chocolate": "COCOA",
    "cotton": "COTTON", "algodon": "COTTON", "algodón": "COTTON",
    "rice": "RICE", "arroz": "RICE",
    "beef": "CATTLE", "cattle": "CATTLE", "ganado": "CATTLE", "carne": "CATTLE",
    "palm oil": "PALM_OIL", "aceite de palma": "PALM_OIL", "palma": "PALM_OIL",
    "rubber": "RUBBER", "caucho": "RUBBER",
    # Shipping
    "baltic": "SHIPPING", "freight": "SHIPPING", "flete": "SHIPPING",
    # General market queries
    "mercados": "GENERAL", "markets": "GENERAL", "commodities": "GENERAL",
}


def detect_commodity(command: str) -> str:
    lower = command.lower()
    for keyword, commodity in COMMODITY_KEYWORDS.items():
        if keyword in lower:
            return commodity
    return "GENERAL"


def _signal_to_direction(signal_action: str) -> str:
    """Map swarm signal (BUY/SELL/HOLD) to trading direction (LONG/SHORT/HOLD)."""
    m = {"BUY": "LONG", "SELL": "SHORT"}
    return m.get(signal_action.upper(), "HOLD")


# Capability display names — maps internal agent names to clean labels
# Keys must match substrings in agent_name (case-insensitive)
CAPABILITY_NAMES = {
    "Satellite": {"en": "Satellite Intelligence", "es": "Inteligencia Satelital"},
    "Maritime": {"en": "Maritime Tracking", "es": "Rastreo Marítimo"},
    "Supply Chain": {"en": "Supply Chain Analysis", "es": "Cadena de Suministro"},
    "LatAm": {"en": "Regional Intelligence", "es": "Inteligencia Regional"},
    "China Demand": {"en": "Demand Analysis", "es": "Análisis de Demanda"},
    "Geopolitical": {"en": "Geopolitical Risk", "es": "Riesgo Geopolítico"},
    "Macro Regime": {"en": "Macro Analysis", "es": "Análisis Macro"},
    "Quant": {"en": "Quantitative Analysis", "es": "Análisis Cuantitativo"},
    "Sentiment": {"en": "Market Sentiment", "es": "Sentimiento de Mercado"},
    "Risk Guardian": {"en": "Risk Management", "es": "Gestión de Riesgo"},
    "Execution": {"en": "Execution Strategy", "es": "Estrategia de Ejecución"},
    "Counterintelligence": {"en": "Validation", "es": "Validación"},
}


def _get_capability_name(agent_name: str, lang: str = "es") -> str:
    """Map internal agent name to a clean capability display name."""
    for key, names in CAPABILITY_NAMES.items():
        if key.lower() in agent_name.lower():
            return names.get(lang, names.get("en", agent_name))
    return agent_name


def _build_narrative(result, commodity: str, language: str = "es") -> dict:
    """
    Build a rich intelligence narrative from swarm results.
    Synthesizes agent findings into a compelling human-readable brief.
    Supports ES and EN locales.
    """
    bullish = result.agents_bullish
    bearish = result.agents_bearish
    neutral = result.agents_neutral
    total = bullish + bearish + neutral
    direction = _signal_to_direction(result.final_signal.value)
    conviction = result.conviction
    lang = language.lower()[:2] if language else "es"

    # Collect key findings from agents (filter out errors and empty reasoning)
    findings = []
    for r in result.all_results:
        if r.error or not r.reasoning or 'unavailable' in r.reasoning.lower():
            continue
        if r.confidence >= 40:
            findings.append({
                "agent": _get_capability_name(r.agent_name, lang),
                "signal": r.signal.value,
                "confidence": r.confidence,
                "finding": r.reasoning[:300],
                "key": r.evidence_pack.get("key_finding", ""),
            })

    # Sort by confidence descending
    findings.sort(key=lambda x: x["confidence"], reverse=True)

    # Build what_happened (locale-aware, dynamic based on actual agents)
    active_agents = sum(1 for r in result.all_results if not r.error and r.confidence > 0)
    active_names = [
        _get_capability_name(r.agent_name, lang)
        for r in result.all_results
        if not r.error and r.confidence > 0
    ]
    # Show up to 5 capability names, then summarize the rest
    if len(active_names) > 5:
        shown = ", ".join(active_names[:5])
        extra = len(active_names) - 5
        if lang == "es":
            cap_list = f"{shown} y {extra} más"
        else:
            cap_list = f"{shown} and {extra} more"
    else:
        cap_list = ", ".join(active_names)

    if lang == "es":
        what_happened = (
            f"Sistema de inteligencia autónoma desplegó {active_agents} capacidades especializadas "
            f"({cap_list}) para analizar {commodity} en tiempo real."
        )
    else:
        what_happened = (
            f"Autonomous intelligence system deployed {active_agents} specialized capabilities "
            f"({cap_list}) to analyze {commodity} in real-time."
        )

    # Build why_it_matters from top findings
    if findings:
        top = findings[:3]
        points = [f.get("key") or f["finding"][:120] for f in top]
        why_parts = [p for p in points if p and len(p) > 10]
        why_it_matters = " | ".join(why_parts) if why_parts else result.reasoning
    else:
        why_it_matters = result.reasoning

    # Build Spanish summary header for why_it_matters
    if lang == "es" and findings:
        n_sources = len(findings)
        signal_word = "alcista" if direction == "LONG" else ("bajista" if direction == "SHORT" else "neutral")
        if conviction >= 70:
            es_header = f"Análisis de {n_sources} fuentes indica tendencia {signal_word} con alta convicción."
        elif conviction >= 50:
            es_header = f"Análisis de {n_sources} fuentes sugiere tendencia {signal_word} con convicción moderada."
        else:
            es_header = f"Análisis de {n_sources} fuentes muestra señales mixtas con sesgo {signal_word}."
        why_it_matters = f"{es_header} {why_it_matters}"

    # Build consensus description (locale-aware)
    if lang == "es":
        if conviction >= 80:
            strength = "Alta convicción"
        elif conviction >= 60:
            strength = "Convicción moderada"
        else:
            strength = "Baja convicción"
        consensus_text = (
            f"{strength} — señal {direction}: {bullish} alcistas, {bearish} bajistas, "
            f"{neutral} neutrales de {total} fuentes de inteligencia."
        )
    else:
        if conviction >= 80:
            strength = "High-conviction"
        elif conviction >= 60:
            strength = "Moderate-conviction"
        else:
            strength = "Low-conviction"
        consensus_text = (
            f"{strength} {direction} signal: {bullish} bullish, {bearish} bearish, "
            f"{neutral} neutral out of {total} intelligence streams."
        )

    return {
        "what_happened": what_happened,
        "why_it_matters": why_it_matters,
        "consensus": consensus_text,
        "top_findings": findings[:5],
        "direction": direction,
    }


# ═══════════════════════════════════════════════════════════════
# INTELLIGENCE BRIEF — Structured by strategic dimensions
# ═══════════════════════════════════════════════════════════════

# Map agent names → strategic dimensions
DIMENSION_MAP = {
    "Satellite": "supply",
    "Maritime": "supply",
    "Supply Chain": "supply",
    "LatAm": "demand",
    "China Demand": "demand",
    "Geopolitical": "geopolitical",
    "Macro Regime": "macro",
    "Quant": "technical",
    "Sentiment": "sentiment",
    "Risk Guardian": "risk",
    "Execution": "execution",
    "Counterintelligence": "validation",
}

DIMENSION_LABELS = {
    "supply": {"es": "Oferta & Logística", "en": "Supply & Logistics"},
    "demand": {"es": "Demanda Global", "en": "Global Demand"},
    "macro": {"es": "Régimen Macro", "en": "Macro Regime"},
    "technical": {"es": "Análisis Técnico", "en": "Technical Analysis"},
    "geopolitical": {"es": "Riesgo Geopolítico", "en": "Geopolitical Risk"},
    "sentiment": {"es": "Sentimiento & Flujos", "en": "Sentiment & Flows"},
}


def _get_agent_dimension(agent_name: str) -> str:
    """Map agent name to strategic dimension."""
    for key, dim in DIMENSION_MAP.items():
        if key.lower() in agent_name.lower():
            return dim
    return "other"


def _build_intelligence_brief(result, commodity: str, price_data: dict, language: str = "es") -> dict:
    """
    Build a structured intelligence brief grouped by strategic dimensions.
    This is what makes Meteoro X useful — not just a signal, but a BRIEF.
    """
    lang = language.lower()[:2] if language else "es"
    direction = _signal_to_direction(result.final_signal.value)

    # ── Try to get quant execution plan (Python-calculated, NOT LLM) ──
    quant_exec = {}
    if hasattr(result, 'metadata') and isinstance(result.metadata, dict):
        md = result.metadata.get("market_data", {})
        if isinstance(md, dict):
            quant_exec = md.get("quant_execution", {})

    # ── Group agents by dimension ──────────────────────────────
    dimensions = {}
    execution_plan = {}
    key_risk = ""

    # Use quant engine execution plan if available (Python math > LLM guesses)
    if quant_exec and quant_exec.get("entry"):
        entry_data = quant_exec.get("entry", {})
        sl_data = quant_exec.get("stop_loss", {})
        tp_data = quant_exec.get("take_profit", {})
        kelly_data = quant_exec.get("kelly", {})
        rr_data = quant_exec.get("risk_reward", {})
        var_data = quant_exec.get("var", {})

        execution_plan = {
            "entry": entry_data.get("price", 0),
            "stop_loss": sl_data.get("price", 0),
            "take_profit": tp_data.get("price", 0),
            "position_size": round((kelly_data.get("half_kelly", 0)) * 100, 1),
            "risk_reward": rr_data.get("ratio", 0),
            "var_95": var_data.get("var_95_pct", 0),
            "source": "quant_engine",
            "reasoning": f"ATR-based entry/exit. R:R {rr_data.get('ratio', 0):.1f}x. Kelly sizing {kelly_data.get('half_kelly', 0)*100:.1f}%. VaR95 {var_data.get('var_95_pct', 0):.1f}%",
        }

    for r in result.all_results:
        if r.error or r.confidence == 0:
            continue

        dim = _get_agent_dimension(r.agent_name)

        if dim == "execution":
            # Only use LLM execution plan as fallback if quant engine didn't provide one
            if not execution_plan:
                ep = r.evidence_pack.get("raw_llm", {})
                if ep:
                    execution_plan = {
                        "entry": ep.get("entry_price", 0),
                        "stop_loss": ep.get("stop_loss", 0),
                        "take_profit": ep.get("take_profit", 0),
                        "position_size": ep.get("position_size_pct", 0),
                        "reasoning": r.evidence_pack.get("key_finding", r.reasoning[:150]),
                        "source": "llm_fallback",
                    }
            continue

        if dim == "validation":
            # Counterintelligence = key risk
            key_risk = r.evidence_pack.get("key_finding", r.reasoning[:200])
            continue

        if dim == "risk":
            # Risk Guardian info
            continue

        if dim not in dimensions:
            dimensions[dim] = {
                "label": DIMENSION_LABELS.get(dim, {}).get(lang, dim),
                "signals": [],
                "avg_confidence": 0,
                "direction": "NEUTRAL",
                "key_finding": "",
            }

        dimensions[dim]["signals"].append({
            "signal": r.signal.value,
            "confidence": r.confidence,
            "key_finding": r.evidence_pack.get("key_finding", ""),
            "reasoning": r.reasoning[:200],
        })

    # ── Aggregate each dimension ───────────────────────────────
    for dim_key, dim_data in dimensions.items():
        signals = dim_data["signals"]
        if not signals:
            continue

        # Average confidence
        dim_data["avg_confidence"] = round(sum(s["confidence"] for s in signals) / len(signals))

        # Dominant direction
        buys = sum(1 for s in signals if s["signal"] in ("BUY", "LONG"))
        sells = sum(1 for s in signals if s["signal"] in ("SELL", "SHORT"))
        if buys > sells:
            dim_data["direction"] = "BULLISH"
        elif sells > buys:
            dim_data["direction"] = "BEARISH"
        else:
            dim_data["direction"] = "NEUTRAL"

        # Best key finding (highest confidence)
        best = max(signals, key=lambda s: s["confidence"])
        dim_data["key_finding"] = best["key_finding"] or best["reasoning"][:120]

    # ── Build headline recommendation ──────────────────────────
    price = price_data.get("price", 0)
    change = price_data.get("change_pct", 0)
    rsi = price_data.get("rsi_14", 0)

    if lang == "es":
        action_word = {"LONG": "COMPRA", "SHORT": "VENTA", "HOLD": "ESPERA"}.get(direction, "ESPERA")
        if result.conviction >= 70:
            headline = f"{action_word} {commodity} — Alta convicción ({result.conviction}%)"
        elif result.conviction >= 50:
            headline = f"{action_word} {commodity} — Convicción moderada ({result.conviction}%)"
        else:
            headline = f"ESPERA EN {commodity} — Señales mixtas ({result.conviction}%)"
    else:
        action_word = {"LONG": "BUY", "SHORT": "SELL", "HOLD": "HOLD"}.get(direction, "HOLD")
        headline = f"{action_word} {commodity} — {result.conviction}% conviction"

    # ── Build summary (2-3 sentences max, clear and actionable) ──
    dim_list = sorted(dimensions.values(), key=lambda d: d["avg_confidence"], reverse=True)
    top_dims = [d for d in dim_list if d["avg_confidence"] >= 50][:3]

    if lang == "es" and top_dims:
        summary_parts = []
        for d in top_dims:
            dir_word = {"BULLISH": "alcista", "BEARISH": "bajista"}.get(d["direction"], "neutral")
            summary_parts.append(f"{d['label']}: {dir_word} ({d['avg_confidence']}%)")
        summary = f"Sistema agéntico analizó {commodity} en tiempo real. " + ". ".join([d["key_finding"][:100] for d in top_dims if d["key_finding"]])[:400]
        # Enrich with quant data if available
        if quant_exec and quant_exec.get("entry"):
            rr = quant_exec.get("risk_reward", {}).get("ratio", 0)
            kelly_pct = quant_exec.get("kelly", {}).get("half_kelly", 0) * 100
            if rr > 0:
                summary += f" R:R {rr:.1f}x, Kelly {kelly_pct:.0f}%."
    else:
        summary = f"Agentic system analyzed {commodity} in real-time across multiple dimensions."

    # ── Build radar data (for frontend visualization) ──────────
    radar = []
    dim_order = ["supply", "demand", "macro", "technical", "geopolitical", "sentiment"]
    for dim_key in dim_order:
        if dim_key in dimensions:
            d = dimensions[dim_key]
            radar.append({
                "dimension": dim_key,
                "label": d["label"],
                "score": d["avg_confidence"],
                "direction": d["direction"],
                "finding": d["key_finding"][:150],
            })
        else:
            radar.append({
                "dimension": dim_key,
                "label": DIMENSION_LABELS.get(dim_key, {}).get(lang, dim_key),
                "score": 0,
                "direction": "NEUTRAL",
                "finding": "",
            })

    return {
        "headline": headline,
        "action": action_word,
        "summary": summary,
        "dimensions": radar,
        "execution_plan": execution_plan,
        "key_risk": key_risk,
    }


# ═══════════════════════════════════════════════════════════════
# MODELS
# ═══════════════════════════════════════════════════════════════

class AnalyzeRequest(BaseModel):
    command: str
    language: str = "es"


class SwarmAnalyzeRequest(BaseModel):
    commodity: str
    context: Optional[dict] = None


# ═══════════════════════════════════════════════════════════════
# REST ENDPOINTS
# ═══════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "meteoro_app.html")
    if os.path.exists(html_path):
        with open(html_path, "r") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Meteoro X v7.0 — Agent Swarm Active</h1>")


@app.get("/api/health")
async def health():
    swarm = get_swarm()
    # Import available provider info without exposing model names
    try:
        from multi_model_router import _get_available_models, get_cost_summary
        active_models = len(_get_available_models())
        cost = get_cost_summary()
    except Exception:
        active_models = 0
        cost = {}
    # Knowledge graph stats
    kg_stats = {}
    if HAS_KNOWLEDGE:
        kg_stats = get_knowledge_graph_stats()

    return {
        "status": "operational",
        "system": "Meteoro X Autonomous Intelligence",
        "version": "14.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "swarm_active": HAS_SWARM,
        "knowledge_graph": HAS_KNOWLEDGE,
        "knowledge_stats": kg_stats,
        "capabilities": {
            "satellite_recon": True,
            "maritime_intel": True,
            "supply_chain": True,
            "geopolitical_risk": True,
            "quantitative_analysis": True,
            "risk_management": True,
            "industry_knowledge": HAS_KNOWLEDGE,
            "autonomous_memory": HAS_MEMORY,
            "correspondents_network": HAS_CORRESPONDENTS,
        },
        "active_providers": active_models,
        "total_providers": 6,
        "data_sources": 8,
        "signals_generated": len(signal_history),
        "cost_summary": cost,
        "memory_active": memory_instance is not None,
        "proactive_engine": HAS_PROACTIVE,
        "uptime": "active",
    }


@app.get("/api/swarm/config")
async def swarm_config():
    swarm = get_swarm()
    if swarm:
        return swarm.to_dict()
    return {"error": "Swarm not initialized", "has_swarm": HAS_SWARM}


@app.post("/api/analyze")
async def analyze_endpoint(request: AnalyzeRequest):
    """
    Full analysis — uses Swarm if available, falls back to legacy pipeline.
    """
    start = time.time()
    commodity = detect_commodity(request.command)

    # Try swarm first
    swarm = get_swarm()
    if swarm:
        try:
            result = await swarm.analyze(commodity, context={"command": request.command})
            latency_ms = int((time.time() - start) * 1000)

            # Build rich narrative (locale-aware)
            narrative = _build_narrative(result, commodity, language=request.language)
            direction = narrative["direction"]

            # Map agent names to capability display names in evidence cards
            lang = (request.language or "es")[:2].lower()

            # Extract price data from swarm's market data if available
            price_data = {}
            md = {}  # Initialize to prevent NameError
            if hasattr(result, 'metadata') and isinstance(result.metadata, dict):
                md = result.metadata.get("market_data", {})
                if isinstance(md, dict):
                    cd = md.get("commodity", {})
                    if isinstance(cd, dict) and "price" in cd:
                        price_data = {
                            "price": cd.get("price"),
                            "change_pct": cd.get("change_pct", 0),
                            "rsi_14": cd.get("rsi_14"),
                            "volatility": cd.get("volatility_ann_pct"),
                            "sma_10": cd.get("sma_10"),
                            "sma_20": cd.get("sma_20"),
                        }
                    # Fallback: check if price is at top level of market_data
                    if not price_data and "price" in md:
                        price_data = {"price": md.get("price"), "change_pct": md.get("change_pct", 0)}
                    # Fallback 2: look for any nested dict with price
                    if not price_data:
                        for key, val in md.items():
                            if isinstance(val, dict) and "price" in val:
                                price_data = {
                                    "price": val.get("price"),
                                    "change_pct": val.get("change_pct", 0),
                                    "rsi_14": val.get("rsi_14"),
                                    "volatility": val.get("volatility_ann_pct"),
                                }
                                break

            # Build intelligence brief (structured by dimension)
            intel_brief = _build_intelligence_brief(
                result, commodity, price_data, language=request.language
            )

            # Build response — frontend-compatible structure
            response = {
                "pack_id": f"SW-{int(time.time())}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "command": request.command,
                "commodity": commodity,
                "system": "swarm_v12",
                "price_data": price_data,
                "signal": {
                    "action": result.final_signal.value,
                    "direction": direction,
                    "ticker": commodity,
                    "conviction": result.conviction,
                    "reasoning": result.reasoning,
                },
                "swarm_results": {
                    "agents_bullish": result.agents_bullish,
                    "agents_bearish": result.agents_bearish,
                    "agents_neutral": result.agents_neutral,
                    "risk_veto": result.risk_guardian_veto,
                    "total_agents": len(result.all_results),
                },
                "what_happened": narrative["what_happened"],
                "why_it_matters": narrative["why_it_matters"],
                "consensus": narrative["consensus"],
                "evidence_cards": [
                    {
                        "source": _get_capability_name(r.agent_name, lang),
                        "signal": r.signal.value,
                        "confidence": r.confidence,
                        "summary": r.reasoning[:200],
                        "key_finding": r.evidence_pack.get("key_finding", ""),
                    }
                    for r in result.all_results
                    if not r.error and r.reasoning and "unavailable" not in r.reasoning.lower()
                ],
                "top_findings": narrative["top_findings"],
                "causal_chain": [
                    f"[{_get_capability_name(r.agent_name, lang)}] {r.signal.value} ({r.confidence}%): {r.reasoning[:80]}"
                    for r in result.all_results if not r.error and r.confidence > 0
                ],
                "risk_assessment": {
                    "veto_active": result.risk_guardian_veto,
                    "conviction_level": "HIGH" if result.conviction >= 75 else "MEDIUM" if result.conviction >= 50 else "LOW",
                },
                "cost_usd": result.cost_usd,
                "pipeline_latency_ms": latency_ms,
                "execution_mode": result.metadata.get("execution_mode", "sequential") if hasattr(result, 'metadata') and isinstance(result.metadata, dict) else "sequential",
                "providers_used": len(set(r.evidence_pack.get("model", "unknown") for r in result.all_results if not r.error)),
                "intelligence_brief": intel_brief,
                "industry_context": {
                    "available": HAS_KNOWLEDGE,
                    "prompt_injected": HAS_KNOWLEDGE,
                } if HAS_KNOWLEDGE else {"available": False},
                # V17: Quantitative Engine + Satellite + Signal Tracker
                "quant_engine": md.get("quant", {}) if isinstance(md, dict) else {},
                "quant_execution": md.get("quant_execution", {}) if isinstance(md, dict) else {},
                "satellite_intel": md.get("satellite", {}) if isinstance(md, dict) else {},
                "data_sources_used": [
                    s for s in [
                        "yfinance" if price_data else None,
                        "GDELT" if isinstance(md, dict) and md.get("news", {}).get("articles") else None,
                        "knowledge_graph" if HAS_KNOWLEDGE else None,
                        "NASA_FIRMS" if isinstance(md, dict) and md.get("satellite") else None,
                        "quant_engine" if isinstance(md, dict) and md.get("quant") else None,
                        "memory" if hasattr(result, 'metadata') and isinstance(result.metadata, dict) and result.metadata.get("has_real_data") else None,
                    ] if s is not None
                ],
                "pack_hash": f"sha256:{hash(result.reasoning) & 0xFFFFFFFF:08x}",
            }

            # Sanitize NaN values (numpy NaN breaks JSON serialization)
            response = _sanitize_nan(response)

            signal_history.append({
                "pack_id": response["pack_id"],
                "timestamp": response["timestamp"],
                "command": request.command,
                "commodity": commodity,
                "signal": response["signal"],
                "latency_ms": latency_ms,
                "system": "swarm_v12",
            })

            return response

        except Exception as e:
            print(f"[SWARM ERROR] {e}")
            traceback.print_exc()

    # Fallback to legacy pipeline
    if HAS_LEGACY:
        try:
            pack = await legacy_analyze(request.command)
            latency_ms = int((time.time() - start) * 1000)

            signal_entry = {
                "pack_id": pack.pack_id,
                "timestamp": pack.timestamp,
                "command": request.command,
                "signal": asdict(pack.signal) if pack.signal else None,
                "latency_ms": latency_ms,
                "system": "legacy_v2",
            }
            signal_history.append(signal_entry)

            return {
                "pack_id": pack.pack_id,
                "timestamp": pack.timestamp,
                "command": pack.command,
                "what_happened": pack.what_happened,
                "why_it_matters": pack.why_it_matters,
                "signal": asdict(pack.signal) if pack.signal else None,
                "evidence_cards": pack.evidence_cards,
                "causal_chain": pack.causal_chain,
                "risk_assessment": pack.risk_assessment,
                "pack_hash": pack.pack_hash,
                "pipeline_latency_ms": latency_ms,
                "system": "legacy_v2",
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Pipeline error: {str(e)}")

    raise HTTPException(status_code=503, detail="No analysis engine available")


@app.post("/api/swarm/analyze")
async def swarm_analyze(request: SwarmAnalyzeRequest):
    """Direct swarm analysis endpoint."""
    swarm = get_swarm()
    if not swarm:
        raise HTTPException(status_code=503, detail="Swarm not available")

    start = time.time()
    result = await swarm.analyze(request.commodity, context=request.context)
    latency_ms = int((time.time() - start) * 1000)

    return {
        "commodity": request.commodity,
        "signal": result.final_signal.value,
        "conviction": result.conviction,
        "reasoning": result.reasoning,
        "agents_bullish": result.agents_bullish,
        "agents_bearish": result.agents_bearish,
        "agents_neutral": result.agents_neutral,
        "risk_veto": result.risk_guardian_veto,
        "agents": [r.to_dict() for r in result.all_results],
        "cost_usd": result.cost_usd,
        "latency_ms": latency_ms,
    }


@app.get("/api/macro")
async def macro_snapshot():
    if HAS_MACRO:
        try:
            data = await get_macro_snapshot()
            return data
        except Exception as e:
            return {"error": str(e), "fallback": True, "data": {}}
    return {"error": "Macro module not available", "fallback": True}


@app.get("/api/signals")
async def get_signals(limit: int = 20):
    return {
        "signals": signal_history[-limit:],
        "total": len(signal_history),
    }


@app.get("/api/latency")
async def get_latency_stats():
    if benchmark:
        return benchmark.get_stats()
    return {"error": "Benchmark not available"}


@app.get("/api/ping")
async def ping():
    """Lightweight keep-alive endpoint — used by background self-ping to prevent Render from sleeping."""
    return {"pong": True, "ts": time.time(), "build": "v15.5-live"}


@app.get("/api/debug")
async def debug_last_analysis():
    """Debug endpoint — shows raw LLM responses from last analysis."""
    try:
        from meteoro_swarm import _last_debug
        return {"debug": _last_debug, "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/diagnostics")
async def diagnostics():
    """System diagnostics — shows provider status without exposing keys."""
    try:
        from multi_model_router import (
            _get_available_models, _get_available_providers,
            get_active_routing, get_cost_summary, MODELS, _is_valid_api_key
        )
        import os

        models = _get_available_models()
        providers = _get_available_providers()
        routing = get_active_routing()
        cost = get_cost_summary()

        # Check each provider (without exposing keys)
        provider_status = {}
        for model_key, profile in MODELS.items():
            key = os.getenv(profile.api_key_env, "")
            if not key:
                status = "not_configured"
            elif not _is_valid_api_key(key):
                status = "placeholder"
            else:
                status = "active"
            provider_status[model_key] = {
                "provider": profile.provider.value,
                "status": status,
                "model_id": profile.model_id,
            }

        # Count routing distribution
        model_counts = {}
        for agent, model in routing.items():
            model_counts[model] = model_counts.get(model, 0) + 1

        return {
            "status": "ok",
            "version": "14.0.0",
            "providers": provider_status,
            "active_models": list(models),
            "active_providers": list(providers),
            "routing_distribution": model_counts,
            "total_agents": len(routing),
            "cost_summary": cost,
            "swarm_active": HAS_SWARM,
            "proactive_engine": HAS_PROACTIVE,
            "data_sources_active": HAS_MARKET_DATA if 'HAS_MARKET_DATA' in dir() else "unknown",
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/providers")
async def provider_status_endpoint():
    """Detailed provider status — health, cost, specialization."""
    if HAS_PROVIDER_STATUS:
        return {
            "status": "ok",
            "providers": get_provider_status(),
            "total": 6,
            "env_keys_needed": [
                "ANTHROPIC_API_KEY", "DEEPSEEK_API_KEY", "KIMI_API_KEY",
                "GEMINI_API_KEY", "OPENAI_API_KEY", "GROQ_API_KEY"
            ],
        }
    return {"error": "Provider status not available"}


# ── PROACTIVE MONITOR ENDPOINTS ──────────────────────────────

@app.get("/api/proactive/status")
async def proactive_status():
    """Status of the proactive monitoring engine."""
    if not HAS_PROACTIVE:
        return {"active": False, "reason": "ProactiveMonitor not loaded"}
    try:
        monitor = ProactiveMonitor.get_instance()
        return monitor.get_status()
    except Exception as e:
        return {"active": False, "error": str(e)}


@app.post("/api/proactive/trigger")
async def proactive_trigger(commodity: Optional[str] = None):
    """Manually trigger a proactive analysis cycle."""
    if not HAS_PROACTIVE:
        return {"error": "ProactiveMonitor not loaded"}
    try:
        monitor = ProactiveMonitor.get_instance()
        if commodity:
            result = await monitor.analyze_commodity(commodity)
            return {"triggered": True, "commodity": commodity, "signal": result}
        else:
            asyncio.create_task(monitor.run_cycle())
            return {"triggered": True, "commodity": "all_watchlist"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/proactive/signals")
async def proactive_signals():
    """Get recent proactive signals."""
    if not HAS_PROACTIVE:
        return {"signals": [], "reason": "ProactiveMonitor not loaded"}
    try:
        monitor = ProactiveMonitor.get_instance()
        return {"signals": monitor.get_recent_signals(), "count": len(monitor.recent_signals)}
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════
# WEBSOCKET ENDPOINT
# ═══════════════════════════════════════════════════════════════

@app.websocket("/ws/analyze")
async def websocket_analyze(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_json()
            command = data.get("command", "")
            if not command:
                await websocket.send_json({"error": "No command provided"})
                continue

            commodity = detect_commodity(command)
            ws_lang = data.get("language", "es")[:2].lower()
            start = time.time()

            await websocket.send_json({
                "type": "progress",
                "stage": "swarm_init",
                "message": f"Deploying agentic system on {commodity}...",
                "progress": 5,
            })

            swarm = get_swarm()
            if swarm:
                try:
                    result = await swarm.analyze(commodity, context={"command": command})
                    latency_ms = int((time.time() - start) * 1000)

                    # Build rich narrative (locale-aware)
                    narrative = _build_narrative(result, commodity, language=ws_lang)
                    direction = narrative["direction"]

                    await websocket.send_json({
                        "type": "result",
                        "pack_id": f"SW-{int(time.time())}",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "command": command,
                        "commodity": commodity,
                        "signal": {
                            "action": result.final_signal.value,
                            "direction": direction,
                            "ticker": commodity,
                            "conviction": result.conviction,
                            "reasoning": result.reasoning,
                        },
                        "swarm_results": {
                            "agents_bullish": result.agents_bullish,
                            "agents_bearish": result.agents_bearish,
                            "agents_neutral": result.agents_neutral,
                            "risk_veto": result.risk_guardian_veto,
                            "total_agents": len(result.all_results),
                        },
                        "what_happened": narrative["what_happened"],
                        "why_it_matters": narrative["why_it_matters"],
                        "consensus": narrative["consensus"],
                        "evidence_cards": [
                            {
                                "source": _get_capability_name(r.agent_name, ws_lang),
                                "signal": r.signal.value,
                                "confidence": r.confidence,
                                "summary": r.reasoning[:200],
                                "key_finding": r.evidence_pack.get("key_finding", ""),
                            }
                            for r in result.all_results if not r.error and r.confidence > 0
                        ],
                        "top_findings": narrative["top_findings"],
                        "risk_assessment": {
                            "veto_active": result.risk_guardian_veto,
                            "conviction_level": "HIGH" if result.conviction >= 75 else "MEDIUM" if result.conviction >= 50 else "LOW",
                        },
                        "cost_usd": result.cost_usd,
                        "pipeline_latency_ms": latency_ms,
                    })
                except Exception as e:
                    await websocket.send_json({"type": "error", "message": str(e)})

            elif HAS_LEGACY:
                try:
                    pack = await legacy_analyze(command)
                    latency_ms = int((time.time() - start) * 1000)
                    await websocket.send_json({
                        "type": "result",
                        "pack_id": pack.pack_id,
                        "timestamp": pack.timestamp,
                        "command": pack.command,
                        "what_happened": pack.what_happened,
                        "why_it_matters": pack.why_it_matters,
                        "signal": asdict(pack.signal) if pack.signal else None,
                        "evidence_cards": pack.evidence_cards,
                        "pipeline_latency_ms": latency_ms,
                    })
                except Exception as e:
                    await websocket.send_json({"type": "error", "message": str(e)})
            else:
                await websocket.send_json({"type": "error", "message": "No engine available"})

    except WebSocketDisconnect:
        pass


# ═══════════════════════════════════════════════════════════════
# KNOWLEDGE GRAPH ENDPOINTS
# ═══════════════════════════════════════════════════════════════

@app.get("/api/knowledge/exchanges")
async def knowledge_exchanges():
    """All global commodity exchanges with geographic data."""
    if not HAS_KNOWLEDGE:
        raise HTTPException(status_code=503, detail="Knowledge graph not available")
    return {
        "exchanges": get_all_exchanges_summary(),
        "total": len(EXCHANGES),
        "source": "meteoro_knowledge_graph_v12",
    }


@app.get("/api/knowledge/traders")
async def knowledge_traders():
    """Major global commodity traders with revenue, commodities, and HQ locations."""
    if not HAS_KNOWLEDGE:
        raise HTTPException(status_code=503, detail="Knowledge graph not available")
    return {
        "traders": get_all_traders_summary(),
        "total": len(MAJOR_TRADERS),
        "source": "meteoro_knowledge_graph_v12",
    }


@app.get("/api/knowledge/mines")
async def knowledge_mines():
    """Major mines worldwide with production data and geographic coordinates."""
    if not HAS_KNOWLEDGE:
        raise HTTPException(status_code=503, detail="Knowledge graph not available")
    return {
        "mines": get_all_mines_summary(),
        "total": len(MAJOR_MINES),
        "source": "meteoro_knowledge_graph_v12",
    }


@app.get("/api/knowledge/plants")
async def knowledge_plants():
    """Refineries and smelters with capacity and geographic data."""
    if not HAS_KNOWLEDGE:
        raise HTTPException(status_code=503, detail="Knowledge graph not available")
    return {
        "plants": get_all_plants_summary(),
        "source": "meteoro_knowledge_graph_v12",
    }


@app.get("/api/knowledge/ports")
async def knowledge_ports():
    """Major commodity ports worldwide — throughput, bottleneck risks, vessel sizes."""
    if not HAS_KNOWLEDGE:
        raise HTTPException(status_code=503, detail="Knowledge graph not available")
    return {
        "ports": get_all_ports_summary(),
        "source": "meteoro_knowledge_graph_v12",
    }


@app.get("/api/knowledge/shipping")
async def knowledge_shipping():
    """Shipping companies (navieras) — fleet sizes, routes, commodity coverage."""
    if not HAS_KNOWLEDGE:
        raise HTTPException(status_code=503, detail="Knowledge graph not available")
    return {
        "shipping_companies": get_all_shipping_summary(),
        "source": "meteoro_knowledge_graph_v12",
    }


@app.get("/api/knowledge/logistics")
async def knowledge_logistics():
    """Rail, road, and pipeline logistics companies — corridors, commodities."""
    if not HAS_KNOWLEDGE:
        raise HTTPException(status_code=503, detail="Knowledge graph not available")
    return {
        "logistics_companies": get_all_logistics_summary(),
        "source": "meteoro_knowledge_graph_v12",
    }


@app.get("/api/knowledge/qa")
async def knowledge_qa():
    """Inspection and quality assurance companies — the trust layer of commodity trading."""
    if not HAS_KNOWLEDGE:
        raise HTTPException(status_code=503, detail="Knowledge graph not available")
    return {
        "inspection_qa": get_all_qa_summary(),
        "source": "meteoro_knowledge_graph_v12",
    }


@app.get("/api/knowledge/clients")
async def knowledge_clients():
    """End consumers / buyers — who ultimately buys what mines and traders sell."""
    if not HAS_KNOWLEDGE:
        raise HTTPException(status_code=503, detail="Knowledge graph not available")
    return {
        "end_clients": get_all_clients_summary(),
        "source": "meteoro_knowledge_graph_v12",
    }


@app.get("/api/knowledge/correspondents")
async def knowledge_correspondents():
    """Autonomous correspondent network — AI correspondents in 12+ commodity countries."""
    if not HAS_CORRESPONDENTS:
        raise HTTPException(status_code=503, detail="Correspondents network not available")
    return {
        "correspondents": get_all_correspondents_summary(),
        "total_countries": len(CORRESPONDENTS),
        "source": "meteoro_correspondents_v12",
    }


@app.get("/api/knowledge/stats")
async def knowledge_stats():
    """Knowledge graph statistics — entity counts across all categories."""
    if not HAS_KNOWLEDGE:
        raise HTTPException(status_code=503, detail="Knowledge graph not available")
    stats = get_knowledge_graph_stats()
    # Add correspondents count
    if HAS_CORRESPONDENTS:
        stats["correspondents"] = len(CORRESPONDENTS)
        stats["total_entities"] = stats.get("total_entities", 0) + len(CORRESPONDENTS)
    return {
        "stats": stats,
        "source": "meteoro_knowledge_graph_v12",
    }


@app.get("/api/knowledge/{commodity}")
async def knowledge_commodity(commodity: str):
    """Full industry context for a specific commodity — complete supply chain from mine to consumer."""
    if not HAS_KNOWLEDGE:
        raise HTTPException(status_code=503, detail="Knowledge graph not available")

    detected = detect_commodity(commodity)
    ctx = get_commodity_context(detected.lower())

    return {
        "commodity": detected,
        "query": commodity,
        "context": ctx,
        "agent_prompt": build_agent_context_prompt(detected.lower()),
        "source": "meteoro_knowledge_graph_v12",
    }


# ═══════════════════════════════════════════════════════════════
# MEMORY & PERFORMANCE ENDPOINTS (v13)
# ═══════════════════════════════════════════════════════════════

@app.get("/api/memory/dashboard")
async def memory_dashboard():
    """Full performance dashboard — accuracy, leaderboard, streaks, calibration."""
    if not memory_instance:
        raise HTTPException(status_code=503, detail="Memory system not available")
    try:
        dashboard = await memory_instance.get_performance_dashboard()
        return {"dashboard": dashboard, "source": "meteoro_autonomous_memory_v13"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/memory/track-record")
async def memory_track_record(commodity: str = None):
    """Track record — overall or per-commodity accuracy statistics."""
    if not memory_instance:
        raise HTTPException(status_code=503, detail="Memory system not available")
    try:
        record = await memory_instance.get_track_record(commodity)
        return {"track_record": record, "source": "meteoro_autonomous_memory_v13"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/memory/leaderboard")
async def memory_agent_leaderboard(commodity: str = None):
    """Agent leaderboard — which agents are most accurate, ranked by performance."""
    if not memory_instance:
        raise HTTPException(status_code=503, detail="Memory system not available")
    try:
        leaderboard = await memory_instance.get_agent_leaderboard(commodity)
        return {"leaderboard": leaderboard, "source": "meteoro_autonomous_memory_v13"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/memory/calibration/{commodity}")
async def memory_calibration_weights(commodity: str):
    """Calibration weights — auto-adjusted agent weights based on historical accuracy."""
    if not memory_instance:
        raise HTTPException(status_code=503, detail="Memory system not available")
    try:
        detected = detect_commodity(commodity)
        weights = await memory_instance.get_calibration_weights(detected)
        return {
            "commodity": detected,
            "weights": weights,
            "source": "meteoro_autonomous_memory_v13",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/memory/context/{commodity}")
async def memory_context(commodity: str):
    """Memory context — narrative historical context for a commodity (what agents receive)."""
    if not memory_instance:
        raise HTTPException(status_code=503, detail="Memory system not available")
    try:
        detected = detect_commodity(commodity)
        context = await memory_instance.get_memory_context(detected)
        return {
            "commodity": detected,
            "memory_context": context or "No historical memory for this commodity yet.",
            "source": "meteoro_autonomous_memory_v13",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/memory/update-outcomes")
async def memory_update_outcomes(days: int = 5):
    """Manually trigger outcome evaluation — compare past signals vs actual price movements."""
    if not memory_instance:
        raise HTTPException(status_code=503, detail="Memory system not available")
    try:
        result = await memory_instance.update_outcomes(days)
        return {"result": result, "source": "meteoro_autonomous_memory_v13"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════

async def _keep_alive_loop():
    """
    Self-ping every 10 minutes to prevent Render free tier from sleeping.
    Pings the PUBLIC URL so Render counts it as inbound traffic.
    """
    # Determine public URL
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "")
    if not render_url:
        # Fallback: try common domains
        for domain in ["https://meteoro.io", "https://meteoro-x-api.onrender.com"]:
            render_url = domain
            break

    if not render_url:
        print("[KEEPALIVE] No public URL detected — disabled")
        return

    print(f"[KEEPALIVE] Active — pinging {render_url}/api/ping every 10 min")

    try:
        import httpx
    except ImportError:
        print("[KEEPALIVE] httpx not available — disabled")
        return

    while True:
        await asyncio.sleep(600)  # 10 minutes
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(f"{render_url}/api/ping")
                print(f"[KEEPALIVE] Ping OK ({r.status_code})")
        except Exception as e:
            print(f"[KEEPALIVE] Ping failed: {str(e)[:60]}")


@app.on_event("startup")
async def startup():
    print("=" * 60)
    print("  METEORO X v14 — Autonomous Intelligence API")
    print("  Agentic System | Multi-Model | Knowledge Graph")
    print(f"  Swarm Available: {HAS_SWARM}")
    print(f"  Legacy Pipeline: {HAS_LEGACY}")
    # Log active providers
    try:
        from multi_model_router import _get_available_models, _get_available_providers
        models = _get_available_models()
        providers = _get_available_providers()
        print(f"  Active Models: {models or 'none'}")
        print(f"  Active Providers: {providers or 'none'}")
    except Exception as e:
        print(f"  Router status: {e}")
    print("=" * 60)

    # WARMUP DISABLED: Consumes free tier tokens/requests quotas
    # Groq free tier: 100K tokens/day, Gemini free tier: 20 requests/day
    # Each warmup call wastes 50-100 tokens. Not worth it with limited quotas.
    print("[WARMUP] ═══ DISABLED — preserving free tier quotas ═══")

    # Pre-initialize swarm
    get_swarm()
    # Start keep-alive background task
    asyncio.create_task(_keep_alive_loop())


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("meteoro_api:app", host="0.0.0.0", port=port, reload=False)
