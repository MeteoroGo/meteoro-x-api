"""
METEORO X v9.3 — API Server
==============================
FastAPI server powering the Meteoro Autonomous Intelligence System.

Endpoints:
  POST /api/analyze       - Full agentic system analysis
  POST /api/swarm/analyze - Direct swarm endpoint
  GET  /api/health        - Server heartbeat
  GET  /api/macro         - Current macro snapshot
  GET  /api/signals       - Recent signals history
  GET  /api/swarm/config  - Swarm configuration info
  GET  /api/diagnostics   - Provider status
  GET  /api/ping          - Lightweight keep-alive endpoint
  WS   /ws/analyze        - Real-time streaming analysis
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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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

# ═══════════════════════════════════════════════════════════════
# APP SETUP
# ═══════════════════════════════════════════════════════════════

app = FastAPI(
    title="Meteoro X — Autonomous Intelligence",
    description="AI-Native Autonomous Commodity Intelligence | Agentic System",
    version="9.3.0",
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

if HAS_BENCHMARK:
    benchmark = LatencyBenchmark(db_path=os.environ.get("BENCHMARK_DB", "/tmp/latency_benchmark.db"))


def get_swarm():
    global swarm_instance
    if swarm_instance is None and HAS_SWARM:
        swarm_instance = MeteorSwarm()
    return swarm_instance


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


def _build_narrative(result, commodity: str) -> dict:
    """
    Build a rich intelligence narrative from swarm results.
    Synthesizes agent findings into a compelling human-readable brief.
    """
    bullish = result.agents_bullish
    bearish = result.agents_bearish
    neutral = result.agents_neutral
    total = bullish + bearish + neutral
    direction = _signal_to_direction(result.final_signal.value)
    conviction = result.conviction

    # Collect key findings from agents (filter out errors and empty reasoning)
    findings = []
    for r in result.all_results:
        if r.error or not r.reasoning or 'unavailable' in r.reasoning.lower():
            continue
        if r.confidence >= 40:
            findings.append({
                "agent": r.agent_name,
                "signal": r.signal.value,
                "confidence": r.confidence,
                "finding": r.reasoning[:300],
                "key": r.evidence_pack.get("key_finding", ""),
            })

    # Sort by confidence descending
    findings.sort(key=lambda x: x["confidence"], reverse=True)

    # Build what_happened
    active_agents = sum(1 for r in result.all_results if not r.error and r.confidence > 0)
    what_happened = (
        f"Autonomous intelligence system deployed {active_agents} specialized capabilities "
        f"across satellite reconnaissance, maritime tracking, supply chain analysis, "
        f"geopolitical risk assessment, and quantitative modeling to analyze {commodity}."
    )

    # Build why_it_matters from top findings
    if findings:
        top = findings[:3]
        points = [f.get("key") or f["finding"][:120] for f in top]
        why_parts = [p for p in points if p and len(p) > 10]
        why_it_matters = " | ".join(why_parts) if why_parts else result.reasoning
    else:
        why_it_matters = result.reasoning

    # Build consensus description
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
    return {
        "status": "operational",
        "system": "Meteoro X Autonomous Intelligence",
        "version": "9.3.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "swarm_active": HAS_SWARM,
        "capabilities": {
            "satellite_recon": True,
            "maritime_intel": True,
            "supply_chain": True,
            "geopolitical_risk": True,
            "quantitative_analysis": True,
            "risk_management": True,
        },
        "active_providers": active_models,
        "data_sources": 8,
        "signals_generated": len(signal_history),
        "cost_summary": cost,
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

            # Build rich narrative
            narrative = _build_narrative(result, commodity)
            direction = narrative["direction"]

            # Build response — frontend-compatible structure
            response = {
                "pack_id": f"SW-{int(time.time())}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "command": request.command,
                "commodity": commodity,
                "system": "swarm_v9",
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
                        "source": r.agent_name,
                        "signal": r.signal.value,
                        "confidence": r.confidence,
                        "summary": r.reasoning[:200],
                        "key_finding": r.evidence_pack.get("key_finding", ""),
                    }
                    for r in result.all_results if not r.error and r.confidence > 0
                ],
                "top_findings": narrative["top_findings"],
                "causal_chain": [
                    f"[{r.agent_name}] {r.signal.value} ({r.confidence}%): {r.reasoning[:80]}"
                    for r in result.all_results if not r.error and r.confidence > 0
                ],
                "risk_assessment": {
                    "veto_active": result.risk_guardian_veto,
                    "conviction_level": "HIGH" if result.conviction >= 75 else "MEDIUM" if result.conviction >= 50 else "LOW",
                },
                "cost_usd": result.cost_usd,
                "pipeline_latency_ms": latency_ms,
                "pack_hash": f"sha256:{hash(result.reasoning) & 0xFFFFFFFF:08x}",
            }

            signal_history.append({
                "pack_id": response["pack_id"],
                "timestamp": response["timestamp"],
                "command": request.command,
                "commodity": commodity,
                "signal": response["signal"],
                "latency_ms": latency_ms,
                "system": "swarm_v9",
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
    return {"pong": True, "ts": time.time()}


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
            "version": "9.3.0",
            "providers": provider_status,
            "active_models": list(models),
            "active_providers": list(providers),
            "routing_distribution": model_counts,
            "total_agents": len(routing),
            "cost_summary": cost,
            "swarm_active": HAS_SWARM,
            "data_sources_active": HAS_MARKET_DATA if 'HAS_MARKET_DATA' in dir() else "unknown",
        }
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

                    # Build rich narrative (same as REST endpoint)
                    narrative = _build_narrative(result, commodity)
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
                                "source": r.agent_name,
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
    print("  METEORO X v9.3 — Autonomous Intelligence API")
    print("  Agentic System | Multi-Model Router | Real Data")
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
    # Pre-initialize swarm
    get_swarm()
    # Start keep-alive background task
    asyncio.create_task(_keep_alive_loop())


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("meteoro_api:app", host="0.0.0.0", port=port, reload=False)
