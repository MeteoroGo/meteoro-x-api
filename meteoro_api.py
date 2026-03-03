"""
METEORO X v7.0 — API Server
==============================
FastAPI server connecting the Meteoro Agent Swarm to the Lovable frontend.

Endpoints:
  POST /api/analyze       - Full swarm analysis (12 Super Agents)
  POST /api/swarm/analyze - Direct swarm endpoint
  GET  /api/health        - Server heartbeat
  GET  /api/macro         - Current macro snapshot
  GET  /api/signals       - Recent signals history
  GET  /api/swarm/config  - Swarm configuration info
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
    title="Meteoro X — Agent Swarm API",
    description="AI-Native Autonomous Hedge Fund | 12 Super Agents | 4 Cerebros",
    version="7.0.0",
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
    "oil": "OIL", "petroleo": "OIL", "petróleo": "OIL", "crude": "OIL",
    "wti": "OIL", "brent": "OIL", "crudo": "OIL",
    "gas": "NATURAL_GAS", "natural gas": "NATURAL_GAS", "gas natural": "NATURAL_GAS",
    "gold": "GOLD", "oro": "GOLD",
    "silver": "SILVER", "plata": "SILVER",
    "copper": "COPPER", "cobre": "COPPER",
    "coal": "COAL", "carbon": "COAL", "carbón": "COAL", "cerrejon": "COAL",
    "coffee": "COFFEE", "cafe": "COFFEE", "café": "COFFEE",
    "wheat": "WHEAT", "trigo": "WHEAT",
    "corn": "CORN", "maiz": "CORN", "maíz": "CORN",
    "soy": "SOY", "soja": "SOY", "soybean": "SOY",
    "lithium": "LITHIUM", "litio": "LITHIUM",
    "nickel": "NICKEL", "niquel": "NICKEL", "níquel": "NICKEL",
}


def detect_commodity(command: str) -> str:
    lower = command.lower()
    for keyword, commodity in COMMODITY_KEYWORDS.items():
        if keyword in lower:
            return commodity
    return "GENERAL"


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
    return {
        "status": "operational",
        "system": "Meteoro X Agent Swarm",
        "version": "7.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "swarm_active": HAS_SWARM,
        "super_agents": 12 if HAS_SWARM else 0,
        "swarms": 4 if HAS_SWARM else 0,
        "models": ["claude-haiku", "deepseek-v3", "kimi-v1", "gemini-flash"],
        "data_sources": 8,
        "signals_generated": len(signal_history),
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

            # Build response
            response = {
                "pack_id": f"SW-{int(time.time())}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "command": request.command,
                "commodity": commodity,
                "system": "swarm_v7",
                "signal": {
                    "action": result.final_signal.value,
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
                "agent_details": [r.to_dict() for r in result.all_results],
                "cost_usd": result.cost_usd,
                "pipeline_latency_ms": latency_ms,
                "what_happened": f"12 Super Agents analyzed {commodity}",
                "why_it_matters": result.reasoning,
                "evidence_cards": [
                    {
                        "source": r.agent_name,
                        "signal": r.signal.value,
                        "confidence": r.confidence,
                        "summary": r.reasoning[:200],
                    }
                    for r in result.all_results if not r.error
                ],
                "causal_chain": [
                    f"[{r.agent_name}] {r.signal.value} ({r.confidence}%): {r.reasoning[:80]}"
                    for r in result.all_results if not r.error
                ],
                "risk_assessment": {
                    "veto_active": result.risk_guardian_veto,
                    "conviction_level": "HIGH" if result.conviction >= 75 else "MEDIUM" if result.conviction >= 50 else "LOW",
                },
                "pack_hash": f"sha256:{hash(result.reasoning) & 0xFFFFFFFF:08x}",
            }

            signal_history.append({
                "pack_id": response["pack_id"],
                "timestamp": response["timestamp"],
                "command": request.command,
                "commodity": commodity,
                "signal": response["signal"],
                "latency_ms": latency_ms,
                "system": "swarm_v7",
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
                "message": f"Deploying 12 Super Agents on {commodity}...",
                "progress": 5,
            })

            swarm = get_swarm()
            if swarm:
                try:
                    result = await swarm.analyze(commodity, context={"command": command})
                    latency_ms = int((time.time() - start) * 1000)

                    await websocket.send_json({
                        "type": "result",
                        "pack_id": f"SW-{int(time.time())}",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "command": command,
                        "commodity": commodity,
                        "signal": {
                            "action": result.final_signal.value,
                            "conviction": result.conviction,
                            "reasoning": result.reasoning,
                        },
                        "what_happened": f"12 Super Agents analyzed {commodity}",
                        "why_it_matters": result.reasoning,
                        "evidence_cards": [
                            {"source": r.agent_name, "signal": r.signal.value, "confidence": r.confidence}
                            for r in result.all_results if not r.error
                        ],
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

@app.on_event("startup")
async def startup():
    print("=" * 60)
    print("  METEORO X v7.0 — Agent Swarm API")
    print("  12 Super Agents | 4 Swarms | 4 Cerebros")
    print(f"  Swarm Available: {HAS_SWARM}")
    print(f"  Legacy Pipeline: {HAS_LEGACY}")
    print("=" * 60)
    # Pre-initialize swarm
    get_swarm()


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("meteoro_api:app", host="0.0.0.0", port=port, reload=False)
