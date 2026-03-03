"""
METEORO X — API Server
=======================
FastAPI + WebSocket server that exposes the Meteoro intelligence pipeline.
Connects the Lovable.dev frontend to the backend brain.

Endpoints:
  POST /api/analyze       — Trigger full analysis (returns EvidencePack)
  GET  /api/health        — Server heartbeat
  GET  /api/macro         — Current macro snapshot
  GET  /api/signals       — Recent signals history
  WS   /ws/analyze        — Real-time streaming analysis with progress updates
"""

import asyncio
import json
import time
import os
import sys
from datetime import datetime, timezone
from dataclasses import asdict
from typing import Optional

# FastAPI
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

# Pipeline
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from meteoro_pipeline import analyze, classify_command, EvidencePack
from data_sources.fred_api import get_macro_snapshot
from latency_benchmark import LatencyBenchmark

# ═══════════════════════════════════════════════════════════════
# APP SETUP
# ═══════════════════════════════════════════════════════════════

app = FastAPI(
    title="Meteoro X — Agentic Intelligence API",
    description="AI-Native Autonomous Hedge Fund for Commodity Trading",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Lovable preview URLs
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory stores
signal_history = []
benchmark = LatencyBenchmark(db_path=os.environ.get("BENCHMARK_DB", "/tmp/latency_benchmark.db"))


# ═══════════════════════════════════════════════════════════════
# MODELS
# ═══════════════════════════════════════════════════════════════

class AnalyzeRequest(BaseModel):
    command: str
    language: str = "es"


class AnalyzeResponse(BaseModel):
    pack_id: str
    timestamp: str
    command: str
    what_happened: str
    why_it_matters: str
    signal: Optional[dict] = None
    evidence_cards: list = []
    causal_chain: list = []
    risk_assessment: dict = {}
    pack_hash: str = ""
    pipeline_latency_ms: int = 0


# ═══════════════════════════════════════════════════════════════
# REST ENDPOINTS
# ═══════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the Meteoro X frontend."""
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "meteoro_app.html")
    with open(html_path, "r") as f:
        return HTMLResponse(content=f.read())


@app.get("/api/health")
async def health():
    """Server heartbeat with system status."""
    return {
        "status": "operational",
        "system": "Meteoro X Agentic Intelligence",
        "version": "2.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "capabilities": 6,
        "data_sources": 8,
        "signals_generated": len(signal_history),
        "uptime": "active",
    }


@app.post("/api/analyze", response_model=AnalyzeResponse)
async def analyze_endpoint(request: AnalyzeRequest):
    """
    Full analysis pipeline.
    Takes a CEO command, deploys all intelligence capabilities,
    returns an EvidencePack with tradeable signal.
    """
    start = time.time()

    try:
        pack = await analyze(request.command)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline error: {str(e)}")

    latency_ms = int((time.time() - start) * 1000)

    # Store in history
    signal_entry = {
        "pack_id": pack.pack_id,
        "timestamp": pack.timestamp,
        "command": request.command,
        "signal": asdict(pack.signal) if pack.signal else None,
        "pack_hash": pack.pack_hash,
        "latency_ms": latency_ms,
    }
    signal_history.append(signal_entry)

    # Record latency benchmark
    benchmark.record_detection(
        pack_id=pack.pack_id,
        command=request.command,
        detection_time=pack.timestamp,
    )

    return AnalyzeResponse(
        pack_id=pack.pack_id,
        timestamp=pack.timestamp,
        command=pack.command,
        what_happened=pack.what_happened,
        why_it_matters=pack.why_it_matters,
        signal=asdict(pack.signal) if pack.signal else None,
        evidence_cards=pack.evidence_cards,
        causal_chain=pack.causal_chain,
        risk_assessment=pack.risk_assessment,
        pack_hash=pack.pack_hash,
        pipeline_latency_ms=latency_ms,
    )


@app.get("/api/macro")
async def macro_snapshot():
    """Current macro data snapshot (VIX, WTI, Gold, DXY, etc.)."""
    try:
        data = await get_macro_snapshot()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/signals")
async def get_signals(limit: int = 20):
    """Recent signal history."""
    return {
        "signals": signal_history[-limit:],
        "total": len(signal_history),
    }


@app.get("/api/latency")
async def get_latency_stats():
    """Latency benchmarking stats (Shadow Engine Module 1)."""
    return benchmark.get_stats()


@app.post("/api/latency/bloomberg")
async def record_bloomberg_headline(pack_id: str, headline_url: str = ""):
    """Record when Bloomberg publishes a headline for a detected event."""
    benchmark.record_bloomberg_headline(
        pack_id=pack_id,
        bloomberg_time=datetime.now(timezone.utc).isoformat(),
        headline_url=headline_url,
    )
    return {"status": "recorded", "pack_id": pack_id}


# ═══════════════════════════════════════════════════════════════
# WEBSOCKET ENDPOINT (Real-time streaming)
# ═══════════════════════════════════════════════════════════════

@app.websocket("/ws/analyze")
async def websocket_analyze(websocket: WebSocket):
    """
    Real-time streaming analysis.
    Client sends: {"command": "Analiza carbon colombiano"}
    Server streams progress updates and final EvidencePack.
    """
    await websocket.accept()

    try:
        while True:
            # Receive command
            data = await websocket.receive_json()
            command = data.get("command", "")

            if not command:
                await websocket.send_json({"error": "No command provided"})
                continue

            start = time.time()

            # Define callback for progress updates
            async def send_progress(update):
                try:
                    await websocket.send_json({
                        "type": "progress",
                        **update,
                    })
                except Exception:
                    pass

            # Run pipeline with streaming progress
            try:
                pack = await analyze(command, callback=send_progress)
                latency_ms = int((time.time() - start) * 1000)

                # Send final result
                await websocket.send_json({
                    "type": "result",
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
                })

                # Store in history
                signal_history.append({
                    "pack_id": pack.pack_id,
                    "timestamp": pack.timestamp,
                    "command": command,
                    "signal": asdict(pack.signal) if pack.signal else None,
                    "latency_ms": latency_ms,
                })

            except Exception as e:
                await websocket.send_json({
                    "type": "error",
                    "message": str(e),
                })

    except WebSocketDisconnect:
        pass


# ═══════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    """Initialize on server start."""
    print("=" * 60)
    print("  METEORO X — Agentic Intelligence API v2.0")
    print("  6 Capabilities | 8 Data Sources | Real-time")
    print("=" * 60)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("meteoro_api:app", host="0.0.0.0", port=port, reload=False)
