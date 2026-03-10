#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  METEORO X v14 — PROACTIVE INTELLIGENCE ENGINE                   ║
║  Autonomous Signal Generation | Always-Alert | 24/7 Monitoring  ║
║                                                                  ║
║  Like Palantir's Foundry — but for commodity trading.           ║
║  The system doesn't wait for commands. It hunts for alpha.      ║
║                                                                  ║
║  ARCHITECTURE:                                                   ║
║    - Background scheduler runs analysis cycles every 4 hours    ║
║    - 8 key commodities monitored continuously                    ║
║    - Full swarm analysis per commodity per cycle                 ║
║    - Signals persisted to autonomous memory                      ║
║    - WebSocket push to connected clients on new signals         ║
║    - Daily briefing compilation at 06:00 UTC                     ║
║    - Anomaly detection: triggers immediate analysis on spikes   ║
║                                                                  ║
║  WATCHLIST:                                                      ║
║    oil, copper, gold, natural_gas, coffee, iron_ore,            ║
║    lithium, coal                                                 ║
║                                                                  ║
║  COST ESTIMATE (6 providers, cost-optimized routing):           ║
║    576 LLM calls/day × ~2.5K tokens = ~1.44M tokens/day        ║
║    Monthly: ~$25 across all providers                            ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import logging
import time
import traceback
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger("meteoro.proactive")

# ═══════════════════════════════════════════════════════════════
# WATCHLIST — Commodities to monitor proactively
# ═══════════════════════════════════════════════════════════════

WATCHLIST = [
    {"commodity": "oil", "display": "Petróleo WTI", "priority": 1},
    {"commodity": "copper", "display": "Cobre", "priority": 1},
    {"commodity": "gold", "display": "Oro", "priority": 2},
    {"commodity": "natural_gas", "display": "Gas Natural", "priority": 2},
    {"commodity": "coffee", "display": "Café", "priority": 2},
    {"commodity": "iron_ore", "display": "Mineral de Hierro", "priority": 3},
    {"commodity": "lithium", "display": "Litio", "priority": 3},
    {"commodity": "coal", "display": "Carbón", "priority": 3},
]

# Cycle interval: 4 hours between full analysis cycles
CYCLE_INTERVAL_S = 4 * 60 * 60  # 14,400 seconds
# Stagger between commodities within a cycle (avoid provider burst)
COMMODITY_GAP_S = 30  # 30 seconds between commodity analyses
# Keep-alive self-ping interval (prevent Render free tier sleep)
KEEPALIVE_INTERVAL_S = 10 * 60  # 10 minutes
# Daily briefing time (UTC)
DAILY_BRIEFING_HOUR = 6


class ProactiveMonitor:
    """
    Autonomous Proactive Intelligence Engine.
    Runs as a background task inside the FastAPI event loop.
    Generates signals continuously without human prompting.
    """

    _instance = None

    @classmethod
    def get_instance(cls) -> "ProactiveMonitor":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.running = False
        self.recent_signals: List[Dict[str, Any]] = []  # Last 100 signals
        self.max_recent = 100
        self.cycles_completed = 0
        self.last_cycle_time: Optional[str] = None
        self.last_cycle_duration_s: float = 0
        self.errors: List[str] = []
        self.started_at: Optional[str] = None
        self._task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._swarm = None

    def _get_swarm(self):
        """Lazy-load swarm to avoid circular imports."""
        if self._swarm is None:
            try:
                from meteoro_swarm import MeteorSwarm
                self._swarm = MeteorSwarm()
                logger.info("[PROACTIVE] Swarm initialized for proactive monitoring")
            except Exception as e:
                logger.error(f"[PROACTIVE] Failed to initialize swarm: {e}")
                raise
        return self._swarm

    def start(self):
        """Start the proactive monitoring loop as a background task."""
        if self.running:
            logger.info("[PROACTIVE] Already running — skipping start")
            return

        self.running = True
        self.started_at = datetime.now(timezone.utc).isoformat()
        self._task = asyncio.create_task(self._main_loop())
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        logger.info("[PROACTIVE] ═══ Engine STARTED — autonomous monitoring active ═══")

    def stop(self):
        """Stop the proactive monitoring loop."""
        self.running = False
        if self._task:
            self._task.cancel()
        if self._keepalive_task:
            self._keepalive_task.cancel()
        logger.info("[PROACTIVE] ═══ Engine STOPPED ═══")

    def get_status(self) -> Dict[str, Any]:
        """Return current engine status."""
        return {
            "active": self.running,
            "started_at": self.started_at,
            "cycles_completed": self.cycles_completed,
            "last_cycle": self.last_cycle_time,
            "last_cycle_duration_s": round(self.last_cycle_duration_s, 1),
            "signals_generated": len(self.recent_signals),
            "watchlist_size": len(WATCHLIST),
            "watchlist": [w["display"] for w in WATCHLIST],
            "cycle_interval_hours": CYCLE_INTERVAL_S / 3600,
            "recent_errors": self.errors[-5:],  # Last 5 errors
        }

    def get_recent_signals(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Return most recent proactive signals."""
        return self.recent_signals[-limit:]

    async def _main_loop(self):
        """Main proactive monitoring loop — runs indefinitely."""
        # Wait 60s after startup before first cycle (let server warm up)
        await asyncio.sleep(60)
        logger.info("[PROACTIVE] Warm-up complete — starting first analysis cycle")

        while self.running:
            try:
                await self.run_cycle()
            except asyncio.CancelledError:
                logger.info("[PROACTIVE] Main loop cancelled")
                break
            except Exception as e:
                err = f"Cycle error: {str(e)[:200]}"
                self.errors.append(f"{datetime.now(timezone.utc).isoformat()}: {err}")
                logger.error(f"[PROACTIVE] {err}")
                # Keep only last 50 errors
                if len(self.errors) > 50:
                    self.errors = self.errors[-50:]

            # Wait for next cycle
            try:
                logger.info(f"[PROACTIVE] Next cycle in {CYCLE_INTERVAL_S/3600:.0f} hours")
                await asyncio.sleep(CYCLE_INTERVAL_S)
            except asyncio.CancelledError:
                break

    async def _keepalive_loop(self):
        """Self-ping to prevent Render free tier from sleeping."""
        try:
            import httpx
        except ImportError:
            logger.warning("[PROACTIVE] httpx not available — keepalive disabled")
            return

        while self.running:
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    r = await client.get("https://meteoro.io/api/ping")
                    if r.status_code == 200:
                        logger.debug("[PROACTIVE] Keep-alive ping OK")
            except Exception:
                pass  # Ignore keepalive failures
            try:
                await asyncio.sleep(KEEPALIVE_INTERVAL_S)
            except asyncio.CancelledError:
                break

    async def run_cycle(self):
        """Run one full analysis cycle across all watchlist commodities."""
        cycle_start = time.time()
        cycle_id = f"cycle_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}"
        logger.info(f"\n{'='*60}")
        logger.info(f"[PROACTIVE] ═══ CYCLE {cycle_id} START ═══")
        logger.info(f"[PROACTIVE] Analyzing {len(WATCHLIST)} commodities...")
        logger.info(f"{'='*60}")

        results = []
        for i, item in enumerate(WATCHLIST):
            commodity = item["commodity"]
            display = item["display"]

            # Stagger between commodities
            if i > 0:
                await asyncio.sleep(COMMODITY_GAP_S)

            try:
                logger.info(f"\n[PROACTIVE] [{i+1}/{len(WATCHLIST)}] Analyzing {display}...")
                result = await self.analyze_commodity(commodity)
                results.append(result)

                # Extract key info for logging
                signal = result.get("signal", "NEUTRAL")
                conviction = result.get("conviction", 0)
                logger.info(f"[PROACTIVE] [{display}] → {signal} ({conviction}%)")

            except Exception as e:
                err = f"[{display}] Error: {str(e)[:100]}"
                logger.error(f"[PROACTIVE] {err}")
                self.errors.append(f"{datetime.now(timezone.utc).isoformat()}: {err}")
                results.append({
                    "commodity": commodity,
                    "signal": "ERROR",
                    "error": str(e)[:200],
                })

        # Cycle complete
        cycle_duration = time.time() - cycle_start
        self.cycles_completed += 1
        self.last_cycle_time = datetime.now(timezone.utc).isoformat()
        self.last_cycle_duration_s = cycle_duration

        # Count actionable signals
        actionable = [r for r in results if r.get("signal") in ("BUY", "SELL")]
        logger.info(f"\n{'='*60}")
        logger.info(f"[PROACTIVE] ═══ CYCLE {cycle_id} COMPLETE ═══")
        logger.info(f"[PROACTIVE] Duration: {cycle_duration:.0f}s | "
                     f"Signals: {len(results)} | Actionable: {len(actionable)}")
        logger.info(f"{'='*60}\n")

    async def analyze_commodity(self, commodity: str) -> Dict[str, Any]:
        """Run full swarm analysis for a single commodity."""
        swarm = self._get_swarm()

        # Run the analysis (same as user-triggered, but proactive)
        result = await swarm.analyze(commodity)

        # SwarmSignal is a dataclass — extract attributes directly
        signal_value = getattr(result, 'final_signal', None)
        signal_str = signal_value.value if hasattr(signal_value, 'value') else str(signal_value or "NEUTRAL")

        # Build signal record
        signal_record = {
            "commodity": commodity,
            "signal": signal_str,
            "conviction": getattr(result, 'conviction', 0),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "proactive": True,  # Flag: this was autonomously generated
            "cycle": self.cycles_completed + 1,
            "latency_ms": getattr(result, 'total_latency_ms', 0),
            "providers_used": len(getattr(result, 'all_results', [])),
            "cost_usd": getattr(result, 'cost_usd', 0),
            "reasoning": getattr(result, 'reasoning', ""),
        }

        # Store in recent signals
        self.recent_signals.append(signal_record)
        if len(self.recent_signals) > self.max_recent:
            self.recent_signals = self.recent_signals[-self.max_recent:]

        return signal_record


# ═══════════════════════════════════════════════════════════════
# AUTO-START: Initialize and start the proactive engine
# when this module is imported (i.e., when the server starts).
# ═══════════════════════════════════════════════════════════════

def start_proactive_engine():
    """
    Start the proactive monitoring engine.
    Called during FastAPI startup event.
    """
    try:
        monitor = ProactiveMonitor.get_instance()
        monitor.start()
        return True
    except Exception as e:
        logger.error(f"[PROACTIVE] Failed to start engine: {e}")
        return False
