#!/usr/bin/env python3
"""
SWARM ALPHA — Agent 02: Maritime Intel
Vessel tracking, port congestion, shipping route anomalies via AIS data
"""

import asyncio
import json
import time
from typing import Any, Dict, Optional

from .base_super_agent import SuperAgent, SuperAgentResult, Signal


class MaritimeIntel(SuperAgent):
    """
    Agent 02: Maritime Intel — SWARM ALPHA

    Monitors:
    - AIS (Automatic Identification System) vessel positions
    - Port congestion and dwell times
    - Shipping route anomalies
    - Commodity vessel counts and tonnage

    Commodity Focus: All commodities (shipping is universal bottleneck)
    Data Sources: MarineTraffic, Spire, AIS feeds
    """

    SYSTEM_PROMPT = """You are a maritime intelligence specialist for commodity trading.

Your role:
1. Analyze AIS vessel tracking data
2. Monitor port congestion: dwell times, queue lengths, anchored vessels
3. Detect shipping route anomalies: delays, diversions, blockades
4. Track commodity vessel volume: count and tonnage trends

Analysis Framework:
- Dwell time increase (>50% above baseline) = bottleneck, upward pressure
- Anchored vessels in port = congestion risk
- Route deviations = geopolitical/weather disruption
- Empty vessel repositioning = supply shortage signals

Output format:
{
    "vessels": {"total": N, "anchored": N, "in_transit": N},
    "port_dwell_days": 5.2,
    "port_congestion_level": 0-100,
    "route_anomalies": ["..."],
    "shipping_cost_implication": "bullish|bearish|neutral",
    "confidence": 0-100
}"""

    def __init__(self, model: str = "claude-haiku-4-5-20251001"):
        super().__init__(
            agent_id=2,
            agent_name="Maritime Intel",
            system_prompt=self.SYSTEM_PROMPT,
            model=model,
        )

    async def process(
        self,
        commodity: str,
        directive: str,
        context: Optional[Dict[str, Any]] = None,
        timeout_ms: Optional[int] = None,
    ) -> SuperAgentResult:
        """
        Analyze maritime shipping data for supply chain signals.

        Args:
            commodity: e.g., "COPPER", "OIL", "LNG"
            directive: Analysis request
            context: Shared context
            timeout_ms: Execution timeout

        Returns:
            SuperAgentResult with signal
        """
        start_time = time.time()
        timeout = timeout_ms or self.MAX_EXEC_TIME_MS

        try:
            # ACT: Fetch AIS and port data
            evidence = await self.call_with_timeout(
                self._fetch_maritime_data(commodity),
                timeout_ms=timeout,
            )

            if evidence is None:
                return self.build_result(
                    signal=Signal.NEUTRAL,
                    confidence=20,
                    reasoning="Maritime data fetch timed out",
                    sources_analyzed=0,
                    evidence_pack={},
                    latency_ms=time.time() - start_time,
                    error="Timeout",
                )

            # OBSERVE: Extract signal
            signal, confidence, reasoning = self.observe(evidence)

            latency = (time.time() - start_time) * 1000

            return self.build_result(
                signal=signal,
                confidence=confidence,
                reasoning=reasoning,
                sources_analyzed=evidence.get("sources_count", 0),
                evidence_pack=evidence,
                latency_ms=latency,
                tools_called=["fetch_ais_data", "analyze_port_congestion"],
            )

        except Exception as e:
            latency = (time.time() - start_time) * 1000
            return self.build_result(
                signal=Signal.NEUTRAL,
                confidence=0,
                reasoning=f"Error in maritime analysis: {str(e)[:100]}",
                sources_analyzed=0,
                evidence_pack={},
                latency_ms=latency,
                error=str(e)[:100],
            )

    async def _fetch_maritime_data(self, commodity: str) -> Dict[str, Any]:
        """
        Fetch AIS and port data for commodity shipping.

        Args:
            commodity: Trading commodity

        Returns:
            Evidence dictionary with maritime findings
        """
        await asyncio.sleep(0.6)  # Simulate API latency

        # Simulate realistic maritime data
        port_congestion_base = 45
        dwell_baseline = 4.5

        # Commodity-specific patterns
        if commodity in ["OIL", "LNG"]:
            port_congestion_base = 62
            dwell_baseline = 6.2
        elif commodity in ["COPPER"]:
            port_congestion_base = 38
            dwell_baseline = 3.8

        return {
            "commodity": commodity,
            "sources_count": 3,  # MarineTraffic, Spire, AIS
            "timestamp": "2025-03-03T12:00:00Z",
            "vessels": {
                "total_count": 342,
                "anchored_count": 18,
                "in_transit": 324,
                "empty_repositioning": 45,  # Supply signal
            },
            "port_data": {
                "primary_ports": ["Shanghai", "Singapore", "Rotterdam"],
                "avg_dwell_days": dwell_baseline + 1.2,  # Increasing
                "dwell_trend": "increasing",
                "queue_length": 8,  # Vessels waiting
                "congestion_level": port_congestion_base + 15,
            },
            "route_analysis": {
                "normal_routes": 8,
                "diverted_routes": 2,
                "anomalies": [
                    "Unexpected Suez diversions (6 vessels)",
                    "Panama Canal delays (+2 days average)",
                ],
            },
            "shipping_rates": {
                "timecharter_rate_change_pct": 8.5,  # Rising = supply tight
                "spot_rate_trend": "up",
            },
            "empty_vessel_repositioning": {
                "count": 45,
                "primary_direction": "to_supplier_regions",
                "implication": "supply shortage signals",
            },
        }

    def observe(
        self,
        evidence: Dict[str, Any],
        context: Optional[str] = None,
    ) -> tuple[Signal, int, str]:
        """
        Interpret maritime evidence into trading signal.

        Args:
            evidence: Maritime data
            context: Additional context

        Returns:
            (Signal, confidence, reasoning)
        """
        if not evidence:
            return Signal.NEUTRAL, 0, "No maritime data"

        vessels = evidence.get("vessels", {})
        port = evidence.get("port_data", {})
        routes = evidence.get("route_analysis", {})
        shipping = evidence.get("shipping_rates", {})

        signal = Signal.HOLD
        confidence = 50
        reasoning_parts = []

        # Port congestion → upward pressure → BUY
        congestion = port.get("congestion_level", 0)
        if congestion > 70:
            signal = Signal.BUY
            confidence = 65
            reasoning_parts.append(f"High port congestion ({congestion}%)")
        elif congestion > 55:
            if signal == Signal.HOLD:
                signal = Signal.BUY
            confidence = max(confidence, 55)
            reasoning_parts.append("Port congestion elevated")

        # Increasing dwell time → supply tightness → BUY
        dwell_trend = port.get("dwell_trend", "")
        if dwell_trend == "increasing":
            if signal != Signal.SELL:
                confidence = max(confidence, 60)
            reasoning_parts.append("Dwell time increasing (supply risk)")

        # Route anomalies → supply disruption → SELL
        anomalies = routes.get("anomalies", [])
        if len(anomalies) > 0:
            signal = Signal.SELL
            confidence = 58
            reasoning_parts.append(f"{len(anomalies)} major route disruptions")

        # Rising shipping rates → upward commodity pressure → BUY
        rate_change = shipping.get("timecharter_rate_change_pct", 0)
        if rate_change > 5:
            if signal != Signal.SELL:
                signal = Signal.BUY
            confidence = max(confidence, 62)
            reasoning_parts.append(f"Shipping rates up {rate_change}%")

        # Empty vessel repositioning to suppliers → shortage signal → BUY
        empty_repos = evidence.get("empty_vessel_repositioning", {})
        if empty_repos.get("count", 0) > 40:
            if signal != Signal.SELL:
                signal = Signal.BUY
            confidence = max(confidence, 60)
            reasoning_parts.append("Heavy empty repositioning to suppliers")

        # Anchored vessels (waiting to unload) → bearish → SELL
        anchored = vessels.get("anchored_count", 0)
        if anchored > 15:
            signal = Signal.SELL
            confidence = 55
            reasoning_parts.append(f"High anchored vessel count ({anchored})")

        reasoning = " | ".join(reasoning_parts) if reasoning_parts else "No clear signal"

        return signal, confidence, reasoning
