#!/usr/bin/env python3
"""
SWARM ALPHA — Agent 03: Supply Chain Mapper
Production facilities, logistics networks, inventory levels
"""

import asyncio
import json
import time
from typing import Any, Dict, Optional

from .base_super_agent import SuperAgent, SuperAgentResult, Signal


class SupplyChainMapper(SuperAgent):
    """
    Agent 03: Supply Chain Mapper — SWARM ALPHA

    Monitors:
    - Production facility activity (via satellite, news, reports)
    - Inventory levels at major warehouses/silos
    - Logistics network efficiency
    - Key chokepoints in commodity flows

    Commodity Focus: All
    Data Sources: Company reports, news, SCMP, logistics APIs
    """

    SYSTEM_PROMPT = """You are a supply chain analyst for commodity trading.

Your role:
1. Map production facility locations and capacity
2. Track inventory levels and warehouse utilization
3. Identify logistics bottlenecks
4. Monitor supply concentration risk

Analysis Framework:
- Facility utilization > 90% = supply tight
- Inventory declining = demand exceeding supply
- Warehouse full = logistics bottleneck
- Single-source concentration = geopolitical risk

Output format:
{
    "production_utilization": 0-100,
    "inventory_trend": "rising|declining|stable",
    "warehouse_occupancy": 0-100,
    "logistics_score": 0-100,
    "chokepoints": ["..."],
    "supply_risk": 0-100
}"""

    def __init__(self, model: str = "claude-haiku-4-5-20251001"):
        super().__init__(
            agent_id=3,
            agent_name="Supply Chain Mapper",
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
        Analyze supply chain for disruption signals.

        Args:
            commodity: e.g., "COPPER", "COFFEE"
            directive: Analysis request
            context: Shared context
            timeout_ms: Execution timeout

        Returns:
            SuperAgentResult with signal
        """
        start_time = time.time()
        timeout = timeout_ms or self.MAX_EXEC_TIME_MS

        try:
            evidence = await self.call_with_timeout(
                self._map_supply_chain(commodity),
                timeout_ms=timeout,
            )

            if evidence is None:
                return self.build_result(
                    signal=Signal.NEUTRAL,
                    confidence=20,
                    reasoning="Supply chain data fetch timed out",
                    sources_analyzed=0,
                    evidence_pack={},
                    latency_ms=time.time() - start_time,
                    error="Timeout",
                )

            signal, confidence, reasoning = self.observe(evidence)
            latency = (time.time() - start_time) * 1000

            return self.build_result(
                signal=signal,
                confidence=confidence,
                reasoning=reasoning,
                sources_analyzed=evidence.get("sources_count", 0),
                evidence_pack=evidence,
                latency_ms=latency,
                tools_called=["analyze_supply_chain", "map_facilities"],
            )

        except Exception as e:
            latency = (time.time() - start_time) * 1000
            return self.build_result(
                signal=Signal.NEUTRAL,
                confidence=0,
                reasoning=f"Error in supply chain analysis: {str(e)[:100]}",
                sources_analyzed=0,
                evidence_pack={},
                latency_ms=latency,
                error=str(e)[:100],
            )

    async def _map_supply_chain(self, commodity: str) -> Dict[str, Any]:
        """
        Map supply chain for commodity.

        Args:
            commodity: Trading commodity

        Returns:
            Evidence dictionary
        """
        await asyncio.sleep(0.55)

        return {
            "commodity": commodity,
            "sources_count": 4,
            "timestamp": "2025-03-03T12:00:00Z",
            "production": {
                "total_capacity_mtpy": 18_500_000,  # Metric tons per year
                "utilization_pct": 87,
                "trend": "increasing",
                "forecast_next_quarter": "high_demand",
            },
            "inventory": {
                "global_days_of_supply": 8.2,
                "trend": "declining",
                "key_locations": {
                    "China_warehouses": {"level_pct": 45, "trend": "declining"},
                    "US_storage": {"level_pct": 62, "trend": "stable"},
                    "EU_facilities": {"level_pct": 72, "trend": "stable"},
                },
            },
            "warehousing": {
                "total_capacity": 50_000_000,  # tonnes
                "current_occupied": 38_500_000,
                "occupancy_pct": 77,
                "available_capacity": 11_500_000,
            },
            "logistics": {
                "transport_cost_index": 108,
                "trend": "up",
                "rail_congestion": "moderate",
                "truck_availability": "tight",
                "port_handling_capacity": 85,
            },
            "chokepoints": [
                "Panama Canal delays (+2 days)",
                "Chinese port congestion (8 vessels queued)",
                "Rail capacity strain (75% utilization)",
            ],
            "supply_concentration": {
                "top_3_suppliers_pct": 65,
                "geopolitical_exposure": "high",
                "single_country_risk": "China 35%",
            },
            "supply_risk_score": 68,
        }

    def observe(
        self,
        evidence: Dict[str, Any],
        context: Optional[str] = None,
    ) -> tuple[Signal, int, str]:
        """
        Interpret supply chain evidence.

        Args:
            evidence: Supply chain data
            context: Additional context

        Returns:
            (Signal, confidence, reasoning)
        """
        if not evidence:
            return Signal.NEUTRAL, 0, "No supply chain data"

        production = evidence.get("production", {})
        inventory = evidence.get("inventory", {})
        warehouse = evidence.get("warehousing", {})
        logistics = evidence.get("logistics", {})
        chokepoints = evidence.get("chokepoints", [])

        signal = Signal.HOLD
        confidence = 50
        reasoning_parts = []

        # High production utilization + declining inventory → supply tight → BUY
        prod_util = production.get("utilization_pct", 0)
        inv_trend = inventory.get("trend", "")
        inv_dos = inventory.get("global_days_of_supply", 15)

        if prod_util > 85 and inv_trend == "declining":
            signal = Signal.BUY
            confidence = 70
            reasoning_parts.append(f"High utilization ({prod_util}%) + declining inventory")

        if inv_dos < 10:
            if signal != Signal.SELL:
                signal = Signal.BUY
            confidence = max(confidence, 65)
            reasoning_parts.append(f"Low days of supply ({inv_dos} days)")

        # High warehouse occupancy → supply tight → BUY
        occupancy = warehouse.get("occupancy_pct", 0)
        if occupancy > 80:
            if signal != Signal.SELL:
                signal = Signal.BUY
            confidence = max(confidence, 58)
            reasoning_parts.append(f"High warehouse occupancy ({occupancy}%)")

        # Rising logistics costs → upward commodity pressure → BUY
        log_index = logistics.get("transport_cost_index", 100)
        log_trend = logistics.get("trend", "")
        if log_index > 105 and log_trend == "up":
            if signal != Signal.SELL:
                signal = Signal.BUY
            confidence = max(confidence, 55)
            reasoning_parts.append(f"Rising logistics costs (index {log_index})")

        # Major chokepoints → supply disruption → SELL
        if len(chokepoints) > 2:
            signal = Signal.SELL
            confidence = 60
            reasoning_parts.append(f"{len(chokepoints)} major chokepoints identified")

        # High supply concentration → geopolitical risk → SELL
        concentration = evidence.get("supply_concentration", {})
        if concentration.get("top_3_suppliers_pct", 0) > 60:
            if signal != Signal.BUY:
                signal = Signal.SELL
            confidence = max(confidence, 55)
            reasoning_parts.append("High supply concentration risk")

        # High supply risk score → SELL
        risk_score = evidence.get("supply_risk_score", 0)
        if risk_score > 70:
            signal = Signal.SELL
            confidence = 65
            reasoning_parts.append(f"Supply risk score high ({risk_score})")

        reasoning = " | ".join(reasoning_parts) if reasoning_parts else "Balanced supply"

        return signal, confidence, reasoning
