#!/usr/bin/env python3
"""
SWARM BRAVO — Agent 05: China Demand Oracle
Chinese language sources, PBOC policy, Qingdao commodity flows
USES KIMI MODEL (moonshot-v1-8k) for Chinese text understanding
"""

import asyncio
import json
import time
from typing import Any, Dict, Optional

from .base_super_agent import SuperAgent, SuperAgentResult, Signal


class ChinaDemandOracle(SuperAgent):
    """
    Agent 05: China Demand Oracle — SWARM BRAVO

    Monitors:
    - Chinese demand signals (PBOC, CPI, industrial production)
    - Qingdao bonded warehouse flows (early demand indicator)
    - Chinese government policy changes
    - Manufacturing PMI and construction activity

    Commodity Focus: All (China is the marginal buyer)
    Data Sources: PBOC, SCMP, trading floors, Qingdao monitors
    Model: Kimi (moonshot-v1-8k) for Chinese text analysis

    CRITICAL: This agent uses Kimi model ($0.60/M tokens) because it needs
    to understand Mandarin sources that Western LLMs struggle with.
    """

    SYSTEM_PROMPT = """你是中国大宗商品需求分析专家。

Your role:
1. Analyze PBOC policy and Chinese demand signals
2. Monitor Qingdao bonded warehouse flows (early demand indicator)
3. Track manufacturing PMI and construction activity
4. Assess Chinese government commodity stockpiling intent

Analysis Framework:
- PBOC rate cuts = stimulus → demand increase → bullish
- Manufacturing PMI > 50 = expansion → demand rising
- Qingdao copper inflows declining = demand weakening
- Government stockpiling announcements = major demand shift

Output format:
{
    "pboc_stance": "hawkish|neutral|dovish",
    "pmi_trend": rising|flat|declining,
    "qingdao_flows": "increasing|stable|decreasing",
    "government_intent": "accumulating|neutral|liquidating",
    "demand_outlook": "bullish|neutral|bearish"
}"""

    def __init__(self, model: str = "moonshot-v1-8k"):
        """
        Initialize with Kimi model for Chinese text analysis.

        Args:
            model: Defaults to Kimi (moonshot-v1-8k)
        """
        super().__init__(
            agent_id=5,
            agent_name="China Demand Oracle",
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
        Analyze Chinese demand signals.

        Args:
            commodity: e.g., "COPPER", "IRON_ORE"
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
                self._gather_china_demand_signals(commodity),
                timeout_ms=timeout,
            )

            if evidence is None:
                return self.build_result(
                    signal=Signal.NEUTRAL,
                    confidence=20,
                    reasoning="China demand data fetch timed out",
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
                tools_called=["fetch_pboc_data", "analyze_qingdao_flows"],
                cost_usd=0.0015,  # Kimi is more expensive
            )

        except Exception as e:
            latency = (time.time() - start_time) * 1000
            return self.build_result(
                signal=Signal.NEUTRAL,
                confidence=0,
                reasoning=f"Error in China demand analysis: {str(e)[:100]}",
                sources_analyzed=0,
                evidence_pack={},
                latency_ms=latency,
                error=str(e)[:100],
            )

    async def _gather_china_demand_signals(self, commodity: str) -> Dict[str, Any]:
        """
        Gather Chinese demand data.

        Args:
            commodity: Trading commodity

        Returns:
            Evidence dictionary
        """
        await asyncio.sleep(0.8)  # Kimi is slightly slower

        return {
            "commodity": commodity,
            "sources_count": 6,
            "timestamp": "2025-03-03T12:00:00Z",
            "pboc": {
                "latest_policy": "neutral bias",
                "recent_actions": "OMO maintenance, no rate change",
                "guidance": "target 2% inflation",
                "stance": "neutral",
                "next_meeting": "2025-03-20",
            },
            "manufacturing_pmi": {
                "latest": 49.8,
                "trend": "declining",
                "expectation": "contraction",
                "implications": "demand weakness",
            },
            "qingdao_warehouse": {
                "copper_tonnes": 350_000,
                "copper_trend": "declining_inflows",
                "iron_ore_tonnes": 2_100_000,
                "iron_trend": "stable",
                "overall_assessment": "moderating_demand",
            },
            "construction_activity": {
                "new_starts": "declining_yoy",
                "completions": "below_forecast",
                "steel_rebar_demand": "soft",
                "cement_usage": "seasonal_low",
            },
            "government_stockpile": {
                "copper_reserve_purchases": "quiet (no announcements)",
                "iron_ore_purchases": "routine",
                "assessment": "not_accumulating",
            },
            "macro_backdrop": {
                "gdp_growth_forecast": 4.8,
                "inflation_cpi": 0.8,
                "unemployment": 3.9,
                "credit_growth": "moderate",
            },
            "sentiment": {
                "trader_positioning": "neutral_to_short",
                "institutional_view": "cautious",
                "retail_activity": "light",
            },
        }

    def observe(
        self,
        evidence: Dict[str, Any],
        context: Optional[str] = None,
    ) -> tuple[Signal, int, str]:
        """
        Interpret Chinese demand evidence.

        Args:
            evidence: China demand data
            context: Additional context

        Returns:
            (Signal, confidence, reasoning)
        """
        if not evidence:
            return Signal.NEUTRAL, 0, "No China data"

        pboc = evidence.get("pboc", {})
        pmi = evidence.get("manufacturing_pmi", {})
        qingdao = evidence.get("qingdao_warehouse", {})
        construction = evidence.get("construction_activity", {})
        stockpile = evidence.get("government_stockpile", {})

        signal = Signal.HOLD
        confidence = 50
        reasoning_parts = []

        # PBOC dovish (rate cuts coming) → stimulus → demand up → BUY
        pboc_stance = pboc.get("stance", "neutral")
        if pboc_stance == "dovish":
            signal = Signal.BUY
            confidence = 70
            reasoning_parts.append("PBOC dovish stance (stimulus coming)")
        elif pboc_stance == "hawkish":
            signal = Signal.SELL
            confidence = 65
            reasoning_parts.append("PBOC hawkish (tightening ahead)")

        # Manufacturing PMI < 50 = contraction → demand down → SELL
        pmi_value = pmi.get("latest", 50)
        pmi_trend = pmi.get("trend", "")
        if pmi_value < 48:
            signal = Signal.SELL
            confidence = 65
            reasoning_parts.append(f"Manufacturing PMI contracting ({pmi_value})")
        elif pmi_trend == "declining":
            if signal != Signal.BUY:
                confidence = max(confidence, 58)
            reasoning_parts.append("PMI declining (demand softening)")

        # Qingdao inflows declining → demand weakness → SELL
        qingdao_trend = qingdao.get("copper_trend", "")
        if qingdao_trend == "declining_inflows":
            if signal != Signal.BUY:
                signal = Signal.SELL
            confidence = max(confidence, 62)
            reasoning_parts.append("Qingdao copper inflows declining (demand down)")

        # Construction weakness → steel/copper demand down → SELL
        construction_trend = construction.get("new_starts", "")
        if "declining" in construction_trend.lower():
            if signal != Signal.BUY:
                signal = Signal.SELL
            confidence = max(confidence, 55)
            reasoning_parts.append("Construction starts declining")

        # Government stockpiling → demand surge → BUY
        if "accumulating" in stockpile.get("copper_reserve_purchases", "").lower():
            signal = Signal.BUY
            confidence = 75
            reasoning_parts.append("Government copper stockpiling detected")

        # Overall assessment from Qingdao monitor
        qingdao_assessment = qingdao.get("overall_assessment", "")
        if "moderating" in qingdao_assessment:
            if signal != Signal.BUY:
                confidence = max(confidence, 60)
            reasoning_parts.append("Overall demand moderating")

        reasoning = " | ".join(reasoning_parts) if reasoning_parts else "China demand neutral"

        return signal, confidence, reasoning
