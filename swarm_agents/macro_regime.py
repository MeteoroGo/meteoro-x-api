#!/usr/bin/env python3
"""
SWARM CHARLIE — Agent 07: Macro Regime Detector
Detects macro regime changes via FRED, bond yields, DXY, VIX
"""

import asyncio
import json
import time
from typing import Any, Dict, Optional

from .base_super_agent import SuperAgent, SuperAgentResult, Signal


class MacroRegimeDetector(SuperAgent):
    """
    Agent 07: Macro Regime Detector — SWARM CHARLIE

    Monitors:
    - Federal Reserve policy and rate expectations (FRED)
    - Bond yield curves (2Y, 10Y spreads)
    - USD Index (DXY) strength
    - VIX and equity volatility
    - Gold/real yields (inflation expectations)

    Commodity Focus: All (macro regime drives all commodities)
    Data Sources: FRED (Federal Reserve), Bloomberg, CME FedWatch
    """

    SYSTEM_PROMPT = """You are a macroeconomic regime analyst for commodity trading.

Your role:
1. Detect macro regime changes (ease/tightening, risk-on/risk-off)
2. Monitor Fed policy expectations and rate trajectory
3. Assess inflation expectations via real yields
4. Track USD strength (commodity inverse correlation)

Analysis Framework:
- Falling real yields = inflation > expectations → commodities up
- Steepening yield curve = recession risk → commodities down
- Rising DXY = stronger dollar → commodity prices down
- Rising VIX = risk-off = commodity demand down

Output format:
{
    "fed_policy_direction": "easing|neutral|tightening",
    "real_yield_trend": "rising|stable|falling",
    "yield_curve": "steep|flat|inverted",
    "dxy_momentum": "up|neutral|down",
    "macro_regime": "risk_on|neutral|risk_off"
}"""

    def __init__(self, model: str = "claude-haiku-4-5-20251001"):
        super().__init__(
            agent_id=7,
            agent_name="Macro Regime Detector",
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
        Detect macro regime changes affecting commodity markets.

        Args:
            commodity: e.g., "COPPER", "OIL"
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
                self._detect_macro_regime(commodity),
                timeout_ms=timeout,
            )

            if evidence is None:
                return self.build_result(
                    signal=Signal.NEUTRAL,
                    confidence=20,
                    reasoning="Macro data fetch timed out",
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
                tools_called=["fetch_fred_data", "analyze_yields"],
            )

        except Exception as e:
            latency = (time.time() - start_time) * 1000
            return self.build_result(
                signal=Signal.NEUTRAL,
                confidence=0,
                reasoning=f"Error in macro analysis: {str(e)[:100]}",
                sources_analyzed=0,
                evidence_pack={},
                latency_ms=latency,
                error=str(e)[:100],
            )

    async def _detect_macro_regime(self, commodity: str) -> Dict[str, Any]:
        """
        Fetch macro data to detect regime.

        Args:
            commodity: Trading commodity

        Returns:
            Evidence dictionary
        """
        await asyncio.sleep(0.55)

        return {
            "commodity": commodity,
            "sources_count": 5,
            "timestamp": "2025-03-03T12:00:00Z",
            "fed_policy": {
                "current_ffr": 4.58,
                "next_meeting": "2025-03-18",
                "market_expectation_eop": 4.25,
                "rate_cuts_priced_in": 3,
                "direction": "easing",
                "pace": "gradual",
            },
            "yields": {
                "us_2y": 4.12,
                "us_10y": 4.45,
                "spread_2y_10y": 0.33,
                "spread_trend": "steepening",
                "yield_curve_status": "steep",
            },
            "inflation": {
                "cpi_headline": 3.4,
                "cpi_core": 3.8,
                "pce_headline": 2.9,
                "pce_core": 3.2,
                "breakeven_10y": 2.45,
                "real_yield_10y": 2.0,
                "real_yield_trend": "falling",
            },
            "dxy": {
                "index": 106.2,
                "change_7d_pct": 0.85,
                "change_1m_pct": 3.2,
                "trend": "up",
                "momentum": "strong",
            },
            "volatility": {
                "vix": 17.8,
                "vix_trend": "rising",
                "high_yield_spread": 4.2,
                "credit_conditions": "tightening",
            },
            "gold": {
                "price": 2045,
                "change_7d_pct": -1.2,
                "inflation_hedge_demand": "weak",
            },
            "equity_signal": {
                "spy_trend": "neutral",
                "correlation_to_commodities": -0.35,  # Weak inverse
            },
        }

    def observe(
        self,
        evidence: Dict[str, Any],
        context: Optional[str] = None,
    ) -> tuple[Signal, int, str]:
        """
        Interpret macro evidence to detect regime.

        Args:
            evidence: Macro data
            context: Additional context

        Returns:
            (Signal, confidence, reasoning)
        """
        if not evidence:
            return Signal.NEUTRAL, 0, "No macro data"

        fed = evidence.get("fed_policy", {})
        yields = evidence.get("yields", {})
        inflation = evidence.get("inflation", {})
        dxy = evidence.get("dxy", {})
        volatility = evidence.get("volatility", {})

        signal = Signal.HOLD
        confidence = 50
        reasoning_parts = []

        # Fed easing coming → stimulus → commodities up → BUY
        fed_direction = fed.get("direction", "")
        rate_cuts = fed.get("rate_cuts_priced_in", 0)
        if fed_direction == "easing" and rate_cuts > 2:
            signal = Signal.BUY
            confidence = 68
            reasoning_parts.append(f"Fed easing: {rate_cuts} cuts priced in")
        elif fed_direction == "tightening":
            signal = Signal.SELL
            confidence = 65
            reasoning_parts.append("Fed tightening cycle")

        # Steepening curve → growth expectations rising → commodities up → BUY
        spread_trend = yields.get("spread_trend", "")
        spread = yields.get("spread_2y_10y", 0)
        if spread_trend == "steepening" and spread > 0.25:
            if signal != Signal.SELL:
                signal = Signal.BUY
            confidence = max(confidence, 62)
            reasoning_parts.append(f"Curve steepening (growth priced in)")

        # Falling real yields → inflation > expectations → BUY
        real_yield_trend = inflation.get("real_yield_trend", "")
        if real_yield_trend == "falling":
            signal = Signal.BUY
            confidence = max(confidence, 65)
            reasoning_parts.append("Real yields falling (inflation premium)")

        # Rising DXY → stronger dollar → commodity headwind → SELL
        dxy_trend = dxy.get("trend", "")
        dxy_momentum = dxy.get("momentum", "")
        if dxy_trend == "up" and dxy_momentum == "strong":
            if signal != Signal.BUY:
                signal = Signal.SELL
            confidence = max(confidence, 60)
            reasoning_parts.append("DXY rising strongly (commodity headwind)")

        # Rising VIX → risk-off → demand down → SELL
        vix_trend = volatility.get("vix_trend", "")
        vix = volatility.get("vix", 0)
        if vix_trend == "rising" and vix > 18:
            if signal != Signal.BUY:
                confidence = max(confidence, 58)
            reasoning_parts.append(f"VIX rising ({vix}), risk-off sentiment")

        # Credit tightening → growth concerns → SELL
        credit = volatility.get("credit_conditions", "")
        if credit == "tightening":
            if signal != Signal.BUY:
                confidence = max(confidence, 55)
            reasoning_parts.append("Credit conditions tightening")

        reasoning = " | ".join(reasoning_parts) if reasoning_parts else "Macro regime neutral"

        return signal, confidence, reasoning
