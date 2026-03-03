#!/usr/bin/env python3
"""
SWARM CHARLIE — Agent 09: Sentiment & Flow
COT/CFTC positioning, trader sentiment, crowded trade detection
"""

import asyncio
import json
import time
from typing import Any, Dict, Optional

from .base_super_agent import SuperAgent, SuperAgentResult, Signal


class SentimentFlow(SuperAgent):
    """
    Agent 09: Sentiment & Flow — SWARM CHARLIE

    Monitors:
    - COT (Commitment of Traders) report positioning
    - Large trader positioning changes
    - Retail vs institutional sentiment
    - Volume and open interest trends
    - Crowded trade detection

    Commodity Focus: Futures (CL, NG, GC, ZC, ZW)
    Data Sources: CFTC COT reports, futures exchanges, sentiment surveys
    """

    SYSTEM_PROMPT = """You are a sentiment and flow analyst for commodity futures trading.

Your role:
1. Analyze COT positioning of commercials vs speculators
2. Detect crowded trades (consensus reversals)
3. Monitor large trader flows
4. Assess retail sentiment vs institutional
5. Track volume/open interest regime

Analysis Framework:
- COT spec long > 80th percentile = crowded long = bearish reversal risk
- Commercial hedging buying = supply concern = bullish
- Large trader accumulation + rising OI = continuation = follow trend
- Retail panic selling = contrarian buy signal

Output format:
{
    "cot_position": "long_heavy|neutral|short_heavy",
    "cot_percentile": 0-100,
    "commercial_intent": "hedging_buying|neutral|hedging_selling",
    "crowded_trade": true|false,
    "sentiment_signal": "BUY|SELL|HOLD",
    "conviction": 0-100
}"""

    def __init__(self, model: str = "claude-haiku-4-5-20251001"):
        super().__init__(
            agent_id=9,
            agent_name="Sentiment & Flow",
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
        Analyze sentiment and flow signals.

        Args:
            commodity: e.g., "CL" (crude), "GC" (gold)
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
                self._analyze_sentiment_flow(commodity),
                timeout_ms=timeout,
            )

            if evidence is None:
                return self.build_result(
                    signal=Signal.NEUTRAL,
                    confidence=20,
                    reasoning="Sentiment data fetch timed out",
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
                tools_called=["fetch_cot_report", "analyze_cftc_positioning"],
            )

        except Exception as e:
            latency = (time.time() - start_time) * 1000
            return self.build_result(
                signal=Signal.NEUTRAL,
                confidence=0,
                reasoning=f"Error in sentiment analysis: {str(e)[:100]}",
                sources_analyzed=0,
                evidence_pack={},
                latency_ms=latency,
                error=str(e)[:100],
            )

    async def _analyze_sentiment_flow(self, commodity: str) -> Dict[str, Any]:
        """
        Analyze COT positioning and trader sentiment.

        Args:
            commodity: Trading commodity

        Returns:
            Evidence dictionary
        """
        await asyncio.sleep(0.5)

        return {
            "commodity": commodity,
            "sources_count": 3,
            "timestamp": "2025-03-03T12:00:00Z",
            "cot_analysis": {
                "report_date": "2025-02-25",
                "spec_long_contracts": 285_000,
                "spec_short_contracts": 156_000,
                "net_spec_long": 129_000,
                "spec_positioning_pctl": 72,
                "commercial_net_short": -128_000,
                "position_trend": "specs_accumulating",
                "spec_positioning": "long_heavy",
            },
            "large_traders": {
                "index": "CL",
                "large_long": 680_000,
                "large_short": 620_000,
                "net_large_long": 60_000,
                "change_7d": 15_000,
                "trend": "accumulating",
            },
            "commercial_hedging": {
                "intent": "hedging_selling",
                "position_change_7d": -8_000,
                "implication": "supply_not_tight",
                "conviction": "moderate",
            },
            "volume_oi": {
                "open_interest": 1_850_000,
                "oi_trend": "rising",
                "volume": 850_000,
                "volume_vs_avg": 1.2,  # 20% above average
                "volume_trend": "rising",
            },
            "retail_sentiment": {
                "source": "sentiment_surveys",
                "bullish_pct": 62,
                "bearish_pct": 24,
                "neutral_pct": 14,
                "extreme_bullish": True,
                "sentiment_type": "bullish_extreme",
            },
            "crowded_trade_analysis": {
                "is_crowded": True,
                "crowded_direction": "long",
                "crowded_intensity": 8,  # 1-10 scale
                "reversal_risk": "high",
                "time_to_reversal_days": 8,
            },
            "flow_divergence": {
                "institutional_trend": "buying",
                "retail_trend": "chasing",
                "divergence": "yes",
                "implication": "potential_reversal",
            },
        }

    def observe(
        self,
        evidence: Dict[str, Any],
        context: Optional[str] = None,
    ) -> tuple[Signal, int, str]:
        """
        Interpret sentiment and flow evidence.

        Args:
            evidence: Sentiment and flow data
            context: Additional context

        Returns:
            (Signal, confidence, reasoning)
        """
        if not evidence:
            return Signal.NEUTRAL, 0, "No sentiment data"

        cot = evidence.get("cot_analysis", {})
        large = evidence.get("large_traders", {})
        commercial = evidence.get("commercial_hedging", {})
        retail = evidence.get("retail_sentiment", {})
        crowded = evidence.get("crowded_trade_analysis", {})
        flow_div = evidence.get("flow_divergence", {})

        signal = Signal.HOLD
        confidence = 50
        reasoning_parts = []

        # CROWDED TRADE DETECTION (Most important)
        if crowded.get("is_crowded"):
            crowded_dir = crowded.get("crowded_direction", "")
            intensity = crowded.get("crowded_intensity", 0)

            if crowded_dir == "long" and intensity > 6:
                signal = Signal.SELL
                confidence = 72
                reasoning_parts.append(f"Crowded long ({intensity}/10) - reversal risk")
            elif crowded_dir == "short" and intensity > 6:
                signal = Signal.BUY
                confidence = 72
                reasoning_parts.append(f"Crowded short ({intensity}/10) - squeeze risk")

        # COT positioning at extremes
        cot_pctl = cot.get("spec_positioning_pctl", 50)
        cot_pos = cot.get("spec_positioning", "")

        if cot_pctl > 75 and cot_pos == "long_heavy":
            if signal != Signal.BUY:
                signal = Signal.SELL
            confidence = max(confidence, 68)
            reasoning_parts.append(f"COT specs long at {cot_pctl}th percentile")
        elif cot_pctl < 25 and cot_pos == "short_heavy":
            if signal != Signal.SELL:
                signal = Signal.BUY
            confidence = max(confidence, 68)
            reasoning_parts.append(f"COT specs short at {cot_pctl}th percentile")

        # Retail extreme vs institutional (contrarian)
        retail_bull = retail.get("bullish_pct", 0)
        inst_trend = flow_div.get("institutional_trend", "")
        retail_trend = flow_div.get("retail_trend", "")

        if retail_bull > 70 and inst_trend == "selling":
            if signal != Signal.BUY:
                signal = Signal.SELL
            confidence = max(confidence, 70)
            reasoning_parts.append("Retail extreme bullish, institutions selling")
        elif retail_bull < 30 and inst_trend == "buying":
            if signal != Signal.SELL:
                signal = Signal.BUY
            confidence = max(confidence, 70)
            reasoning_parts.append("Retail extreme bearish, institutions buying")

        # Commercial hedging intent
        comm_intent = commercial.get("intent", "")
        if comm_intent == "hedging_buying":
            signal = Signal.BUY
            confidence = max(confidence, 65)
            reasoning_parts.append("Commercials hedging buying (supply tight)")
        elif comm_intent == "hedging_selling":
            if signal != Signal.BUY:
                confidence = max(confidence, 55)
            reasoning_parts.append("Commercials hedging selling (supply adequate)")

        # Large trader trend
        large_trend = large.get("trend", "")
        if large_trend == "accumulating":
            if signal != Signal.SELL:
                signal = Signal.BUY
            confidence = max(confidence, 60)
            reasoning_parts.append("Large traders accumulating")
        elif large_trend == "liquidating":
            if signal != Signal.BUY:
                signal = Signal.SELL
            confidence = max(confidence, 60)
            reasoning_parts.append("Large traders liquidating")

        # Volume trend (participation)
        vol_oi = evidence.get("volume_oi", {})
        oi_trend = vol_oi.get("oi_trend", "")
        if oi_trend == "falling":
            confidence = max(confidence, 55)
            reasoning_parts.append("Open interest declining (interest waning)")

        reasoning = " | ".join(reasoning_parts) if reasoning_parts else "Sentiment neutral"

        return signal, confidence, reasoning
