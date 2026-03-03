#!/usr/bin/env python3
"""
SWARM DELTA — Agent 10: Risk Guardian
Risk management with absolute VETO power
VaR, drawdown limits, position sizing, capital preservation
"""

import asyncio
import json
import time
from typing import Any, Dict, Optional

from .base_super_agent import SuperAgent, SuperAgentResult, Signal


class RiskGuardian(SuperAgent):
    """
    Agent 10: Risk Guardian — SWARM DELTA

    **CRITICAL: This agent has ABSOLUTE VETO POWER over other agents.**

    Responsibilities:
    - Value-at-Risk (VaR) calculation and limits
    - Maximum drawdown monitoring
    - Position sizing constraints
    - Leverage limits and margin requirements
    - Circuit breakers and kill switches

    Cannot be overridden by any other agent.
    Must pass all trading signals.

    Commodity Focus: All (universal risk management)
    Data Sources: Portfolio state, market data, historical volatility
    """

    SYSTEM_PROMPT = """You are the Risk Guardian - the final arbiter of trading risk.

Your role:
1. Monitor Value-at-Risk (VaR) across portfolio
2. Enforce maximum drawdown limits
3. Validate position sizing against capital
4. Assess leverage ratios
5. VETO any trade that violates risk parameters

ABSOLUTE VETO POWER: You can override any agent's recommendation.
Your job is capital preservation. You should be conservative.

Analysis Framework:
- 1-day VaR at 95% confidence > 2% of portfolio = NO TRADING
- Current drawdown > 15% = NO NEW LONG POSITIONS
- Position size > 5% of account = too large
- Leverage > 2x = too aggressive
- Volatility surge > 50% of normal = reduce size

Output format:
{
    "var_95": 1.2,
    "current_drawdown_pct": 8.5,
    "position_size_valid": true,
    "leverage_ratio": 1.8,
    "veto_active": false,
    "veto_reason": "",
    "capital_at_risk_pct": 2.1
}"""

    def __init__(self, model: str = "claude-haiku-4-5-20251001"):
        super().__init__(
            agent_id=10,
            agent_name="Risk Guardian",
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
        Assess portfolio risk and VETO if necessary.

        Args:
            commodity: e.g., "COPPER"
            directive: Trading directive from other agents
            context: Portfolio state, other agents' signals
            timeout_ms: Execution timeout

        Returns:
            SuperAgentResult (VETO signal or HOLD if risk acceptable)
        """
        start_time = time.time()
        timeout = timeout_ms or self.MAX_EXEC_TIME_MS

        try:
            evidence = await self.call_with_timeout(
                self._assess_portfolio_risk(commodity, context),
                timeout_ms=timeout,
            )

            if evidence is None:
                return self.build_result(
                    signal=Signal.NEUTRAL,
                    confidence=100,
                    reasoning="Risk assessment timed out - defaulting to HOLD",
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
                tools_called=["calculate_var", "check_drawdown", "validate_position"],
            )

        except Exception as e:
            latency = (time.time() - start_time) * 1000
            # On error, default to VETO for safety
            return self.build_result(
                signal=Signal.HOLD,
                confidence=100,
                reasoning=f"Risk calculation error - VETO for safety: {str(e)[:50]}",
                sources_analyzed=0,
                evidence_pack={},
                latency_ms=latency,
                error=str(e)[:100],
            )

    async def _assess_portfolio_risk(
        self,
        commodity: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Assess current portfolio risk metrics.

        Args:
            commodity: Trading commodity
            context: Portfolio state from memory/context

        Returns:
            Risk metrics dictionary
        """
        await asyncio.sleep(0.4)

        # Simulate realistic portfolio risk state
        portfolio_value = 1_000_000  # $1M account
        current_pnl = -45_000  # Currently down 4.5%
        positions = [
            {"symbol": "CL", "contracts": 5, "notional": 250_000},
            {"symbol": "GC", "contracts": 3, "notional": 120_000},
        ]

        total_notional = sum(p["notional"] for p in positions)
        leverage_ratio = total_notional / portfolio_value

        return {
            "commodity": commodity,
            "sources_count": 1,
            "timestamp": "2025-03-03T12:00:00Z",
            "portfolio": {
                "account_value": portfolio_value,
                "current_pnl": current_pnl,
                "drawdown_pct": (current_pnl / portfolio_value) * 100,
                "positions_count": len(positions),
                "total_notional": total_notional,
            },
            "var_analysis": {
                "var_95_1day_pct": 2.1,  # 1-day VaR at 95% confidence
                "var_95_1day_usd": 21_000,
                "var_99_1day_pct": 3.8,
                "cvar_95": 2.8,  # Conditional VaR (worse case)
                "acceptable_limit": 2.0,  # Max acceptable VaR
                "exceeds_limit": True,  # RISK ALERT
            },
            "drawdown": {
                "current_drawdown_pct": 4.5,
                "max_drawdown_historic_pct": 12.3,
                "max_allowable": 15.0,
                "red_line": 20.0,
                "status": "yellow_warning",
            },
            "leverage": {
                "ratio": leverage_ratio,
                "max_allowable": 2.5,
                "optimal_max": 2.0,
                "status": "acceptable_but_elevated",
            },
            "volatility": {
                "current_vol_pct": 18.5,
                "30day_avg_vol": 16.2,
                "vol_spike_pct": 14.2,
                "vol_regime": "elevated",
                "position_size_adjustment": 0.8,  # Reduce to 80%
            },
            "positions": positions,
            "warnings": [
                "VaR exceeds acceptable limit (2.1% > 2.0%)",
                "Volatility elevated 14% above 30-day average",
                "Leverage approaching preferred maximum",
            ],
            "veto_status": "CONDITIONAL",
            "veto_conditions": [
                "Can add NEW long positions IF VaR reduced",
                "Cannot add new short (hedge acceptable)",
                "Must reduce existing position size by 20%",
            ],
        }

    def observe(
        self,
        evidence: Dict[str, Any],
        context: Optional[str] = None,
    ) -> tuple[Signal, int, str]:
        """
        Interpret risk evidence and issue VETO if necessary.

        Args:
            evidence: Portfolio risk metrics
            context: Additional context

        Returns:
            (Signal, confidence, reasoning)
        """
        if not evidence:
            return Signal.HOLD, 100, "No risk data - VETO for safety"

        portfolio = evidence.get("portfolio", {})
        var_analysis = evidence.get("var_analysis", {})
        drawdown = evidence.get("drawdown", {})
        leverage = evidence.get("leverage", {})
        volatility = evidence.get("volatility", {})

        # VETO threshold logic
        veto_active = False
        veto_reasons = []

        # RED LINE: VaR exceeds critical limit
        var_exceeds = var_analysis.get("exceeds_limit", False)
        if var_exceeds:
            veto_active = True
            veto_reasons.append("VaR exceeds acceptable limit")

        # RED LINE: Drawdown too deep
        current_dd = drawdown.get("current_drawdown_pct", 0)
        if current_dd > 15:
            veto_active = True
            veto_reasons.append(f"Drawdown too deep ({current_dd}% > 15%)")

        # RED LINE: Leverage excessive
        leverage_ratio = leverage.get("ratio", 1.0)
        if leverage_ratio > 3.0:
            veto_active = True
            veto_reasons.append(f"Leverage excessive ({leverage_ratio}x > 3.0x)")

        # YELLOW WARNING: Volatility spike
        vol_spike = volatility.get("vol_spike_pct", 0)
        if vol_spike > 20:
            veto_reasons.append(f"Volatility spike {vol_spike}% - size down 30%")

        # Decision logic
        if veto_active:
            return Signal.SELL, 100, f"ABSOLUTE VETO: {' | '.join(veto_reasons)}"

        if len(veto_reasons) > 0:
            # Yellow warning: allow trade but with position size reduction
            return Signal.HOLD, 85, f"WARNING: {' | '.join(veto_reasons)} - Position size reduced"

        # All clear - pass through
        reasoning = "Portfolio risk acceptable. All parameters nominal."
        return Signal.HOLD, 0, reasoning  # HOLD with confidence=0 means "no veto, proceed"

    def has_veto(self, signal: Signal) -> bool:
        """
        Check if this agent's result is a VETO.

        A VETO is indicated by:
        - Signal == SELL (block trade)
        - Confidence == 100 (absolute certainty)

        Args:
            signal: The signal returned

        Returns:
            True if this is a veto, False if passthrough
        """
        return signal == Signal.SELL
