#!/usr/bin/env python3
"""
SWARM DELTA — Agent 11: Execution Engine
Order management, trade execution, position management via Alpaca API
"""

import asyncio
import json
import time
from typing import Any, Dict, Optional
from enum import Enum

from .base_super_agent import SuperAgent, SuperAgentResult, Signal


class ExecutionMode(Enum):
    """Paper vs Live trading."""
    PAPER = "paper"
    LIVE = "live"
    BACKTEST = "backtest"


class ExecutionEngine(SuperAgent):
    """
    Agent 11: Execution Engine — SWARM DELTA

    Responsibilities:
    - Execute buy/sell orders on Alpaca
    - Manage position sizing and averaging
    - Trail stops and take profits
    - Handle partial fills and slippage
    - Track P&L and execution quality

    Commodity Focus: Liquid futures and spot commodities
    Execution: Alpaca Trading API (stocks, crypto), CME (futures)
    """

    SYSTEM_PROMPT = """You are the execution specialist for commodity trading.

Your role:
1. Execute buy/sell orders with optimal pricing
2. Manage position sizing and risk
3. Trail stops and manage exits
4. Monitor slippage and execution quality
5. Track all trades and P&L

Analysis Framework:
- Entry: Limit orders 0.5-1% better than market to improve execution
- Trail stop: 2x ATR below entry to avoid whipsaws
- Partial exit: Take 50% profit at 2:1 R/R, let runners run
- Position size: Kelly Criterion or % of account risk
- Slippage limit: Max 0.1% on entry

Output format:
{
    "order_status": "pending|filled|partial|rejected",
    "execution_price": 85.25,
    "position_size": 10,
    "slippage_bps": 8,
    "estimated_pnl": 1250,
    "risk_reward_ratio": 2.5
}"""

    def __init__(self, model: str = "claude-haiku-4-5-20251001", mode: ExecutionMode = ExecutionMode.PAPER):
        super().__init__(
            agent_id=11,
            agent_name="Execution Engine",
            system_prompt=self.SYSTEM_PROMPT,
            model=model,
        )
        self.mode = mode

    async def process(
        self,
        commodity: str,
        directive: str,
        context: Optional[Dict[str, Any]] = None,
        timeout_ms: Optional[int] = None,
    ) -> SuperAgentResult:
        """
        Execute trading signal via Alpaca or paper trading.

        Args:
            commodity: e.g., "COPPER"
            directive: "BUY 10 contracts at market" or similar
            context: Position state, account info
            timeout_ms: Execution timeout

        Returns:
            SuperAgentResult with execution details
        """
        start_time = time.time()
        timeout = timeout_ms or self.MAX_EXEC_TIME_MS

        try:
            evidence = await self.call_with_timeout(
                self._execute_trade(commodity, directive, context),
                timeout_ms=timeout,
            )

            if evidence is None:
                return self.build_result(
                    signal=Signal.HOLD,
                    confidence=0,
                    reasoning="Trade execution timed out",
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
                sources_analyzed=1,
                evidence_pack=evidence,
                latency_ms=latency,
                tools_called=["execute_order", "manage_position"],
            )

        except Exception as e:
            latency = (time.time() - start_time) * 1000
            return self.build_result(
                signal=Signal.HOLD,
                confidence=0,
                reasoning=f"Execution error: {str(e)[:100]}",
                sources_analyzed=0,
                evidence_pack={},
                latency_ms=latency,
                error=str(e)[:100],
            )

    async def _execute_trade(
        self,
        commodity: str,
        directive: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute a trade order.

        Args:
            commodity: Trading commodity
            directive: Trade instruction
            context: Portfolio context

        Returns:
            Execution details
        """
        await asyncio.sleep(0.3)  # Simulate order submission latency

        # Parse directive (simplified)
        side = "BUY" if "BUY" in directive.upper() else "SELL"
        size = 5  # Default size (would parse from directive)

        # Simulate execution
        market_price = 85.50  # Mock market price
        execution_price = market_price * 0.9995 if side == "BUY" else market_price * 1.0005
        slippage_bps = 5

        return {
            "commodity": commodity,
            "timestamp": "2025-03-03T12:00:00Z",
            "execution_mode": self.mode.value,
            "order": {
                "side": side,
                "size": size,
                "symbol": commodity,
                "order_type": "limit",
            },
            "execution": {
                "status": "filled",
                "market_price": market_price,
                "execution_price": execution_price,
                "slippage_bps": slippage_bps,
                "filled_size": size,
                "fill_time": "2025-03-03T12:00:05Z",
            },
            "position": {
                "symbol": commodity,
                "side": side,
                "size": size,
                "avg_fill_price": execution_price,
                "notional_value": execution_price * size * 100,  # Futures contract multiplier
                "unrealized_pnl": 0,
            },
            "risk_management": {
                "stop_loss_price": execution_price * 0.97 if side == "BUY" else execution_price * 1.03,
                "take_profit_price": execution_price * 1.05 if side == "BUY" else execution_price * 0.95,
                "risk_reward_ratio": 2.5,
                "position_size_valid": True,
            },
            "account": {
                "buying_power": 45_000,
                "cash": 12_000,
                "equity": 1_000_000,
                "margin_used": 25_000,
            },
        }

    def observe(
        self,
        evidence: Dict[str, Any],
        context: Optional[str] = None,
    ) -> tuple[Signal, int, str]:
        """
        Interpret execution evidence.

        Args:
            evidence: Execution data
            context: Additional context

        Returns:
            (Signal, confidence, reasoning)
        """
        if not evidence:
            return Signal.HOLD, 0, "No execution data"

        order_status = evidence.get("execution", {}).get("status", "")
        slippage = evidence.get("execution", {}).get("slippage_bps", 0)
        position_valid = evidence.get("risk_management", {}).get("position_size_valid", False)

        reasoning_parts = []

        if order_status == "filled":
            reasoning = f"Order filled successfully (slippage {slippage}bps)"
            return Signal.HOLD, 100, reasoning
        elif order_status == "partial":
            reasoning_parts.append(f"Partially filled ({evidence['execution']['filled_size']} of {evidence['order']['size']})")
        elif order_status == "rejected":
            reasoning_parts.append("Order rejected - insufficient liquidity or price range exceeded")
            return Signal.HOLD, 0, "Order rejected"

        if not position_valid:
            reasoning_parts.append("Position size validation failed")

        reasoning = " | ".join(reasoning_parts) if reasoning_parts else "Trade executed"
        return Signal.HOLD, 100 if order_status == "filled" else 50, reasoning
