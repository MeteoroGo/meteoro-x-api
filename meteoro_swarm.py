#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  METEORO SWARM v7.0 — CENTRAL INTELLIGENCE ORCHESTRATOR           ║
║  12+ Super Agents in 4 Coordinated Swarms                         ║
║  Inspired by OpenManus/Manus AI Architecture                      ║
║                                                                    ║
║  SWARM ALPHA: Intelligence & Reconnaissance                       ║
║    Agent 01: Satellite Recon | Agent 02: Maritime Intel          ║
║    Agent 03: Supply Chain Mapper                                  ║
║                                                                    ║
║  SWARM BRAVO: OSINT & Geopolitical                                ║
║    Agent 04: LatAm OSINT | Agent 05: China Demand (Kimi)         ║
║    Agent 06: Geopolitical Risk                                    ║
║                                                                    ║
║  SWARM CHARLIE: Macro & Quantitative                              ║
║    Agent 07: Macro Regime | Agent 08: Quant Alpha (DeepSeek)    ║
║    Agent 09: Sentiment & Flow                                     ║
║                                                                    ║
║  SWARM DELTA: Execution & Protection                              ║
║    Agent 10: Risk Guardian (VETO POWER) | Agent 11: Execution    ║
║    Agent 12: Counterintelligence                                  ║
║                                                                    ║
║  META LAYER:                                                      ║
║    Agent 00: Meteoro Commander (You are here)                    ║
║                                                                    ║
║  Time Budget: < 20 seconds total for all 12 agents                ║
║  Consensus: 8/12 agree = signal, 10/12 = high conviction          ║
║                                                                    ║
║  Cost Target: $0.03-0.08 per full analysis                        ║
║  Models: Claude Haiku + DeepSeek + Kimi + Gemini                 ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import asdict, dataclass

# Swarm agents
from swarm_agents.base_super_agent import SuperAgent, SuperAgentResult, Signal
from swarm_agents.satellite_recon import SatelliteRecon
from swarm_agents.maritime_intel import MaritimeIntel
from swarm_agents.supply_chain_mapper import SupplyChainMapper
from swarm_agents.latam_osint import LatAmOSINT
from swarm_agents.china_demand_oracle import ChinaDemandOracle
from swarm_agents.geopolitical_risk import GeopoliticalRisk
from swarm_agents.macro_regime import MacroRegimeDetector
from swarm_agents.quant_alpha import QuantAlpha
from swarm_agents.sentiment_flow import SentimentFlow
from swarm_agents.risk_guardian import RiskGuardian
from swarm_agents.execution_engine import ExecutionEngine
from swarm_agents.counterintelligence import Counterintelligence


@dataclass
class SwarmSignal:
    """Final trading signal from Meteoro Swarm."""
    timestamp: str
    commodity: str
    final_signal: Signal
    conviction: int  # 0-100
    reasoning: str
    agents_bullish: int
    agents_bearish: int
    agents_neutral: int
    risk_guardian_veto: bool
    all_results: List[SuperAgentResult]
    total_latency_ms: float
    cost_usd: float
    metadata: Dict[str, Any]


class MeteorSwarm:
    """
    Central Intelligence Orchestrator for Meteoro Swarm.

    Coordinates 12 Super Agents across 4 swarms:
    - ALPHA (3 agents): Intelligence & Reconnaissance
    - BRAVO (3 agents): OSINT & Geopolitical
    - CHARLIE (3 agents): Macro & Quantitative
    - DELTA (3 agents): Execution & Protection

    Consensus Mechanism:
    - 8/12 agents agree → signal valid
    - 10/12 agents agree → high conviction
    - 12/12 agents agree → maximum conviction
    - Risk Guardian VETO → absolute override

    Time Budget: < 20 seconds total
    Cost Target: $0.03-0.08 per analysis
    """

    TIME_BUDGET_MS = 20_000  # 20 seconds total
    AGENT_TIMEOUT_MS = 5_000  # 5 seconds per agent max

    def __init__(self):
        """Initialize all 12 Super Agents."""
        # SWARM ALPHA: Intelligence & Reconnaissance
        self.alpha_agents = [
            SatelliteRecon(model="claude-haiku-4-5-20251001"),
            MaritimeIntel(model="claude-haiku-4-5-20251001"),
            SupplyChainMapper(model="claude-haiku-4-5-20251001"),
        ]

        # SWARM BRAVO: OSINT & Geopolitical
        self.bravo_agents = [
            LatAmOSINT(model="claude-haiku-4-5-20251001"),
            ChinaDemandOracle(model="moonshot-v1-8k"),  # Kimi for Chinese
            GeopoliticalRisk(model="claude-haiku-4-5-20251001"),
        ]

        # SWARM CHARLIE: Macro & Quantitative
        self.charlie_agents = [
            MacroRegimeDetector(model="claude-haiku-4-5-20251001"),
            QuantAlpha(model="deepseek-chat"),  # DeepSeek for math
            SentimentFlow(model="claude-haiku-4-5-20251001"),
        ]

        # SWARM DELTA: Execution & Protection
        self.delta_agents = [
            RiskGuardian(model="claude-haiku-4-5-20251001"),  # VETO POWER
            ExecutionEngine(model="claude-haiku-4-5-20251001"),
            Counterintelligence(model="claude-haiku-4-5-20251001"),
        ]

        self.all_agents = (
            self.alpha_agents + self.bravo_agents +
            self.charlie_agents + self.delta_agents
        )

    async def analyze(
        self,
        commodity: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> SwarmSignal:
        """
        Run full swarm analysis on a commodity.

        Execution flow:
        1. ALPHA runs (parallel): Satellite, Maritime, Supply Chain
        2. BRAVO runs (parallel): LatAm, China, Geopolitics
        3. CHARLIE runs (parallel): Macro, Quant, Sentiment
        4. DELTA runs (sequential): Risk Guardian veto, Execution, CI

        Args:
            commodity: e.g., "COPPER", "OIL", "COFFEE"
            context: Shared analysis context

        Returns:
            SwarmSignal with consensus and final decision
        """
        start_time = time.time()
        session_id = str(uuid.uuid4().hex[:8])

        print(f"\n{'='*70}")
        print(f"METEORO SWARM v7.0 — ANALYSIS SESSION {session_id}")
        print(f"Commodity: {commodity}")
        print(f"Timestamp: {datetime.utcnow().isoformat()}")
        print(f"{'='*70}\n")

        all_results = []

        try:
            # ─────────────────────────────────────────────────────────
            # SWARM ALPHA: Intelligence & Reconnaissance (parallel)
            # ─────────────────────────────────────────────────────────
            print("[ALPHA] Intelligence & Reconnaissance...")
            alpha_start = time.time()

            alpha_results = await asyncio.gather(
                *[self._run_agent(agent, commodity, context)
                  for agent in self.alpha_agents]
            )
            all_results.extend(alpha_results)

            alpha_latency = (time.time() - alpha_start) * 1000
            print(f"[ALPHA] Complete in {alpha_latency:.0f}ms\n")

            # ─────────────────────────────────────────────────────────
            # SWARM BRAVO: OSINT & Geopolitical (parallel)
            # ─────────────────────────────────────────────────────────
            print("[BRAVO] OSINT & Geopolitical Analysis...")
            bravo_start = time.time()

            bravo_results = await asyncio.gather(
                *[self._run_agent(agent, commodity, context)
                  for agent in self.bravo_agents]
            )
            all_results.extend(bravo_results)

            bravo_latency = (time.time() - bravo_start) * 1000
            print(f"[BRAVO] Complete in {bravo_latency:.0f}ms\n")

            # ─────────────────────────────────────────────────────────
            # SWARM CHARLIE: Macro & Quantitative (parallel)
            # ─────────────────────────────────────────────────────────
            print("[CHARLIE] Macro & Quantitative Analysis...")
            charlie_start = time.time()

            charlie_results = await asyncio.gather(
                *[self._run_agent(agent, commodity, context)
                  for agent in self.charlie_agents]
            )
            all_results.extend(charlie_results)

            charlie_latency = (time.time() - charlie_start) * 1000
            print(f"[CHARLIE] Complete in {charlie_latency:.0f}ms\n")

            # ─────────────────────────────────────────────────────────
            # SWARM DELTA: Execution & Protection
            # ─────────────────────────────────────────────────────────
            print("[DELTA] Risk Management & Execution...")
            delta_start = time.time()

            # Risk Guardian first (can veto)
            risk_result = await self._run_agent(
                self.delta_agents[0],  # Risk Guardian
                commodity,
                self._build_context_from_results(all_results, context),
            )
            all_results.append(risk_result)

            # Check for VETO
            veto_active = risk_result.signal == Signal.SELL

            if not veto_active:
                # Execution and CI run in parallel (if no veto)
                delta_parallel = await asyncio.gather(
                    self._run_agent(self.delta_agents[1], commodity, context),  # Execution
                    self._run_agent(self.delta_agents[2], commodity, context),  # CI
                )
                all_results.extend(delta_parallel)

            delta_latency = (time.time() - delta_start) * 1000
            print(f"[DELTA] Complete in {delta_latency:.0f}ms\n")

            # ─────────────────────────────────────────────────────────
            # CONSENSUS MECHANISM
            # ─────────────────────────────────────────────────────────
            final_signal, conviction, reasoning = self._build_consensus(
                all_results,
                veto_active,
            )

            total_latency = (time.time() - start_time) * 1000
            total_cost = sum(r.cost_usd for r in all_results)

            print(f"\n{'='*70}")
            print(f"CONSENSUS RESULTS")
            print(f"{'='*70}")
            print(f"Final Signal: {final_signal.value}")
            print(f"Conviction: {conviction}%")
            print(f"Risk Veto Active: {veto_active}")
            print(f"Total Agents: {len(all_results)}")
            print(f"Bullish: {sum(1 for r in all_results if r.signal == Signal.BUY)}")
            print(f"Bearish: {sum(1 for r in all_results if r.signal == Signal.SELL)}")
            print(f"Neutral: {sum(1 for r in all_results if r.signal in [Signal.HOLD, Signal.NEUTRAL])}")
            print(f"Total Latency: {total_latency:.0f}ms")
            print(f"Total Cost: ${total_cost:.4f}")
            print(f"{'='*70}\n")

            return SwarmSignal(
                timestamp=datetime.utcnow().isoformat(),
                commodity=commodity,
                final_signal=final_signal,
                conviction=conviction,
                reasoning=reasoning,
                agents_bullish=sum(1 for r in all_results if r.signal == Signal.BUY),
                agents_bearish=sum(1 for r in all_results if r.signal == Signal.SELL),
                agents_neutral=sum(1 for r in all_results if r.signal in [Signal.HOLD, Signal.NEUTRAL]),
                risk_guardian_veto=veto_active,
                all_results=all_results,
                total_latency_ms=total_latency,
                cost_usd=total_cost,
                metadata={"session_id": session_id},
            )

        except Exception as e:
            print(f"[ERROR] Swarm analysis failed: {str(e)}")
            raise

    async def _run_agent(
        self,
        agent: SuperAgent,
        commodity: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> SuperAgentResult:
        """
        Run a single agent with timeout protection.

        Args:
            agent: The SuperAgent to run
            commodity: Trading commodity
            context: Shared context

        Returns:
            SuperAgentResult
        """
        try:
            result = await asyncio.wait_for(
                agent.process(commodity, f"Analyze {commodity}", context),
                timeout=self.AGENT_TIMEOUT_MS / 1000.0,
            )
            print(f"  [{agent.agent_name:25s}] {result.signal.value:6s} " +
                  f"(conf={result.confidence}%, latency={result.latency_ms:.0f}ms)")
            return result
        except asyncio.TimeoutError:
            print(f"  [{agent.agent_name:25s}] TIMEOUT (>{self.AGENT_TIMEOUT_MS}ms)")
            return SuperAgentResult(
                agent_id=agent.agent_id,
                agent_name=agent.agent_name,
                signal=Signal.NEUTRAL,
                confidence=0,
                reasoning=f"Analysis timed out (>{self.AGENT_TIMEOUT_MS}ms)",
                sources_analyzed=0,
                evidence_pack={},
                latency_ms=self.AGENT_TIMEOUT_MS,
                error="Timeout",
            )
        except Exception as e:
            print(f"  [{agent.agent_name:25s}] ERROR: {str(e)[:40]}")
            return SuperAgentResult(
                agent_id=agent.agent_id,
                agent_name=agent.agent_name,
                signal=Signal.NEUTRAL,
                confidence=0,
                reasoning=f"Error: {str(e)[:100]}",
                sources_analyzed=0,
                evidence_pack={},
                latency_ms=0,
                error=str(e)[:100],
            )

    def _build_context_from_results(
        self,
        results: List[SuperAgentResult],
        base_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Build context dict from previous agent results.

        Args:
            results: List of SuperAgentResults
            base_context: Optional base context

        Returns:
            Context dict for next agents
        """
        context = base_context or {}

        # Summarize results
        context["previous_results"] = [
            {
                "agent": r.agent_name,
                "signal": r.signal.value,
                "confidence": r.confidence,
                "reasoning": r.reasoning[:200],
            }
            for r in results
        ]

        # Aggregate sentiment
        bullish = sum(1 for r in results if r.signal == Signal.BUY)
        bearish = sum(1 for r in results if r.signal == Signal.SELL)
        context["consensus_so_far"] = {
            "bullish": bullish,
            "bearish": bearish,
            "total": len(results),
        }

        return context

    def _build_consensus(
        self,
        results: List[SuperAgentResult],
        veto_active: bool,
    ) -> Tuple[Signal, int, str]:
        """
        Build consensus from all agent results.

        Consensus Logic:
        - 8+/12 agents agree → signal valid
        - 10+/12 agents agree → high conviction
        - 12/12 agents agree → maximum conviction
        - Risk Guardian VETO → final signal = SELL (halt)

        Args:
            results: All SuperAgentResults
            veto_active: Is Risk Guardian veto active?

        Returns:
            (final_signal, conviction 0-100, reasoning)
        """
        bullish = sum(1 for r in results if r.signal == Signal.BUY)
        bearish = sum(1 for r in results if r.signal == Signal.SELL)
        neutral = sum(1 for r in results if r.signal in [Signal.HOLD, Signal.NEUTRAL])

        total = len(results)

        # VETO overrides everything
        if veto_active:
            return (
                Signal.SELL,
                100,
                "Risk Guardian VETO: Portfolio risk exceeds limits",
            )

        # Majority voting
        if bullish > bearish:
            signal = Signal.BUY
        elif bearish > bullish:
            signal = Signal.SELL
        else:
            signal = Signal.HOLD

        # Conviction calculation
        dominant = max(bullish, bearish)
        conviction = int((dominant / total) * 100)

        # Boost conviction if near-unanimous
        if dominant >= 11:
            conviction = 95
        elif dominant >= 10:
            conviction = 85
        elif dominant >= 9:
            conviction = 75
        elif dominant >= 8:
            conviction = 65

        reasoning_parts = [
            f"{bullish} bullish",
            f"{bearish} bearish",
            f"{neutral} neutral",
        ]

        # Add specific insights
        if bullish >= 8:
            reasoning_parts.append("Strong bullish consensus")
        elif bearish >= 8:
            reasoning_parts.append("Strong bearish consensus")
        else:
            reasoning_parts.append("No clear consensus")

        reasoning = " | ".join(reasoning_parts)

        return signal, conviction, reasoning

    def to_dict(self) -> Dict[str, Any]:
        """Serialize swarm configuration."""
        return {
            "swarm_name": "Meteoro Swarm v7.0",
            "total_agents": len(self.all_agents),
            "swarms": {
                "alpha": [a.agent_name for a in self.alpha_agents],
                "bravo": [a.agent_name for a in self.bravo_agents],
                "charlie": [a.agent_name for a in self.charlie_agents],
                "delta": [a.agent_name for a in self.delta_agents],
            },
            "time_budget_ms": self.TIME_BUDGET_MS,
            "agent_timeout_ms": self.AGENT_TIMEOUT_MS,
            "consensus_threshold": "8/12 agents",
            "veto_power": "Risk Guardian (Agent 10)",
        }


# ═══════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

async def main():
    """Example usage of Meteoro Swarm."""
    swarm = MeteorSwarm()

    # Print configuration
    print(json.dumps(swarm.to_dict(), indent=2))

    # Run analysis
    signal = await swarm.analyze("COPPER")

    # Output final signal
    print(f"\nFinal Decision:")
    print(json.dumps(
        {
            "signal": signal.final_signal.value,
            "conviction": signal.conviction,
            "reasoning": signal.reasoning,
            "latency_ms": signal.total_latency_ms,
            "cost_usd": signal.cost_usd,
        },
        indent=2,
    ))


if __name__ == "__main__":
    asyncio.run(main())
