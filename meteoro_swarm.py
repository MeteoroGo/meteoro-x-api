#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  METEORO SWARM v7.2 — REAL DATA + RATE-LIMITED LLM               ║
║  12 Super Agents | DeepSeek + Gemini | Real Market Data           ║
║                                                                    ║
║  FLOW:                                                            ║
║    1. Fetch REAL market data (yfinance, GDELT)                   ║
║    2. Send data to LLMs (DeepSeek/Gemini) per agent              ║
║    3. Parse LLM response → Signal + Confidence + Evidence        ║
║    4. Consensus mechanism → Final trade signal                    ║
║                                                                    ║
║  Each agent has a specialized prompt + gets real data.            ║
║  LLMs do the actual thinking. No hardcoded results.              ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import time
import uuid
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import asdict, dataclass, field

from swarm_agents.base_super_agent import SuperAgent, SuperAgentResult, Signal

# Real data sources
try:
    from data_sources.market_data import (
        fetch_commodity_price,
        fetch_macro_indicators,
        fetch_latam_fx,
        fetch_gdelt_events,
        fetch_full_market_context,
    )
    HAS_MARKET_DATA = True
except Exception as e:
    HAS_MARKET_DATA = False
    print(f"[WARN] market_data import failed: {e}")

# LLM Router
try:
    from multi_model_router import call_llm, LLMResponse
    HAS_LLM = True
except Exception as e:
    HAS_LLM = False
    print(f"[WARN] multi_model_router import failed: {e}")


# ═══════════════════════════════════════════════════════════════
# AGENT PROMPTS — Each agent's specialized instructions
# ═══════════════════════════════════════════════════════════════

AGENT_CONFIGS = [
    {
        "id": 1, "name": "Satellite Recon", "router_name": "satellite_recon",
        "prompt": """You are SA-01 Satellite Recon for commodity trading intelligence.
Analyze the commodity price data and market context to assess PHYSICAL supply signals:
- Are there signs of supply disruption (price spikes, volatility)?
- Weather/climate impact on production (use price patterns as proxy)
- Infrastructure stress signals from price action

Based on the data, output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"...","sources_analyzed":N,"key_finding":"one line summary"}"""
    },
    {
        "id": 2, "name": "Maritime Intel", "router_name": "maritime_intel",
        "prompt": """You are SA-02 Maritime Intelligence for commodity trading.
Analyze shipping and trade flow indicators from the market data:
- Baltic Dry Index trends (if available)
- Supply chain stress from commodity price patterns
- Import/export dynamics from FX rate movements
- Port congestion signals from price volatility

Based on the data, output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"...","sources_analyzed":N,"key_finding":"one line summary"}"""
    },
    {
        "id": 3, "name": "Supply Chain Mapper", "router_name": "supply_chain_mapper",
        "prompt": """You are SA-03 Supply Chain Mapper for commodity trading.
Analyze supply chain dynamics from the market data:
- Production capacity signals (price trends)
- Inventory levels implied by price backwardation/contango
- Logistics cost trends from macro data
- Supply concentration risks

Based on the data, output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"...","sources_analyzed":N,"key_finding":"one line summary"}"""
    },
    {
        "id": 4, "name": "LatAm OSINT", "router_name": "latam_osint",
        "prompt": """You are SA-04 LatAm OSINT Intelligence for commodity trading.
Analyze Latin American market dynamics:
- LatAm currency movements (COP, BRL, CLP, PEN, MXN) and export competitiveness
- Political stability signals from FX volatility
- Trade policy implications from price/FX divergences
- Regional production signals

Based on the data, output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"...","sources_analyzed":N,"key_finding":"one line summary"}"""
    },
    {
        "id": 5, "name": "China Demand Oracle", "router_name": "china_demand_oracle",
        "prompt": """You are SA-05 China Demand Oracle for commodity trading. You specialize in Chinese demand analysis.
Analyze Chinese demand signals:
- USD/CNY exchange rate movements and PBOC policy signals
- Chinese manufacturing demand implied by commodity prices
- Stockpiling patterns from price action
- Construction and infrastructure demand signals

Based on the data, output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"...","sources_analyzed":N,"key_finding":"one line summary"}"""
    },
    {
        "id": 6, "name": "Geopolitical Risk Assessor", "router_name": "geopolitical_risk",
        "prompt": """You are SA-06 Geopolitical Risk Assessor for commodity trading.
Analyze geopolitical risks affecting commodities using the news articles and market data:
- Active conflicts near commodity production/shipping zones
- Sanctions and trade restrictions
- Political instability in producer nations
- Supply disruption probability from news sentiment

Based on the data and news articles, output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"...","sources_analyzed":N,"key_finding":"one line summary"}"""
    },
    {
        "id": 7, "name": "Macro Regime Detector", "router_name": "macro_regime",
        "prompt": """You are SA-07 Macro Regime Detector for commodity trading.
Analyze the macroeconomic regime from REAL market data:
- VIX level and trend (fear vs complacency)
- DXY (Dollar Index) strength — inverse correlation with commodities
- US Treasury yields (10Y, 2Y) and yield curve shape
- S&P 500 risk appetite signal
- Gold as safe haven indicator

KEY RULES:
- Rising DXY = bearish for commodities
- Rising VIX = risk-off = bearish
- Falling real yields = inflationary = bullish for commodities
- Steepening yield curve = growth expectations = bullish

Based on the REAL data provided, output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"...","sources_analyzed":N,"key_finding":"one line summary"}"""
    },
    {
        "id": 8, "name": "Quantitative Alpha", "router_name": "quant_alpha",
        "prompt": """You are SA-08 Quantitative Alpha, a quant analyst for commodity trading.
Analyze the REAL price data using quantitative methods:
- RSI (overbought >70, oversold <30)
- SMA crossovers (price vs SMA-10 vs SMA-20)
- Annualized volatility and regime
- Price momentum (% change)
- Mean reversion signals (distance from moving averages)
- Risk/reward based on recent price range

Use the actual numbers provided. Do NOT invent data.

Based on the REAL technical data, output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"...","sources_analyzed":N,"key_finding":"one line summary"}"""
    },
    {
        "id": 9, "name": "Sentiment & Flow", "router_name": "sentiment_flow",
        "prompt": """You are SA-09 Sentiment & Flow Analyst for commodity trading.
Analyze market sentiment and positioning:
- News sentiment from GDELT articles (tone positive/negative)
- Volume patterns from price data
- Retail vs institutional positioning signals
- Crowding risk from extreme price moves
- Contrarian signals

Based on the data, output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"...","sources_analyzed":N,"key_finding":"one line summary"}"""
    },
    {
        "id": 10, "name": "Risk Guardian", "router_name": "risk_guardian",
        "prompt": """You are SA-10 Risk Guardian with VETO POWER for commodity trading.
You are the last line of defense. Analyze ALL previous agent signals and market data:
- If majority signal aligns with macro conditions → APPROVE (signal = majority signal)
- If signals conflict strongly (close to 50/50) → HOLD (too uncertain)
- If volatility is extreme (VIX>30) and majority is BUY → VETO with SELL
- If price is at extreme RSI and majority goes against mean reversion → VETO

YOU HAVE VETO POWER. Use it wisely. Only VETO if risk is genuinely elevated.
Do NOT always veto. If the trade makes sense, let it through.

Based on ALL the data and previous agent signals, output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"...","sources_analyzed":N,"key_finding":"one line summary","veto":true|false}"""
    },
    {
        "id": 11, "name": "Execution Engine", "router_name": "execution_engine",
        "prompt": """You are SA-11 Execution Engine for commodity trading.
Based on the consensus signal and market data, determine optimal execution:
- Entry price (current price adjusted for slippage)
- Position size (based on volatility and confidence)
- Stop loss level
- Take profit targets
- Time horizon

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"...","sources_analyzed":N,"key_finding":"one line summary","entry_price":0,"stop_loss":0,"take_profit":0,"position_size_pct":0}"""
    },
    {
        "id": 12, "name": "Counterintelligence", "router_name": "counterintelligence",
        "prompt": """You are SA-12 Counterintelligence for commodity trading.
Validate the analysis quality and check for blind spots:
- Are the signals based on solid data?
- Any conflicting indicators being ignored?
- Data quality assessment
- Confidence calibration (are agents overconfident?)

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"...","sources_analyzed":N,"key_finding":"one line summary"}"""
    },
]


@dataclass
class SwarmSignal:
    """Final trading signal from Meteoro Swarm."""
    timestamp: str
    commodity: str
    final_signal: Signal
    conviction: int
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
    Meteoro Swarm v7.2 — Real Data + Real LLM Analysis.
    12 Super Agents with DeepSeek + Gemini doing actual analysis.
    """

    TIME_BUDGET_MS = 60_000  # 60 seconds (batched LLM calls + rate limit delays)
    AGENT_TIMEOUT_MS = 20_000  # 20 seconds per agent (includes retry wait)

    def __init__(self):
        self.agent_configs = AGENT_CONFIGS

    async def analyze(
        self,
        commodity: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> SwarmSignal:
        """
        Run full swarm analysis with REAL data and REAL LLM calls.
        """
        start_time = time.time()
        session_id = str(uuid.uuid4().hex[:8])

        print(f"\n{'='*70}")
        print(f"METEORO SWARM v7.2 — REAL DATA + RATE LIMITED — {session_id}")
        print(f"Commodity: {commodity}")
        print(f"{'='*70}\n")

        # ─── STEP 1: FETCH REAL MARKET DATA ───────────────────────
        print("[DATA] Fetching REAL market data...")
        data_start = time.time()

        market_data = {}
        if HAS_MARKET_DATA:
            try:
                market_data = await asyncio.wait_for(
                    fetch_full_market_context(commodity),
                    timeout=15.0,
                )
                # Also fetch GDELT news
                try:
                    gdelt = await asyncio.wait_for(
                        fetch_gdelt_events(commodity),
                        timeout=8.0,
                    )
                    market_data["news"] = gdelt
                except Exception as ge:
                    market_data["news"] = {"error": str(ge), "articles": []}

                print(f"[DATA] Real data fetched in {(time.time()-data_start)*1000:.0f}ms")

                # Log what we got
                cd = market_data.get("commodity", {})
                if "price" in cd:
                    print(f"[DATA] {commodity} price: ${cd['price']} ({cd.get('change_pct',0):+.2f}%)")
                macro = market_data.get("macro", {}).get("indicators", {})
                if "VIX" in macro:
                    print(f"[DATA] VIX: {macro['VIX'].get('value',0)} | DXY: {macro.get('DXY',{}).get('value',0)}")

            except Exception as e:
                print(f"[DATA] Error fetching market data: {e}")
                market_data = {"error": str(e)}
        else:
            print("[DATA] market_data module not available")

        # ─── STEP 2: RUN ALL AGENTS WITH LLM ─────────────────────
        if not HAS_LLM:
            print("[ERROR] LLM router not available — cannot run swarm")
            raise RuntimeError("LLM router not available")

        # Build data string for agents
        data_str = json.dumps(market_data, default=str, ensure_ascii=False)
        # Truncate if too long (LLM context limits)
        if len(data_str) > 8000:
            data_str = data_str[:8000] + "...[truncated]"

        all_results: List[SuperAgentResult] = []

        # Run agents 1-9 in BATCHES of 3 (avoid rate limiting LLM providers)
        # Gemini free tier = 15 RPM, so 3 concurrent + stagger is safe
        BATCH_SIZE = 3
        BATCH_DELAY = 1.5  # seconds between batches

        intelligence_agents = self.agent_configs[:9]  # Agents 1-9
        num_batches = (len(intelligence_agents) + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_idx in range(num_batches):
            batch_start = batch_idx * BATCH_SIZE
            batch_end = min(batch_start + BATCH_SIZE, len(intelligence_agents))
            batch = intelligence_agents[batch_start:batch_end]
            batch_names = [c["name"] for c in batch]

            print(f"\n[SWARM] Batch {batch_idx+1}/{num_batches}: {', '.join(batch_names)}")

            batch_tasks = [
                self._run_llm_agent(config, commodity, data_str, context)
                for config in batch
            ]

            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            for r in batch_results:
                if isinstance(r, Exception):
                    print(f"  [Agent ERROR] {r}")
                    all_results.append(SuperAgentResult(
                        agent_id=0, agent_name="Failed Agent",
                        signal=Signal.NEUTRAL, confidence=0,
                        reasoning=f"Error: {str(r)[:100]}",
                        sources_analyzed=0,
                    ))
                else:
                    all_results.append(r)

            # Delay between batches to respect rate limits
            if batch_idx < num_batches - 1:
                print(f"  [RATE LIMIT] Waiting {BATCH_DELAY}s before next batch...")
                await asyncio.sleep(BATCH_DELAY)

        # ─── STEP 3: RISK GUARDIAN (with all previous results) ────
        print("\n[DELTA] Risk Guardian analyzing...")
        prev_signals = "\n".join([
            f"  {r.agent_name}: {r.signal.value} ({r.confidence}%) — {r.reasoning[:80]}"
            for r in all_results
        ])

        risk_data = f"""PREVIOUS AGENT SIGNALS:
{prev_signals}

MARKET DATA:
{data_str}"""

        risk_config = self.agent_configs[9]  # Agent 10 = Risk Guardian
        risk_result = await self._run_llm_agent(
            risk_config, commodity, risk_data, context
        )
        all_results.append(risk_result)

        # Check for veto
        veto_active = False
        if risk_result.evidence_pack.get("veto", False):
            veto_active = True
            print(f"  [RISK GUARDIAN] ⚠ VETO ACTIVE: {risk_result.reasoning}")
        else:
            print(f"  [RISK GUARDIAN] ✓ No veto: {risk_result.reasoning[:60]}")

        # ─── STEP 4: EXECUTION + COUNTERINTEL (parallel) ─────────
        print("\n[DELTA] Execution + Counterintelligence...")
        delta_tasks = []
        for config in self.agent_configs[10:12]:  # Agents 11-12
            delta_tasks.append(
                self._run_llm_agent(config, commodity, data_str, context)
            )
        delta_results = await asyncio.gather(*delta_tasks, return_exceptions=True)
        for r in delta_results:
            if isinstance(r, Exception):
                all_results.append(SuperAgentResult(
                    agent_id=0, agent_name="Delta Failed",
                    signal=Signal.NEUTRAL, confidence=0,
                    reasoning=str(r)[:100], sources_analyzed=0,
                ))
            else:
                all_results.append(r)

        # ─── STEP 5: CONSENSUS ───────────────────────────────────
        final_signal, conviction, reasoning = self._build_consensus(
            all_results, veto_active
        )

        total_latency = (time.time() - start_time) * 1000
        total_cost = sum(r.cost_usd for r in all_results)

        print(f"\n{'='*70}")
        print(f"CONSENSUS: {final_signal.value} | Conviction: {conviction}%")
        print(f"Bullish: {sum(1 for r in all_results if r.signal == Signal.BUY)}")
        print(f"Bearish: {sum(1 for r in all_results if r.signal == Signal.SELL)}")
        print(f"Latency: {total_latency:.0f}ms | Cost: ${total_cost:.4f}")
        print(f"{'='*70}\n")

        return SwarmSignal(
            timestamp=datetime.now(timezone.utc).isoformat(),
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
            metadata={"session_id": session_id, "has_real_data": HAS_MARKET_DATA},
        )

    async def _run_llm_agent(
        self,
        config: Dict[str, Any],
        commodity: str,
        data_str: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> SuperAgentResult:
        """
        Run a single agent by calling the LLM with real data.
        """
        agent_start = time.time()
        agent_name = config["name"]
        agent_id = config["id"]
        router_name = config["router_name"]

        try:
            user_message = f"""COMMODITY: {commodity}

REAL MARKET DATA (from yfinance + GDELT, fetched just now):
{data_str}

Analyze this REAL data and provide your assessment. Use the ACTUAL numbers.
Do NOT invent or hallucinate data. Base your analysis on what you see above."""

            # Call LLM via router (DeepSeek, Gemini, or Claude)
            llm_response: LLMResponse = await asyncio.wait_for(
                call_llm(
                    agent_name=router_name,
                    system_prompt=config["prompt"],
                    user_message=user_message,
                ),
                timeout=self.AGENT_TIMEOUT_MS / 1000.0,
            )

            latency = int((time.time() - agent_start) * 1000)

            # Parse LLM response into structured result
            signal, confidence, reasoning, evidence = self._parse_llm_response(
                llm_response.content, agent_name
            )

            print(f"  [{agent_name:25s}] {signal.value:4s} ({confidence}%) "
                  f"via {llm_response.model_used} [{latency}ms] "
                  f"{'(fallback)' if llm_response.fallback_used else ''}")

            return SuperAgentResult(
                agent_id=agent_id,
                agent_name=agent_name,
                signal=signal,
                confidence=confidence,
                reasoning=reasoning,
                sources_analyzed=evidence.get("sources_analyzed", 3),
                evidence_pack=evidence,
                latency_ms=latency,
                timestamp=datetime.now(timezone.utc).isoformat(),
                tools_called=[f"llm:{llm_response.model_used}"],
                cost_usd=llm_response.cost_usd,
                metadata={
                    "session_id": str(uuid.uuid4().hex[:8]),
                    "model_used": llm_response.model_used,
                    "provider": llm_response.provider,
                    "fallback": llm_response.fallback_used,
                },
            )

        except asyncio.TimeoutError:
            latency = int((time.time() - agent_start) * 1000)
            print(f"  [{agent_name:25s}] TIMEOUT ({latency}ms)")
            return SuperAgentResult(
                agent_id=agent_id, agent_name=agent_name,
                signal=Signal.NEUTRAL, confidence=0,
                reasoning="LLM call timed out",
                sources_analyzed=0, latency_ms=latency,
                error="timeout",
            )
        except Exception as e:
            latency = int((time.time() - agent_start) * 1000)
            print(f"  [{agent_name:25s}] ERROR: {str(e)[:50]}")
            return SuperAgentResult(
                agent_id=agent_id, agent_name=agent_name,
                signal=Signal.NEUTRAL, confidence=0,
                reasoning=f"Error: {str(e)[:100]}",
                sources_analyzed=0, latency_ms=latency,
                error=str(e)[:200],
            )

    def _parse_llm_response(
        self,
        content: str,
        agent_name: str,
    ) -> Tuple[Signal, int, str, Dict[str, Any]]:
        """
        Parse LLM text response into structured signal.
        Handles JSON, partial JSON, and plain text responses.
        """
        # Try to extract JSON from response
        try:
            # Find JSON in response (may have text around it)
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                json_str = content[json_start:json_end]
                data = json.loads(json_str)

                # Extract signal
                sig_str = data.get("signal", "HOLD").upper().strip()
                if sig_str == "BUY":
                    signal = Signal.BUY
                elif sig_str == "SELL":
                    signal = Signal.SELL
                else:
                    signal = Signal.HOLD

                confidence = max(0, min(100, int(data.get("confidence", 50))))
                reasoning = data.get("reasoning", data.get("key_finding", "No reasoning provided"))
                sources = data.get("sources_analyzed", 3)

                return signal, confidence, reasoning, {
                    "sources_analyzed": sources,
                    "key_finding": data.get("key_finding", ""),
                    "veto": data.get("veto", False),
                    "raw_llm": data,
                }

        except (json.JSONDecodeError, ValueError, TypeError) as e:
            pass

        # Fallback: try to extract signal from plain text
        content_upper = content.upper()
        if "BUY" in content_upper and "SELL" not in content_upper:
            signal = Signal.BUY
        elif "SELL" in content_upper and "BUY" not in content_upper:
            signal = Signal.SELL
        else:
            signal = Signal.HOLD

        return signal, 40, content[:200], {"raw_text": content[:500], "parse_error": True}

    def _build_consensus(
        self,
        results: List[SuperAgentResult],
        veto_active: bool,
    ) -> Tuple[Signal, int, str]:
        """Build consensus from all agent results."""
        bullish = sum(1 for r in results if r.signal == Signal.BUY)
        bearish = sum(1 for r in results if r.signal == Signal.SELL)
        neutral = sum(1 for r in results if r.signal in [Signal.HOLD, Signal.NEUTRAL])
        total = len(results)

        if veto_active:
            return (Signal.SELL, 100, "Risk Guardian VETO: Portfolio risk exceeds limits")

        if bullish > bearish:
            signal = Signal.BUY
        elif bearish > bullish:
            signal = Signal.SELL
        else:
            signal = Signal.HOLD

        dominant = max(bullish, bearish)
        if total > 0:
            conviction = int((dominant / total) * 100)
        else:
            conviction = 0

        if dominant >= 11:
            conviction = 95
        elif dominant >= 10:
            conviction = 85
        elif dominant >= 9:
            conviction = 75
        elif dominant >= 8:
            conviction = 65

        parts = [f"{bullish} bullish", f"{bearish} bearish", f"{neutral} neutral"]
        if bullish >= 8:
            parts.append("Strong bullish consensus")
        elif bearish >= 8:
            parts.append("Strong bearish consensus")
        else:
            parts.append("Mixed signals")

        return signal, conviction, " | ".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "swarm_name": "Meteoro Swarm v7.2",
            "total_agents": len(self.agent_configs),
            "swarms": {
                "alpha": ["Satellite Recon", "Maritime Intel", "Supply Chain Mapper"],
                "bravo": ["LatAm OSINT", "China Demand Oracle", "Geopolitical Risk"],
                "charlie": ["Macro Regime Detector", "Quantitative Alpha", "Sentiment & Flow"],
                "delta": ["Risk Guardian", "Execution Engine", "Counterintelligence"],
            },
            "data_sources": "yfinance + GDELT (real data)",
            "llm_models": "DeepSeek V3 + Gemini Flash",
            "real_data": HAS_MARKET_DATA,
            "real_llm": HAS_LLM,
        }


async def main():
    swarm = MeteorSwarm()
    print(json.dumps(swarm.to_dict(), indent=2))
    signal = await swarm.analyze("COFFEE")
    print(f"\nFinal: {signal.final_signal.value} ({signal.conviction}%)")
    print(f"Cost: ${signal.cost_usd:.4f} | Latency: {signal.total_latency_ms:.0f}ms")


if __name__ == "__main__":
    asyncio.run(main())
