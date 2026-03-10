#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  METEORO SWARM v13 — Autonomous Learning Intelligence             ║
║  Agentic System | Memory | Knowledge Graph | Correspondents       ║
║                                                                    ║
║  FLOW:                                                            ║
║    1. Fetch REAL market data (yfinance, GDELT, macro)            ║
║    2. Inject INDUSTRY KNOWLEDGE (mines, smelters, traders, exchanges)
║    3. Route agents to LLMs (Claude/DeepSeek/Kimi/Gemini)        ║
║    4. Parse LLM response → Signal + Confidence + Evidence        ║
║    5. Parallel batch execution (Alpha/Bravo/Charlie teams)       ║
║    6. Consensus mechanism → Final trade signal                    ║
║                                                                    ║
║  Each agent has specialized prompt + real data + industry context ║
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

# Industry Knowledge Graph
try:
    from data_sources.industry_knowledge import build_agent_context_prompt, get_commodity_context
    HAS_KNOWLEDGE = True
except Exception as e:
    HAS_KNOWLEDGE = False
    print(f"[WARN] industry_knowledge import failed: {e}")

# Autonomous Correspondents Network
try:
    from data_sources.correspondents import build_commodity_correspondent_prompt
    HAS_CORRESPONDENTS = True
except Exception as e:
    HAS_CORRESPONDENTS = False
    print(f"[WARN] correspondents import failed: {e}")

# Autonomous Memory System — learns, remembers, auto-calibrates
try:
    from memory.autonomous_memory import AutonomousMemory
    HAS_MEMORY = True
except Exception as e:
    HAS_MEMORY = False
    print(f"[WARN] autonomous_memory import failed: {e}")


# ═══════════════════════════════════════════════════════════════
# AGENT PROMPTS — Each agent's specialized instructions
# ═══════════════════════════════════════════════════════════════

AGENT_CONFIGS = [
    {
        "id": 1, "name": "Satellite Recon", "router_name": "satellite_recon",
        "prompt": """You are a physical supply intelligence analyst for commodity trading.
Your lens: PHYSICAL SUPPLY DISRUPTIONS inferred from price data and market signals.

ANALYSIS FRAMEWORK:
1. Price volatility spikes → potential supply disruption (weather, strikes, infrastructure failure)
2. Sudden price gaps → inventory shock or logistics breakdown
3. Annualized volatility regime: <15% calm, 15-25% elevated, >25% crisis-level
4. Seasonal patterns vs anomalies in current price action
5. Producer FX weakness → margin pressure → potential supply curtailment

CONFIDENCE CALIBRATION:
- 70-85: Clear physical supply signal confirmed by multiple indicators
- 55-69: Probable signal but needs confirmation
- 40-54: Weak or ambiguous signal
- Never go above 85 (you lack real satellite/physical data)

Be DECISIVE. Use BUY when supply disruption = bullish. SELL when oversupply signals clear.
Avoid defaulting to HOLD — take a position when data supports it.

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"2-3 sentences max with specific numbers from the data","sources_analyzed":5,"key_finding":"ONE actionable sentence — e.g. 'Volatility spike to 28% signals supply disruption; bullish bias above $X'"}"""
    },
    {
        "id": 2, "name": "Maritime Intel", "router_name": "maritime_intel",
        "prompt": """You are a maritime trade flow analyst for commodity trading.
Your lens: SHIPPING, TRADE ROUTES, and LOGISTICS from market signals.

ANALYSIS FRAMEWORK:
1. Baltic Dry Index (if available): rising = increasing shipping demand = bullish commodities
2. FX movements in exporter currencies (BRL, CLP, AUD) → trade flow competitiveness
3. Price contango → storage/shipping costs rising → logistics stress
4. Volume spikes → large physical shipments or stockpiling
5. Commodity-DXY divergence → changing trade flow dynamics

CONFIDENCE CALIBRATION:
- 70-85: Strong maritime/trade signal from multiple data points
- 55-69: Moderate signal
- 40-54: Weak signal
- Never exceed 85 without actual shipping data

Be DECISIVE. Take a directional view. Your key_finding should be actionable and specific.

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"2-3 sentences with specific data points","sources_analyzed":5,"key_finding":"ONE actionable sentence with price levels or percentages"}"""
    },
    {
        "id": 3, "name": "Supply Chain Mapper", "router_name": "supply_chain_mapper",
        "prompt": """You are a supply chain dynamics analyst for commodity trading.
Your lens: PRODUCTION, INVENTORY, and LOGISTICS COST signals.

ANALYSIS FRAMEWORK:
1. Price vs SMA-20: sustained above = demand > supply; below = oversupply
2. Contango (futures > spot) = ample supply/storage; backwardation = tight supply
3. Producer currency strength/weakness → production cost changes
4. Volatility clustering → supply chain instability
5. Cross-commodity correlations → substitution effects

CONFIDENCE CALIBRATION: 40-85 range. Be specific. Be directional.

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"2-3 sentences with numbers","sources_analyzed":5,"key_finding":"ONE sentence — e.g. 'Supply tightness confirmed: price 2.1% above SMA-20 with backwardation structure'"}"""
    },
    {
        "id": 4, "name": "LatAm OSINT", "router_name": "latam_osint",
        "prompt": """You are a Latin American open-source intelligence analyst for commodity trading.
Your lens: LATAM PRODUCER DYNAMICS — currencies, politics, trade policy.

ANALYSIS FRAMEWORK:
1. COP (Colombia): coffee, coal, oil. BRL (Brazil): iron, soy, coffee. CLP (Chile): copper, lithium. PEN (Peru): copper, gold, zinc. MXN (Mexico): silver, oil.
2. Currency depreciation in producer country = lower production costs in USD = bearish
3. Currency volatility >2% daily = political instability risk = supply disruption = bullish
4. FX divergence between producer vs consumer currencies = trade flow shift
5. EM FX selloff (broad) = risk-off, capital flight = mixed for commodities

KEY: Always tie LatAm FX moves back to specific commodity supply/demand implications.

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"2-3 sentences linking LatAm dynamics to commodity","sources_analyzed":5,"key_finding":"ONE sentence — e.g. 'CLP -1.8% signals Chilean copper producer margin pressure; watch for output cuts'"}"""
    },
    {
        "id": 5, "name": "China Demand Oracle", "router_name": "china_demand_oracle",
        "prompt": """You are a China demand specialist for commodity trading.
Your lens: CHINESE DEMAND SIGNALS — the world's largest commodity consumer.

ANALYSIS FRAMEWORK:
1. USD/CNY: weakening CNY = PBOC easing = stimulus = bullish demand. Strengthening CNY = tightening.
2. Commodity price vs CNY: divergence = stockpiling or destocking cycle
3. Industrial metals (copper, iron, nickel) most sensitive to China construction/manufacturing
4. Seasonal patterns: Q1 restocking, Q2-Q3 construction peak, Q4 pre-holiday stockpiling
5. Gold demand from China = both jewelry (consumer) and reserve accumulation (central bank)

CONFIDENCE CALIBRATION: 55-80 range. China signals are noisy — don't be overconfident.

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"2-3 sentences with CNY data","sources_analyzed":5,"key_finding":"ONE sentence — e.g. 'CNY weakening + copper price resilience = Chinese strategic stockpiling likely'"}"""
    },
    {
        "id": 6, "name": "Geopolitical Risk Assessor", "router_name": "geopolitical_risk",
        "prompt": """You are a geopolitical risk analyst for commodity trading.
Your lens: CONFLICTS, SANCTIONS, POLITICAL INSTABILITY affecting supply chains.

ANALYSIS FRAMEWORK:
1. News sentiment (GDELT tone): negative tone in producer regions = supply risk premium
2. VIX as geopolitical fear gauge: VIX>25 = elevated risk environment
3. Gold as safe-haven proxy: rising gold + falling equities = geopolitical risk bid
4. Oil-specific: Middle East news, OPEC policy, Russia/sanctions
5. FX volatility in producer nations = political instability signal

SEVERITY SCALE:
- Low (VIX<20, neutral news): HOLD, confidence 40-55
- Moderate (VIX 20-30, negative news): directional bias, confidence 55-70
- High (VIX>30, crisis news): strong signal, confidence 70-85

Use the ACTUAL news articles provided. Reference specific events in your reasoning.

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"2-3 sentences referencing specific news/data","sources_analyzed":5,"key_finding":"ONE sentence — e.g. 'Elevated VIX (26.1) + negative GDELT tone in Peru = copper supply risk premium building'"}"""
    },
    {
        "id": 7, "name": "Macro Regime Detector", "router_name": "macro_regime",
        "prompt": """You are a macro regime analyst for commodity trading.
Your lens: MACROECONOMIC REGIME — inflation, rates, liquidity, risk appetite.

REGIME CLASSIFICATION (choose one):
- RISK-ON: VIX<18, DXY falling, yields stable/falling, equities rising → bullish commodities
- RISK-OFF: VIX>25, DXY rising, equities falling → bearish most commodities, bullish gold
- INFLATIONARY: yields rising, DXY falling, commodities rising → bullish commodities
- DEFLATIONARY: yields falling, DXY rising, commodities falling → bearish commodities
- TRANSITIONAL: mixed signals, no clear regime → HOLD

KEY RULES WITH NUMBERS:
- DXY>105: strong headwind for commodities. DXY<100: tailwind
- VIX<15: complacent (contrarian bearish). VIX 15-25: normal. VIX>25: fear (bearish risk assets, bullish gold)
- US10Y-US2Y spread: positive = growth. Inverted = recession risk
- S&P500 down + VIX up + Gold up = classic risk-off (bullish gold/silver, bearish industrial metals)

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"2-3 sentences with exact numbers from data","sources_analyzed":5,"key_finding":"ONE sentence — e.g. 'RISK-OFF regime: VIX at 26.1 (+9.7%), DXY at 104.2; bearish industrial metals, bullish gold'"}"""
    },
    {
        "id": 8, "name": "Quantitative Alpha", "router_name": "quant_alpha",
        "prompt": """You are a quantitative analyst for commodity trading.
Your lens: TECHNICAL ANALYSIS using real price data. Numbers only — no narratives.

QUANTITATIVE FRAMEWORK:
1. RSI: <30 oversold (BUY signal), 30-70 neutral, >70 overbought (SELL signal)
2. Price vs SMA-10: above = short-term bullish momentum; below = bearish
3. Price vs SMA-20: above = medium-term uptrend; below = downtrend
4. Golden cross (SMA-10 > SMA-20) = bullish. Death cross (SMA-10 < SMA-20) = bearish
5. Volatility regime: <15% low, 15-25% normal, 25-35% high, >35% extreme
6. Volume: spike >2x average = institutional activity, confirms direction
7. Distance from SMA-20: >5% = extended (mean reversion likely), <2% = consolidation

SIGNAL LOGIC:
- BUY: RSI<40 + price near/above SMA-20 + volume confirmation
- SELL: RSI>65 + price below SMA-10 + high volatility
- HOLD: RSI 45-55 + price between SMAs + low volume

Use the EXACT numbers provided. Calculate signals mathematically.

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"Quote specific numbers: RSI=X, price=$Y vs SMA10=$Z","sources_analyzed":5,"key_finding":"ONE sentence with numbers — e.g. 'RSI 42 + price above SMA-20 ($5.08) = bullish momentum; target SMA-10 at $5.17'"}"""
    },
    {
        "id": 9, "name": "Sentiment & Flow", "router_name": "sentiment_flow",
        "prompt": """You are a market sentiment and flow analyst for commodity trading.
Your lens: POSITIONING, SENTIMENT, CROWDING from news and price patterns.

SENTIMENT FRAMEWORK:
1. GDELT news tone: strongly negative (<-3) = fear = contrarian bullish. Positive (>3) = complacency = risk of reversal
2. Volume spike + price up = bullish conviction. Volume spike + price down = distribution/panic
3. Extreme RSI + high volume = crowded trade, reversal risk
4. VIX/equity divergence from commodity = sentiment disconnect (opportunity)
5. Gold vs industrial metals ratio: rising = defensive positioning

CONTRARIAN SIGNALS (these override trend-following):
- Everyone bullish (RSI>70 + high volume + positive news) = SELL (crowded long)
- Everyone bearish (RSI<30 + high volume + negative news) = BUY (capitulation)

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"2-3 sentences on sentiment signals","sources_analyzed":5,"key_finding":"ONE sentence — e.g. 'Volume 15x avg + neutral RSI = institutional accumulation; smart money building positions'"}"""
    },
    {
        "id": 10, "name": "Risk Guardian", "router_name": "risk_guardian",
        "prompt": """You are the risk management system with VETO POWER.
Your role: PROTECT CAPITAL. Override the swarm only when risk is genuinely elevated.

VETO CRITERIA (use sparingly — only 1 in 5 analyses should trigger veto):
1. VIX>30 AND majority signal is BUY → VETO (extreme fear, not the time to go long)
2. RSI>75 AND majority is BUY → VETO (overbought, reversal imminent)
3. RSI<25 AND majority is SELL → VETO (oversold, bounce likely)
4. Volatility >40% annualized AND any directional signal → VETO (too unpredictable)

APPROVAL (default — let good trades through):
- If data supports the majority signal → APPROVE (match majority signal)
- If signals are mixed but macro is clear → align with macro
- If genuinely uncertain (50/50 split) → HOLD

Do NOT be a perma-bear. Your job is risk management, not prediction.

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"2-3 sentences justifying approval or veto","sources_analyzed":5,"key_finding":"ONE sentence — e.g. 'APPROVED: VIX 24.3 within normal range, RSI 53 neutral — no grounds for veto'","veto":false}"""
    },
    {
        "id": 11, "name": "Execution Engine", "router_name": "execution_engine",
        "prompt": """You are the execution strategy engine for commodity trading.
Your role: TRANSLATE the consensus signal into an actionable trade plan.

EXECUTION FRAMEWORK:
1. Entry: current price ± slippage (0.1-0.3% for liquid commodities, 0.5-1% for illiquid)
2. Stop loss: based on volatility — use 1.5x ATR or nearest support/resistance
3. Take profit: risk/reward minimum 2:1. Use SMA levels as targets.
4. Position size: inversely proportional to volatility (Kelly criterion simplified)
   - Low vol (<15%): 5-8% of portfolio. Normal (15-25%): 3-5%. High (>25%): 1-3%
5. Time horizon: based on SMA crossover timeframe (10-day signals = 1-2 weeks)

Use the ACTUAL price and technical data to calculate levels.

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"Entry at $X, stop at $Y, target $Z, size N%","sources_analyzed":5,"key_finding":"ONE sentence — e.g. 'BUY entry $5.08, stop $4.92 (-3.1%), target $5.29 (+4.1%), R:R 1.3:1, size 3%'","entry_price":0,"stop_loss":0,"take_profit":0,"position_size_pct":0}"""
    },
    {
        "id": 12, "name": "Counterintelligence", "router_name": "counterintelligence",
        "prompt": """You are the counterintelligence and validation analyst.
Your role: CHALLENGE the consensus. Find blind spots. Calibrate confidence.

VALIDATION FRAMEWORK:
1. Data quality: are key indicators actually available or was the analysis based on limited data?
2. Conflicting signals: identify the strongest counter-argument to the consensus
3. Confidence calibration: if most agents agree at high confidence, consider if they're echoing each other
4. Missing data: what information WOULD change the signal? (e.g., actual COT data, physical inventory)
5. Time sensitivity: is this signal time-critical or can it wait for confirmation?

ADVERSARIAL APPROACH:
- If consensus is BUY → argue the bear case. What could go wrong?
- If consensus is SELL → argue the bull case. What's being missed?
- If consensus is HOLD → is the uncertainty real or are agents being lazy?

Your signal should reflect YOUR assessment after adversarial analysis.

Output ONLY valid JSON:
{"signal":"BUY|SELL|HOLD","confidence":0-100,"reasoning":"2-3 sentences of adversarial analysis","sources_analyzed":5,"key_finding":"ONE sentence with the key risk — e.g. 'Blind spot: no physical inventory data; apparent bullish consensus may reverse on actual stock reports'"}"""
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
    Meteoro Swarm v9.3 — Real Data + Real LLM Analysis.
    Agentic system with multi-model intelligence doing actual analysis.
    Supports parallel batch execution when 2+ providers available.
    """

    TIME_BUDGET_MS = 120_000  # 120 seconds total
    AGENT_TIMEOUT_MS = 45_000  # 45 seconds per agent (allows for rate limit retries: 3s + 6s + 12s backoff)
    AGENT_SPACING_S = 4.0  # seconds between sequential calls (Gemini 15 RPM = 1 req/4s)

    def __init__(self):
        self.agent_configs = AGENT_CONFIGS

        # Initialize autonomous memory
        self.memory = None
        if HAS_MEMORY:
            try:
                self.memory = AutonomousMemory()
                print("[MEMORY] Autonomous memory system initialized")
            except Exception as me:
                print(f"[MEMORY] Failed to initialize: {me}")
                self.memory = None

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
        print(f"METEORO SWARM v12 — AUTONOMOUS INTELLIGENCE — {session_id}")
        print(f"Commodity: {commodity}")
        print(f"{'='*70}\n")

        # ─── STEP 0: INDUSTRY KNOWLEDGE CONTEXT ──────────────────
        industry_context = ""
        if HAS_KNOWLEDGE:
            try:
                industry_context = build_agent_context_prompt(commodity)
                if industry_context and "No specific" not in industry_context:
                    print(f"[KNOWLEDGE] Industry context loaded for {commodity}")
                else:
                    print(f"[KNOWLEDGE] No specific industry context for {commodity}")
            except Exception as ke:
                print(f"[KNOWLEDGE] Error building context: {ke}")

        # ─── STEP 0.5: CORRESPONDENT NETWORK ────────────────────
        correspondent_context = ""
        if HAS_CORRESPONDENTS:
            try:
                correspondent_context = build_commodity_correspondent_prompt(commodity)
                if correspondent_context:
                    print(f"[CORRESPONDENTS] Local intelligence loaded for {commodity}")
            except Exception as ce:
                print(f"[CORRESPONDENTS] Error: {ce}")

        # ─── STEP 0.7: MEMORY CONTEXT ─────────────────────────────
        memory_context = ""
        if self.memory:
            try:
                memory_context = await self.memory.get_memory_context(commodity)
                if memory_context:
                    print(f"[MEMORY] Historical context loaded for {commodity}")
            except Exception as me:
                print(f"[MEMORY] Error loading context: {me}")

        # Store context for agents to access
        self._industry_context = industry_context
        self._correspondent_context = correspondent_context
        self._memory_context = memory_context

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
                market_data = {"error": str(e), "note": "Using LLM knowledge as fallback"}
        else:
            print("[DATA] market_data module not available")

        # Fallback context: ensure agents always have something to analyze
        if not market_data or "error" in market_data:
            market_data = market_data or {}
            market_data["fallback"] = True
            market_data["context"] = (
                f"Analyze {commodity} commodity markets using your training knowledge. "
                f"Focus on: current supply-demand dynamics, geopolitical risks, price trends, "
                f"and any recent developments that could impact {commodity} prices. "
                f"Provide your best assessment based on known market conditions."
            )

        # ─── STEP 2: RUN ALL AGENTS WITH LLM ─────────────────────
        if not HAS_LLM:
            print("[ERROR] LLM router not available — cannot run swarm")
            raise RuntimeError("LLM router not available")

        # Build data string for agents — compact but complete
        data_str = json.dumps(market_data, default=str, ensure_ascii=False)
        # Truncate if too long (LLM context limits)
        if len(data_str) > 10000:
            data_str = data_str[:10000] + "...[truncated]"

        all_results: List[SuperAgentResult] = []

        # Detect how many providers are available for parallel execution
        try:
            from multi_model_router import _get_available_models
            available_models = _get_available_models()
            n_providers = len(available_models)
        except Exception:
            available_models = set()
            n_providers = 1

        # Determine if we have multi-provider capability
        # With 2+ providers (e.g., Anthropic + Gemini): run agent BATCHES in parallel
        # With 1 provider (e.g., Gemini only at 15 RPM): sequential with spacing
        multi_provider = n_providers >= 2

        if multi_provider:
            # ── PARALLEL BATCH MODE ─────────────────────────────────
            # Run 3 batches of 3 agents each (like Navy SEAL teams)
            # Each batch runs concurrently; batches run sequentially with gap
            # Router semaphores serialize per-provider access within each batch
            BATCH_GAP_S = 4.0  # gap between batches (provider rate limit breathing room)
            STAGGER_S = 1.0    # stagger within batch (avoid simultaneous provider hits)
            batches = [
                ("ALPHA", self.agent_configs[:3]),   # Physical: Satellite, Maritime, Supply Chain
                ("BRAVO", self.agent_configs[3:6]),   # Regional: LatAm, China, Geopolitical
                ("CHARLIE", self.agent_configs[6:9]), # Quant: Macro, Quant, Sentiment
            ]
            n_agents = len(self.agent_configs[:9])
            print(f"\n[SWARM] Deploying intelligence capabilities in 3 parallel batches (providers: {n_providers})...")

            for batch_name, batch_configs in batches:
                if all_results:  # Gap between batches (not before first)
                    await asyncio.sleep(BATCH_GAP_S)

                print(f"\n  [BATCH {batch_name}] Deploying {len(batch_configs)} agents...")

                async def _staggered_agent(idx, cfg):
                    """Launch agent with stagger delay to avoid burst."""
                    if idx > 0:
                        await asyncio.sleep(STAGGER_S * idx)
                    return await self._run_llm_agent(cfg, commodity, data_str, context)

                tasks = [
                    _staggered_agent(i, config)
                    for i, config in enumerate(batch_configs)
                ]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                for j, result in enumerate(batch_results):
                    config = batch_configs[j]
                    if isinstance(result, Exception):
                        print(f"    [Agent ERROR] {config['name']}: {result}")
                        all_results.append(SuperAgentResult(
                            agent_id=config["id"], agent_name=config["name"],
                            signal=Signal.NEUTRAL, confidence=0,
                            reasoning=f"Error: {str(result)[:100]}",
                            sources_analyzed=0,
                        ))
                    else:
                        all_results.append(result)

                print(f"  [BATCH {batch_name}] Complete — {len(batch_configs)} agents done")

        else:
            # ── SEQUENTIAL MODE (single provider, rate limited) ─────
            print(f"\n[SWARM] Deploying intelligence capabilities sequentially (providers: {n_providers}, spacing: {self.AGENT_SPACING_S}s)...")

            for i, config in enumerate(self.agent_configs[:9]):
                # Space calls to avoid Gemini 429
                if i > 0:
                    await asyncio.sleep(self.AGENT_SPACING_S)

                try:
                    result = await self._run_llm_agent(config, commodity, data_str, context)
                    all_results.append(result)
                except Exception as e:
                    print(f"  [Agent ERROR] {config['name']}: {e}")
                    all_results.append(SuperAgentResult(
                        agent_id=config["id"], agent_name=config["name"],
                        signal=Signal.NEUTRAL, confidence=0,
                        reasoning=f"Error: {str(e)[:100]}",
                        sources_analyzed=0,
                    ))

        # ─── STEP 3: RISK GUARDIAN (with all previous results) ────
        await asyncio.sleep(2.0 if multi_provider else self.AGENT_SPACING_S)
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

        # ─── STEP 4: EXECUTION + COUNTERINTEL ───────────────────
        if multi_provider:
            # Run both in parallel
            await asyncio.sleep(2.0)
            print("\n[DELTA] Execution + Counterintelligence (parallel)...")
            exec_tasks = [
                self._run_llm_agent(config, commodity, data_str, context)
                for config in self.agent_configs[10:12]
            ]
            exec_results = await asyncio.gather(*exec_tasks, return_exceptions=True)
            for j, result in enumerate(exec_results):
                config = self.agent_configs[10 + j]
                if isinstance(result, Exception):
                    all_results.append(SuperAgentResult(
                        agent_id=config["id"], agent_name=config["name"],
                        signal=Signal.NEUTRAL, confidence=0,
                        reasoning=str(result)[:100], sources_analyzed=0,
                    ))
                else:
                    all_results.append(result)
        else:
            # Sequential with spacing
            print("\n[DELTA] Execution + Counterintelligence...")
            for config in self.agent_configs[10:12]:  # Agents 11-12
                await asyncio.sleep(self.AGENT_SPACING_S)
                try:
                    r = await self._run_llm_agent(config, commodity, data_str, context)
                    all_results.append(r)
                except Exception as e:
                    all_results.append(SuperAgentResult(
                        agent_id=config["id"], agent_name=config["name"],
                        signal=Signal.NEUTRAL, confidence=0,
                        reasoning=str(e)[:100], sources_analyzed=0,
                    ))

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

        # ─── STEP 6: PERSIST TO AUTONOMOUS MEMORY ─────────────────
        if self.memory:
            try:
                # Extract price from market data
                price_at_signal = 0.0
                cd = market_data.get("commodity", {})
                if isinstance(cd, dict):
                    price_at_signal = cd.get("price", 0.0)

                # Build agent results list for memory
                agent_results_for_memory = []
                for r in all_results:
                    agent_results_for_memory.append({
                        "agent_name": r.agent_name,
                        "signal": r.signal.value,
                        "confidence": r.confidence,
                        "model_used": r.metadata.get("model_used", "unknown"),
                        "provider": r.metadata.get("provider", "unknown"),
                        "latency_ms": r.latency_ms,
                    })

                signal_id = await self.memory.save_signal({
                    "commodity": commodity,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "final_signal": final_signal.value,
                    "conviction": conviction,
                    "reasoning": reasoning[:500],
                    "agents_bullish": sum(1 for r in all_results if r.signal == Signal.BUY),
                    "agents_bearish": sum(1 for r in all_results if r.signal == Signal.SELL),
                    "agents_neutral": sum(1 for r in all_results if r.signal in [Signal.HOLD, Signal.NEUTRAL]),
                    "risk_guardian_veto": veto_active,
                    "total_latency_ms": total_latency,
                    "cost_usd": total_cost,
                    "session_id": session_id,
                    "price_at_signal": price_at_signal,
                    "agent_results": agent_results_for_memory,
                })

                # Record individual agent performance
                for r in all_results:
                    await self.memory.record_agent_performance(
                        commodity=commodity,
                        agent_name=r.agent_name,
                        signal=r.signal.value,
                        confidence=r.confidence,
                    )

                # Snapshot the price
                if price_at_signal > 0:
                    await self.memory.snapshot_price(
                        commodity=commodity,
                        price=price_at_signal,
                        signal_id=signal_id,
                    )

                print(f"[MEMORY] Signal #{signal_id} persisted with {len(all_results)} agent records")

                # Async: try to update outcomes from past signals (non-blocking)
                try:
                    outcome_result = await self.memory.update_outcomes(days=5)
                    if outcome_result.get("updated", 0) > 0:
                        print(f"[MEMORY] Updated {outcome_result['updated']} past signal outcomes")
                except Exception:
                    pass  # Non-critical, will retry next analysis

            except Exception as mem_err:
                print(f"[MEMORY] Error persisting signal: {mem_err}")

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
            metadata={
                "session_id": session_id,
                "has_real_data": HAS_MARKET_DATA,
                "market_data": market_data,
                "execution_mode": "parallel" if multi_provider else "sequential",
            },
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
            # Build industry context block if available
            industry_block = ""
            if hasattr(self, '_industry_context') and self._industry_context:
                industry_block = f"""
INDUSTRY INTELLIGENCE (proprietary knowledge graph — mines, ports, smelters, traders, clients, shipping, QA):
{self._industry_context}
"""

            # Build correspondent context block if available
            correspondent_block = ""
            if hasattr(self, '_correspondent_context') and self._correspondent_context:
                correspondent_block = f"""
LOCAL INTELLIGENCE (autonomous correspondent network — producer country press, gazettes, alerts):
{self._correspondent_context}
"""

            # Build memory context block if available
            memory_block = ""
            if hasattr(self, '_memory_context') and self._memory_context:
                memory_block = f"""
HISTORICAL MEMORY (autonomous learning system — past signals, track record, agent calibration):
{self._memory_context}
"""

            user_message = f"""COMMODITY: {commodity}

REAL MARKET DATA (from yfinance + GDELT, fetched just now):
{data_str}
{industry_block}{correspondent_block}{memory_block}
INSTRUCTIONS:
1. Analyze this REAL data. Use the ACTUAL numbers provided above.
2. Cross-reference with industry knowledge: who are the key players, where are the mines/smelters/ports, what exchanges set the price, who are the end buyers.
3. Consider local correspondent intelligence: what is happening on the ground in producer countries.
4. Consider historical memory: what signals did we generate before for this commodity, what was our accuracy, calibrate your confidence accordingly.
5. Do NOT invent or hallucinate data.
6. Respond with ONLY a JSON object. No text before or after the JSON.
7. The JSON must have these exact fields: signal, confidence, reasoning, sources_analyzed, key_finding."""

            # Call LLM via router (router handles rate limiting + retry internally)
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
                evidence_pack={**evidence, "model": llm_response.model_used, "provider": llm_response.provider},
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
        Handles JSON, partial JSON, markdown code blocks, and plain text responses.
        """
        if not content or not content.strip():
            return Signal.HOLD, 0, "Empty response from model", {"parse_error": True}

        # Strip markdown code block wrappers (```json ... ```)
        cleaned = content.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            # Remove first line (```json or ```) and last line (```)
            if len(lines) >= 3:
                cleaned = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])
            cleaned = cleaned.strip()

        # Try to extract JSON from response
        json_data = None

        # Attempt 1: Direct JSON parse
        try:
            json_data = json.loads(cleaned)
        except (json.JSONDecodeError, ValueError):
            pass

        # Attempt 2: Find JSON object in response
        if json_data is None:
            try:
                json_start = cleaned.find("{")
                json_end = cleaned.rfind("}") + 1
                if json_start >= 0 and json_end > json_start:
                    json_str = cleaned[json_start:json_end]
                    json_data = json.loads(json_str)
            except (json.JSONDecodeError, ValueError):
                pass

        # Attempt 3: Try to fix common JSON issues (trailing comma, unquoted keys)
        if json_data is None:
            try:
                json_start = cleaned.find("{")
                json_end = cleaned.rfind("}") + 1
                if json_start >= 0 and json_end > json_start:
                    json_str = cleaned[json_start:json_end]
                    # Remove trailing commas before } or ]
                    import re
                    json_str = re.sub(r',\s*([}\]])', r'\1', json_str)
                    json_data = json.loads(json_str)
            except (json.JSONDecodeError, ValueError, ImportError):
                pass

        if json_data and isinstance(json_data, dict):
            # Extract signal
            sig_str = str(json_data.get("signal", "HOLD")).upper().strip()
            if sig_str in ("BUY", "LONG", "BULLISH"):
                signal = Signal.BUY
            elif sig_str in ("SELL", "SHORT", "BEARISH"):
                signal = Signal.SELL
            else:
                signal = Signal.HOLD

            confidence = max(0, min(100, int(json_data.get("confidence", 50))))
            reasoning = str(json_data.get("reasoning", json_data.get("key_finding", "No reasoning provided")))
            sources = int(json_data.get("sources_analyzed", 3))
            key_finding = str(json_data.get("key_finding", ""))

            return signal, confidence, reasoning, {
                "sources_analyzed": sources,
                "key_finding": key_finding,
                "veto": json_data.get("veto", False),
                "raw_llm": json_data,
            }

        # Fallback: try to extract signal from plain text
        content_upper = content.upper()
        # Count occurrences for better signal detection
        buy_count = content_upper.count("BUY") + content_upper.count("LONG") + content_upper.count("BULLISH")
        sell_count = content_upper.count("SELL") + content_upper.count("SHORT") + content_upper.count("BEARISH")

        if buy_count > sell_count:
            signal = Signal.BUY
        elif sell_count > buy_count:
            signal = Signal.SELL
        else:
            signal = Signal.HOLD

        # Extract a reasonable confidence from text
        confidence = 45 if signal != Signal.HOLD else 30

        return signal, confidence, content[:200], {"raw_text": content[:500], "parse_error": True}

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
