"""
METEORO X — Unified Data-to-Signal Pipeline
============================================
The brain of Meteoro. Takes a CEO command, deploys all data sources
in parallel, synthesizes intelligence, and produces an EvidencePack
with a tradeable signal.

This pipeline uses REAL data (no mocks) from:
- GDELT (geopolitical events, 300K articles/day)
- NASA FIRMS (satellite thermal anomalies)
- yfinance/FRED (macro data, prices)
- LatAm scrapers (local news)
- AIS tracker (maritime intelligence)
- DuckDuckGo (general web search)

Architecture: Data Sources → Intelligence Synthesis → Signal Generation → EvidencePack
"""

import asyncio
import hashlib
import json
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional
import sys
import os

# Add parent to path for data_sources imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_sources.gdelt_monitor import scan_commodity_disruptions, query_disruption
from data_sources.fred_api import get_macro_snapshot, get_coal_data
from data_sources.nasa_firms import scan_all_sites, monitor_site
from data_sources.latam_scraper import scan_latam_disruptions, check_government_gazettes
from data_sources.ais_tracker import scan_all_ports, monitor_port


# ═══════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════

@dataclass
class IntelligenceReport:
    """Output from a single intelligence capability."""
    capability: str
    summary: str
    signals: list = field(default_factory=list)
    raw_data: dict = field(default_factory=dict)
    sources: list = field(default_factory=list)
    confidence: float = 0.0
    timestamp: str = ""
    latency_ms: int = 0


@dataclass
class TradeSignal:
    """A synthesized trading signal with full evidence."""
    direction: str  # LONG, SHORT, ESPERAR
    ticker: str
    ticker_name: str
    conviction: float  # 0-100
    consensus: str  # "X/6 capabilities agree"
    entry_price: float = 0.0
    stop_loss_pct: float = 0.0
    take_profit_pct: float = 0.0
    kelly_fraction: float = 0.0
    horizon: str = ""
    reasoning: str = ""


@dataclass
class EvidencePack:
    """Immutable evidence package for every signal Meteoro generates."""
    pack_id: str
    timestamp: str
    command: str
    what_happened: str
    why_it_matters: str
    signal: Optional[TradeSignal]
    intelligence_reports: list
    causal_chain: list
    evidence_cards: list
    risk_assessment: dict
    pack_hash: str = ""

    def compute_hash(self):
        """SHA-256 hash for immutability."""
        content = json.dumps({
            "pack_id": self.pack_id,
            "timestamp": self.timestamp,
            "command": self.command,
            "what_happened": self.what_happened,
            "signal_direction": self.signal.direction if self.signal else "NONE",
            "signal_ticker": self.signal.ticker if self.signal else "NONE",
            "conviction": self.signal.conviction if self.signal else 0,
        }, sort_keys=True)
        self.pack_hash = hashlib.sha256(content.encode()).hexdigest()
        return self.pack_hash


# ═══════════════════════════════════════════════════════════════
# COMMAND CLASSIFIER
# ═══════════════════════════════════════════════════════════════

# Commodity keyword mappings
COMMODITY_MAP = {
    "carbon": {"tickers": ["MTF=F"], "name": "Thermal Coal", "category": "coal",
               "gdelt": "coal_colombia", "firms_sites": ["cerrejon", "drummond"],
               "ports": ["puerto_bolivar", "santa_marta"]},
    "coal": {"tickers": ["MTF=F"], "name": "Thermal Coal", "category": "coal",
             "gdelt": "coal_colombia", "firms_sites": ["cerrejon", "drummond"],
             "ports": ["puerto_bolivar", "santa_marta"]},
    "cobre": {"tickers": ["HG=F", "SCCO"], "name": "Copper", "category": "copper",
              "gdelt": "copper_peru", "firms_sites": ["las_bambas", "cerro_verde", "antamina"],
              "ports": ["callao", "matarani"]},
    "copper": {"tickers": ["HG=F", "SCCO"], "name": "Copper", "category": "copper",
               "gdelt": "copper_peru", "firms_sites": ["las_bambas", "cerro_verde", "antamina"],
               "ports": ["callao", "matarani"]},
    "petroleo": {"tickers": ["CL=F", "BZ=F"], "name": "Crude Oil", "category": "oil",
                 "gdelt": "oil_latam", "firms_sites": ["cano_limon"],
                 "ports": ["puerto_bolivar"]},
    "oil": {"tickers": ["CL=F", "BZ=F"], "name": "Crude Oil", "category": "oil",
            "gdelt": "oil_latam", "firms_sites": ["cano_limon"],
            "ports": ["puerto_bolivar"]},
    "crude": {"tickers": ["CL=F", "BZ=F"], "name": "Crude Oil", "category": "oil",
              "gdelt": "oil_latam", "firms_sites": ["cano_limon"],
              "ports": ["puerto_bolivar"]},
    "oro": {"tickers": ["GC=F", "GLD"], "name": "Gold", "category": "gold",
            "gdelt": "gold_latam", "firms_sites": ["buritica"],
            "ports": ["callao"]},
    "gold": {"tickers": ["GC=F", "GLD"], "name": "Gold", "category": "gold",
             "gdelt": "gold_latam", "firms_sites": ["buritica"],
             "ports": ["callao"]},
    "cerrejon": {"tickers": ["MTF=F", "QS=F"], "name": "Cerrejon Coal", "category": "coal",
                 "gdelt": "coal_colombia", "firms_sites": ["cerrejon"],
                 "ports": ["puerto_bolivar"]},
    "bambas": {"tickers": ["HG=F", "SCCO"], "name": "Las Bambas Copper", "category": "copper",
               "gdelt": "copper_peru", "firms_sites": ["las_bambas"],
               "ports": ["callao"]},
}


def classify_command(command: str) -> dict:
    """Classify CEO command into commodity context and data source routing."""
    cmd_lower = command.lower()

    # Find matching commodities
    matched = []
    for keyword, config in COMMODITY_MAP.items():
        if keyword in cmd_lower:
            matched.append(config)

    if not matched:
        # Default: scan everything (broad market query)
        return {
            "commodities": ["general"],
            "tickers": ["CL=F", "BZ=F", "HG=F", "GC=F"],
            "ticker_name": "Broad Commodities",
            "category": "general",
            "gdelt_queries": ["geopolitical_macro"],
            "firms_sites": ["cerrejon", "las_bambas", "buritica", "cano_limon"],
            "ports": ["puerto_bolivar", "callao", "richards_bay"],
            "scan_all": True,
        }

    # Merge all matched configs
    tickers = []
    gdelt_queries = []
    firms_sites = []
    ports = []
    categories = []
    names = []
    for config in matched:
        tickers.extend(config["tickers"])
        gdelt_queries.append(config["gdelt"])
        firms_sites.extend(config["firms_sites"])
        ports.extend(config["ports"])
        categories.append(config["category"])
        names.append(config["name"])

    return {
        "commodities": categories,
        "tickers": list(set(tickers)),
        "ticker_name": " / ".join(set(names)),
        "category": categories[0],
        "gdelt_queries": list(set(gdelt_queries)),
        "firms_sites": list(set(firms_sites)),
        "ports": list(set(ports)),
        "scan_all": False,
    }


# ═══════════════════════════════════════════════════════════════
# INTELLIGENCE CAPABILITIES (The 6 from Santo Grial)
# ═══════════════════════════════════════════════════════════════

async def run_asymmetric_intelligence(context: dict, command: str) -> IntelligenceReport:
    """Capability 1: Asymmetric Intelligence — GDELT + LatAm + non-western sources."""
    start = time.time()
    sources = []
    signals = []
    raw = {}

    try:
        # GDELT scan
        for query_type in context["gdelt_queries"]:
            gdelt_result = await query_disruption(query_type)
            if gdelt_result and gdelt_result.get("status") == "success":
                articles = gdelt_result.get("articles", [])
                raw["gdelt_articles"] = len(articles)
                raw["gdelt_sources"] = [a.get("source", "unknown") for a in articles[:5]]
                sources.extend([a.get("url", "") for a in articles[:3]])

                # Check for disruption signals
                for article in articles:
                    title = article.get("title", "").lower()
                    disruption_keywords = ["strike", "blockade", "shutdown", "protest",
                                          "sanction", "embargo", "huelga", "bloqueo",
                                          "paro", "suspension", "crisis"]
                    if any(kw in title for kw in disruption_keywords):
                        signals.append({
                            "type": "DISRUPTION_DETECTED",
                            "source": article.get("source", "GDELT"),
                            "title": article.get("title", ""),
                            "url": article.get("url", ""),
                        })

        # LatAm disruption scan
        latam_result = await scan_latam_disruptions(
            context["category"] if not context.get("scan_all") else "general"
        )
        if latam_result:
            raw["latam_sources_scanned"] = latam_result.get("sources_scanned", 0)
            raw["latam_disruptions"] = latam_result.get("disruptions_found", 0)
            if latam_result.get("disruptions"):
                for d in latam_result["disruptions"][:3]:
                    signals.append({
                        "type": "LATAM_DISRUPTION",
                        "source": d.get("source", "LatAm"),
                        "title": d.get("title", ""),
                    })

    except Exception as e:
        raw["error"] = str(e)

    latency = int((time.time() - start) * 1000)

    # Calculate confidence based on signal count and source diversity
    source_count = len(set(raw.get("gdelt_sources", [])))
    signal_count = len(signals)
    confidence = min(95, 30 + (source_count * 8) + (signal_count * 15))

    summary = f"Scanned {raw.get('gdelt_articles', 0)} GDELT articles across {source_count} sources. "
    summary += f"Found {signal_count} disruption signals. "
    summary += f"LatAm scan: {raw.get('latam_disruptions', 0)} disruptions detected."

    return IntelligenceReport(
        capability="Asymmetric Intelligence",
        summary=summary,
        signals=signals,
        raw_data=raw,
        sources=sources[:5],
        confidence=confidence,
        timestamp=datetime.now(timezone.utc).isoformat(),
        latency_ms=latency,
    )


async def run_physical_intelligence(context: dict, command: str) -> IntelligenceReport:
    """Capability 2: Physical Intelligence — NASA FIRMS + AIS."""
    start = time.time()
    sources = []
    signals = []
    raw = {}

    firms_results = {}
    port_results = {}

    try:
        # NASA FIRMS satellite monitoring
        for site_name in context["firms_sites"]:
            try:
                site_result = await monitor_site(site_name)
                if site_result and site_result.get("status") != "error":
                    firms_results[site_name] = site_result
                    anomaly = site_result.get("anomaly_level", "NORMAL")
                    if anomaly in ("SPIKE", "ELEVATED", "SEVERELY_REDUCED", "ZERO_ACTIVITY"):
                        signals.append({
                            "type": f"THERMAL_{anomaly}",
                            "source": "NASA FIRMS",
                            "site": site_name,
                            "fire_count": site_result.get("fire_count", 0),
                            "anomaly": anomaly,
                        })
                    sources.append(f"NASA FIRMS VIIRS/MODIS — {site_name}")
            except Exception:
                pass

        raw["firms_sites_monitored"] = len(firms_results)
        raw["firms_anomalies"] = len(signals)

    except Exception as e:
        raw["firms_error"] = str(e)

    try:
        # AIS maritime tracking
        for port_name in context["ports"]:
            try:
                port_result = await monitor_port(port_name)
                if port_result and port_result.get("status") != "error":
                    port_results[port_name] = port_result
                    sources.append(f"AIS Tracking — {port_name}")
            except Exception:
                pass

        raw["ports_monitored"] = len(port_results)
        raw["port_data"] = {k: v.get("summary", "") for k, v in port_results.items()}

    except Exception as e:
        raw["ais_error"] = str(e)

    latency = int((time.time() - start) * 1000)

    confidence = min(90, 25 + (len(firms_results) * 10) + (len(signals) * 20))

    summary = f"Monitored {len(firms_results)} mining sites via satellite. "
    summary += f"Detected {len(signals)} thermal anomalies. "
    summary += f"Tracked {len(port_results)} ports via AIS."

    return IntelligenceReport(
        capability="Physical Intelligence",
        summary=summary,
        signals=signals,
        raw_data=raw,
        sources=sources[:5],
        confidence=confidence,
        timestamp=datetime.now(timezone.utc).isoformat(),
        latency_ms=latency,
    )


async def run_geopolitical_intelligence(context: dict, command: str) -> IntelligenceReport:
    """Capability 3: Geopolitical Intelligence — GDELT events + government gazettes."""
    start = time.time()
    sources = []
    signals = []
    raw = {}

    try:
        # GDELT geopolitical macro scan
        geo_result = await query_disruption("geopolitical_macro")
        if geo_result and geo_result.get("status") == "success":
            articles = geo_result.get("articles", [])
            raw["geopolitical_articles"] = len(articles)

            geo_keywords = ["sanction", "tariff", "opec", "embargo", "war",
                           "conflict", "treaty", "trade war", "ban", "restrict"]
            for article in articles:
                title = article.get("title", "").lower()
                if any(kw in title for kw in geo_keywords):
                    signals.append({
                        "type": "GEOPOLITICAL_EVENT",
                        "source": article.get("source", "GDELT"),
                        "title": article.get("title", ""),
                    })
                    sources.append(article.get("url", ""))

        # Government gazette check
        gazette_result = await check_government_gazettes()
        if gazette_result:
            raw["gazettes_checked"] = gazette_result.get("gazettes_checked", 0)
            raw["gazette_alerts"] = gazette_result.get("alerts_found", 0)

    except Exception as e:
        raw["error"] = str(e)

    latency = int((time.time() - start) * 1000)
    confidence = min(85, 20 + (len(signals) * 15) + raw.get("geopolitical_articles", 0))

    summary = f"Scanned {raw.get('geopolitical_articles', 0)} geopolitical articles. "
    summary += f"Found {len(signals)} geopolitical events. "
    summary += f"Checked {raw.get('gazettes_checked', 0)} government gazettes."

    return IntelligenceReport(
        capability="Geopolitical Intelligence",
        summary=summary,
        signals=signals,
        raw_data=raw,
        sources=sources[:5],
        confidence=confidence,
        timestamp=datetime.now(timezone.utc).isoformat(),
        latency_ms=latency,
    )


async def run_macro_intelligence(context: dict, command: str) -> IntelligenceReport:
    """Capability 4: Macro Intelligence — FRED/yfinance macro data."""
    start = time.time()
    sources = []
    signals = []
    raw = {}

    try:
        # Get macro snapshot
        macro = await get_macro_snapshot()
        if macro and macro.get("status") in ("success", "OK"):
            snapshot = macro.get("snapshot", macro.get("data", {}))

            # Map FRED series IDs to friendly extraction
            series_map = {
                "VIXCLS": "vix",
                "DCOILWTICO": "wti",
                "DCOILBRENTEU": "brent",
                "GOLDAMGBD228NLBM": "gold",
                "DTWEXBGS": "dxy",
                "DGS10": "treasury_10y",
                "PCOPPUSDM": "copper",
            }

            for series_id, key in series_map.items():
                entry = snapshot.get(series_id, {})
                val = entry.get("value")
                if val is not None and not isinstance(val, str):
                    raw[key] = float(val)

            # Analyze VIX regime
            vix_val = raw.get("vix")
            if vix_val:
                if vix_val > 30:
                    signals.append({"type": "HIGH_VIX", "value": vix_val, "regime": "FEAR"})
                elif vix_val < 15:
                    signals.append({"type": "LOW_VIX", "value": vix_val, "regime": "COMPLACENCY"})

            # Analyze DXY
            dxy_val = raw.get("dxy")
            if dxy_val:
                if dxy_val > 105:
                    signals.append({"type": "STRONG_DOLLAR", "value": dxy_val})
                elif dxy_val < 95:
                    signals.append({"type": "WEAK_DOLLAR", "value": dxy_val})

            sources.append("FRED / yfinance — macro data")

        # Get commodity-specific data if coal
        if context["category"] in ("coal", "general"):
            coal = await get_coal_data()
            if coal and coal.get("status") == "success":
                raw["coal_data"] = coal.get("data", {})
                sources.append("yfinance — coal/energy data")

    except Exception as e:
        raw["error"] = str(e)

    latency = int((time.time() - start) * 1000)

    # Determine macro regime
    regime = "NEUTRAL"
    vix_val = raw.get("vix", 20)
    dxy_val = raw.get("dxy", 100)
    if vix_val > 25 and dxy_val > 103:
        regime = "RISK-OFF"
    elif vix_val < 18 and dxy_val < 100:
        regime = "RISK-ON"

    raw["regime"] = regime
    confidence = min(95, 50 + len(signals) * 10)

    summary = f"Macro regime: {regime}. VIX: {raw.get('vix', 'N/A')}. "
    summary += f"WTI: ${raw.get('wti', 'N/A')}. Gold: ${raw.get('gold', 'N/A')}. "
    summary += f"DXY: {raw.get('dxy', 'N/A')}."

    return IntelligenceReport(
        capability="Macro Intelligence",
        summary=summary,
        signals=signals,
        raw_data=raw,
        sources=sources,
        confidence=confidence,
        timestamp=datetime.now(timezone.utc).isoformat(),
        latency_ms=latency,
    )


async def run_quantitative_intelligence(context: dict, command: str) -> IntelligenceReport:
    """Capability 5: Quantitative Intelligence — price analysis, technicals."""
    start = time.time()
    sources = []
    signals = []
    raw = {}

    try:
        import yfinance as yf
        import numpy as np

        for ticker in context["tickers"][:3]:  # Max 3 tickers
            try:
                data = yf.download(ticker, period="90d", progress=False)
                if data.empty or len(data) < 20:
                    continue

                close = data["Close"].values.flatten()
                returns = np.diff(close) / close[:-1]

                # Current price
                current_price = float(close[-1])
                raw[f"{ticker}_price"] = current_price

                # Volatility Z-score
                vol_20d = np.std(returns[-20:]) * np.sqrt(252) * 100
                vol_60d = np.std(returns[-60:]) * np.sqrt(252) * 100 if len(returns) >= 60 else vol_20d
                vol_z = (vol_20d - vol_60d) / (vol_60d + 0.001)
                raw[f"{ticker}_vol_z"] = round(float(vol_z), 2)

                # RSI (14-period)
                gains = np.where(returns > 0, returns, 0)
                losses = np.where(returns < 0, -returns, 0)
                avg_gain = np.mean(gains[-14:])
                avg_loss = np.mean(losses[-14:])
                rs = avg_gain / (avg_loss + 0.0001)
                rsi = 100 - (100 / (1 + rs))
                raw[f"{ticker}_rsi"] = round(float(rsi), 1)

                # EMA crossover
                ema_12 = float(np.mean(close[-12:]))
                ema_26 = float(np.mean(close[-26:])) if len(close) >= 26 else ema_12
                ema_signal = "BULLISH" if ema_12 > ema_26 else "BEARISH"
                raw[f"{ticker}_ema_signal"] = ema_signal

                # 52-week range position
                high_52w = float(np.max(close))
                low_52w = float(np.min(close))
                range_position = (current_price - low_52w) / (high_52w - low_52w + 0.001)
                raw[f"{ticker}_range_pct"] = round(float(range_position * 100), 1)

                # Generate quant signal
                direction = "ESPERAR"
                if rsi < 30 and ema_signal == "BEARISH" and vol_z > 1.5:
                    direction = "LONG"  # Oversold + high vol = mean reversion opportunity
                    signals.append({"type": "OVERSOLD_REVERSAL", "ticker": ticker, "rsi": rsi})
                elif rsi > 70 and ema_signal == "BULLISH":
                    direction = "SHORT"  # Overbought
                    signals.append({"type": "OVERBOUGHT", "ticker": ticker, "rsi": rsi})
                elif ema_signal == "BULLISH" and rsi > 50 and rsi < 65:
                    direction = "LONG"  # Trend continuation
                    signals.append({"type": "TREND_CONTINUATION", "ticker": ticker})

                raw[f"{ticker}_quant_direction"] = direction
                sources.append(f"yfinance — {ticker} ({current_price:.2f})")

            except Exception as e:
                raw[f"{ticker}_error"] = str(e)

    except ImportError as e:
        raw["error"] = f"Missing dependency: {e}"

    latency = int((time.time() - start) * 1000)
    confidence = min(90, 40 + len(signals) * 20)

    summary = f"Analyzed {len(context['tickers'][:3])} instruments. "
    for ticker in context["tickers"][:3]:
        price = raw.get(f"{ticker}_price", "N/A")
        rsi = raw.get(f"{ticker}_rsi", "N/A")
        direction = raw.get(f"{ticker}_quant_direction", "N/A")
        summary += f"{ticker}: ${price} (RSI {rsi}, {direction}). "

    return IntelligenceReport(
        capability="Quantitative Intelligence",
        summary=summary,
        signals=signals,
        raw_data=raw,
        sources=sources,
        confidence=confidence,
        timestamp=datetime.now(timezone.utc).isoformat(),
        latency_ms=latency,
    )


async def run_capital_protection(context: dict, reports: list) -> IntelligenceReport:
    """Capability 6: Capital Protection — risk assessment based on other capabilities."""
    start = time.time()
    signals = []
    raw = {}

    # Aggregate signals from all capabilities
    all_long = 0
    all_short = 0
    all_esperar = 0
    total_confidence = 0
    disruption_count = 0

    for report in reports:
        total_confidence += report.confidence
        for sig in report.signals:
            sig_type = sig.get("type", "")
            if "DISRUPTION" in sig_type or "SPIKE" in sig_type:
                disruption_count += 1
            if sig.get("direction") == "LONG" or "LONG" in sig_type or "REVERSAL" in sig_type or "CONTINUATION" in sig_type:
                all_long += 1
            elif sig.get("direction") == "SHORT" or "SHORT" in sig_type or "OVERBOUGHT" in sig_type:
                all_short += 1

    avg_confidence = total_confidence / max(len(reports), 1)
    raw["avg_capability_confidence"] = round(avg_confidence, 1)
    raw["disruption_count"] = disruption_count
    raw["long_signals"] = all_long
    raw["short_signals"] = all_short

    # Risk assessment
    veto = False
    veto_reason = ""

    # Check for conflicting signals
    if all_long > 0 and all_short > 0:
        conflict_ratio = min(all_long, all_short) / max(all_long, all_short)
        if conflict_ratio > 0.7:
            veto = True
            veto_reason = f"Conflicting signals: {all_long} LONG vs {all_short} SHORT"
            signals.append({"type": "VETO_CONFLICT", "reason": veto_reason})

    # Check for low confidence
    if avg_confidence < 40:
        veto = True
        veto_reason = f"Low aggregate confidence: {avg_confidence:.0f}/100"
        signals.append({"type": "VETO_LOW_CONFIDENCE", "reason": veto_reason})

    # Position sizing (simplified Kelly)
    if not veto and (all_long > 0 or all_short > 0):
        win_signals = max(all_long, all_short)
        total_signals = all_long + all_short + all_esperar + 1
        win_rate = win_signals / total_signals
        kelly = max(0, min(0.15, (win_rate * 2 - 1) / 1))  # Simplified half-Kelly
        raw["kelly_fraction"] = round(kelly, 4)
        raw["recommended_position_pct"] = round(kelly * 100, 1)
    else:
        raw["kelly_fraction"] = 0
        raw["recommended_position_pct"] = 0

    raw["veto"] = veto
    raw["veto_reason"] = veto_reason

    latency = int((time.time() - start) * 1000)
    confidence = 85 if not veto else 95  # High confidence in the veto itself

    summary = f"Risk assessment: {'VETO — ' + veto_reason if veto else 'APPROVED'}. "
    summary += f"Position sizing: {raw.get('recommended_position_pct', 0)}% Kelly. "
    summary += f"Disruptions detected: {disruption_count}."

    return IntelligenceReport(
        capability="Capital Protection",
        summary=summary,
        signals=signals,
        raw_data=raw,
        sources=["Internal risk model"],
        confidence=confidence,
        timestamp=datetime.now(timezone.utc).isoformat(),
        latency_ms=latency,
    )


# ═══════════════════════════════════════════════════════════════
# SIGNAL SYNTHESIZER
# ═══════════════════════════════════════════════════════════════

def synthesize_signal(context: dict, reports: list, risk_report: IntelligenceReport) -> Optional[TradeSignal]:
    """Synthesize all intelligence reports into a single trade signal."""

    if risk_report.raw_data.get("veto"):
        return TradeSignal(
            direction="ESPERAR",
            ticker=context["tickers"][0] if context["tickers"] else "N/A",
            ticker_name=context["ticker_name"],
            conviction=0,
            consensus="VETOED by Capital Protection",
            reasoning=risk_report.raw_data.get("veto_reason", "Risk too high"),
        )

    # Count directional signals from all capabilities
    long_votes = 0
    short_votes = 0
    esperar_votes = 0

    for report in reports:
        has_disruption = any("DISRUPTION" in s.get("type", "") or "SPIKE" in s.get("type", "")
                           for s in report.signals)
        has_long = any("LONG" in s.get("type", "") or "CONTINUATION" in s.get("type", "") or "REVERSAL" in s.get("type", "")
                      for s in report.signals)
        has_short = any("SHORT" in s.get("type", "") or "OVERBOUGHT" in s.get("type", "")
                       for s in report.signals)

        if has_disruption:
            long_votes += 1  # Supply disruption = bullish for price
        elif has_long:
            long_votes += 1
        elif has_short:
            short_votes += 1
        else:
            esperar_votes += 1

    total_votes = long_votes + short_votes + esperar_votes
    if total_votes == 0:
        total_votes = 1

    # Determine direction
    if long_votes > short_votes and long_votes >= 3:
        direction = "LONG"
        dominant_votes = long_votes
    elif short_votes > long_votes and short_votes >= 3:
        direction = "SHORT"
        dominant_votes = short_votes
    else:
        direction = "ESPERAR"
        dominant_votes = esperar_votes

    # Calculate conviction
    consensus_ratio = dominant_votes / len(reports) if reports else 0
    avg_confidence = sum(r.confidence for r in reports) / max(len(reports), 1)
    conviction = min(98, avg_confidence * consensus_ratio)

    # Get price from quant report
    quant_report = next((r for r in reports if r.capability == "Quantitative Intelligence"), None)
    entry_price = 0.0
    if quant_report and context["tickers"]:
        entry_price = quant_report.raw_data.get(f"{context['tickers'][0]}_price", 0.0)

    # Position sizing from risk report
    kelly = risk_report.raw_data.get("kelly_fraction", 0.02)

    # Stop loss and take profit based on volatility
    vol_z = 0
    if quant_report and context["tickers"]:
        vol_z = abs(quant_report.raw_data.get(f"{context['tickers'][0]}_vol_z", 1.0))

    stop_loss = min(5.0, max(2.0, 2.0 + vol_z * 0.5))
    take_profit = stop_loss * 2.5  # 2.5:1 risk-reward

    return TradeSignal(
        direction=direction,
        ticker=context["tickers"][0] if context["tickers"] else "N/A",
        ticker_name=context["ticker_name"],
        conviction=round(conviction, 1),
        consensus=f"{dominant_votes}/{len(reports)} capabilities agree",
        entry_price=round(entry_price, 2),
        stop_loss_pct=round(stop_loss, 1),
        take_profit_pct=round(take_profit, 1),
        kelly_fraction=round(kelly, 4),
        horizon="5-10 trading days",
        reasoning=f"{direction} based on {dominant_votes}/{len(reports)} capability consensus. "
                  f"Avg confidence: {avg_confidence:.0f}/100. "
                  f"{'Supply disruption detected. ' if long_votes > 0 else ''}"
                  f"Risk: {stop_loss:.1f}% stop, {take_profit:.1f}% target.",
    )


# ═══════════════════════════════════════════════════════════════
# EVIDENCE PACK BUILDER
# ═══════════════════════════════════════════════════════════════

def build_evidence_pack(command: str, context: dict, reports: list,
                       risk_report: IntelligenceReport,
                       signal: Optional[TradeSignal]) -> EvidencePack:
    """Build an immutable EvidencePack from all intelligence."""

    pack_id = f"MX-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{hashlib.md5(command.encode()).hexdigest()[:6]}"

    # Build causal chain
    causal_chain = []
    for report in reports:
        if report.signals:
            for sig in report.signals[:2]:
                causal_chain.append({
                    "source": report.capability,
                    "event": sig.get("type", "UNKNOWN"),
                    "detail": sig.get("title", sig.get("reason", sig.get("ticker", ""))),
                })

    # Build evidence cards
    evidence_cards = []
    for report in reports:
        card = {
            "capability": report.capability,
            "summary": report.summary,
            "confidence": report.confidence,
            "sources": report.sources[:3],
            "signals_count": len(report.signals),
            "latency_ms": report.latency_ms,
        }
        evidence_cards.append(card)

    # What happened
    disruption_reports = [r for r in reports if any("DISRUPTION" in s.get("type", "") for s in r.signals)]
    if disruption_reports:
        what_happened = f"Supply chain disruption detected in {context['ticker_name']} market. "
        what_happened += f"{len(disruption_reports)} intelligence capabilities flagged disruption signals."
    else:
        what_happened = f"Market analysis for {context['ticker_name']}. "
        what_happened += f"{len(reports)} intelligence capabilities deployed."

    # Why it matters
    if signal and signal.direction != "ESPERAR":
        why_it_matters = f"Consensus {signal.consensus}: {signal.direction} {signal.ticker} "
        why_it_matters += f"at conviction {signal.conviction}/100. "
        why_it_matters += f"Entry ${signal.entry_price}, Stop {signal.stop_loss_pct}%, Target {signal.take_profit_pct}%."
    else:
        why_it_matters = "Insufficient consensus for a trade signal. Monitoring continues."

    pack = EvidencePack(
        pack_id=pack_id,
        timestamp=datetime.now(timezone.utc).isoformat(),
        command=command,
        what_happened=what_happened,
        why_it_matters=why_it_matters,
        signal=signal,
        intelligence_reports=[asdict(r) for r in reports + [risk_report]],
        causal_chain=causal_chain,
        evidence_cards=evidence_cards,
        risk_assessment=risk_report.raw_data,
    )
    pack.compute_hash()
    return pack


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════

async def analyze(command: str, callback=None) -> EvidencePack:
    """
    Main pipeline entry point.
    Takes a CEO command, deploys all intelligence capabilities,
    synthesizes a signal, and returns an EvidencePack.
    """
    pipeline_start = time.time()

    if callback:
        await callback({"phase": "classifying", "message": "Classifying command..."})

    # Step 1: Classify the command
    context = classify_command(command)

    if callback:
        await callback({
            "phase": "deploying",
            "message": f"Deploying 6 intelligence capabilities for {context['ticker_name']}...",
            "tickers": context["tickers"],
        })

    # Step 2: Deploy 5 capabilities in parallel (6th is risk, runs after)
    tasks = [
        run_asymmetric_intelligence(context, command),
        run_physical_intelligence(context, command),
        run_geopolitical_intelligence(context, command),
        run_macro_intelligence(context, command),
        run_quantitative_intelligence(context, command),
    ]

    if callback:
        await callback({"phase": "scanning", "message": "Scanning satellite data, maritime AIS, GDELT, macro indicators..."})

    reports = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out exceptions
    valid_reports = []
    for r in reports:
        if isinstance(r, IntelligenceReport):
            valid_reports.append(r)
        elif isinstance(r, Exception):
            valid_reports.append(IntelligenceReport(
                capability="ERROR",
                summary=str(r),
                confidence=0,
                timestamp=datetime.now(timezone.utc).isoformat(),
            ))

    if callback:
        await callback({"phase": "risk_check", "message": "Running Capital Protection assessment..."})

    # Step 3: Run Capital Protection (needs other reports as input)
    risk_report = await run_capital_protection(context, valid_reports)

    if callback:
        await callback({"phase": "synthesizing", "message": "Synthesizing consensus signal..."})

    # Step 4: Synthesize signal
    signal = synthesize_signal(context, valid_reports, risk_report)

    # Step 5: Build EvidencePack
    pack = build_evidence_pack(command, context, valid_reports, risk_report, signal)

    pipeline_latency = int((time.time() - pipeline_start) * 1000)

    if callback:
        await callback({
            "phase": "complete",
            "message": f"Analysis complete in {pipeline_latency/1000:.1f}s",
            "signal": signal.direction if signal else "NONE",
            "conviction": signal.conviction if signal else 0,
            "pack_id": pack.pack_id,
        })

    return pack


def analyze_sync(command: str) -> EvidencePack:
    """Synchronous wrapper for the pipeline."""
    return asyncio.run(analyze(command))


# ═══════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    command = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Analiza el mercado del carbon colombiano"

    print(f"\n{'='*60}")
    print(f"  METEORO X — Agentic Intelligence Pipeline")
    print(f"  Command: {command}")
    print(f"{'='*60}\n")

    pack = analyze_sync(command)

    print(f"\n{'='*60}")
    print(f"  EVIDENCE PACK: {pack.pack_id}")
    print(f"{'='*60}")
    print(f"  Timestamp: {pack.timestamp}")
    print(f"  What happened: {pack.what_happened}")
    print(f"  Why it matters: {pack.why_it_matters}")
    print(f"")

    if pack.signal:
        sig = pack.signal
        print(f"  SIGNAL: {sig.direction} {sig.ticker} ({sig.ticker_name})")
        print(f"  Conviction: {sig.conviction}/100 ({sig.consensus})")
        print(f"  Entry: ${sig.entry_price}")
        print(f"  Stop Loss: {sig.stop_loss_pct}%")
        print(f"  Take Profit: {sig.take_profit_pct}%")
        print(f"  Kelly Fraction: {sig.kelly_fraction}")
        print(f"  Horizon: {sig.horizon}")
        print(f"  Reasoning: {sig.reasoning}")

    print(f"\n  Evidence Cards ({len(pack.evidence_cards)}):")
    for card in pack.evidence_cards:
        print(f"    - {card['capability']}: {card['summary'][:80]}...")
        print(f"      Confidence: {card['confidence']}/100 | Latency: {card['latency_ms']}ms")

    print(f"\n  Causal Chain ({len(pack.causal_chain)} nodes):")
    for node in pack.causal_chain[:5]:
        print(f"    {node['source']} → {node['event']}: {node.get('detail', '')[:60]}")

    print(f"\n  Risk Assessment:")
    risk = pack.risk_assessment
    print(f"    Veto: {risk.get('veto', False)}")
    print(f"    Kelly: {risk.get('kelly_fraction', 0)}")
    print(f"    Long signals: {risk.get('long_signals', 0)} | Short: {risk.get('short_signals', 0)}")

    print(f"\n  SHA-256: {pack.pack_hash}")
    print(f"{'='*60}\n")
