#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  METEORO AGENTIC SYSTEM — FRED API INTEGRATION                   ║
║  Federal Reserve Economic Data — 250,000+ time series            ║
║                                                                  ║
║  FREE API. Key available at: https://fred.stlouisfed.org/        ║
║                                                                  ║
║  Key series for commodity intelligence:                          ║
║    - Coal prices (API2, Newcastle proxy)                         ║
║    - Oil prices (WTI, Brent)                                     ║
║    - Natural gas (Henry Hub)                                     ║
║    - Metals (copper, gold)                                       ║
║    - VIX, DXY, yield curves                                      ║
║    - Central bank rates (Fed Funds, ECB, PBOC)                   ║
║    - CPI, PPI, PMI indicators                                    ║
║    - Trade balances, inventories                                 ║
║                                                                  ║
║  Fallback: If no FRED_API_KEY, uses yfinance proxies             ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import aiohttp
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("meteoro.fred")

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

FRED_BASE_URL = "https://api.stlouisfed.org/fred"
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

# ═══════════════════════════════════════════════════════════════
# CRITICAL SERIES FOR COMMODITY TRADING
# ═══════════════════════════════════════════════════════════════

COMMODITY_SERIES = {
    # ── Energy ────────────────────────────────────────────────
    "DCOILWTICO": {
        "name": "WTI Crude Oil",
        "frequency": "daily",
        "unit": "USD/barrel",
        "category": "energy",
    },
    "DCOILBRENTEU": {
        "name": "Brent Crude Oil",
        "frequency": "daily",
        "unit": "USD/barrel",
        "category": "energy",
    },
    "DHHNGSP": {
        "name": "Henry Hub Natural Gas",
        "frequency": "daily",
        "unit": "USD/MMBtu",
        "category": "energy",
    },

    # ── Coal (proxies — FRED doesn't have direct Newcastle) ──
    "PCOALAUUSDM": {
        "name": "Australia Coal Price (Newcastle proxy)",
        "frequency": "monthly",
        "unit": "USD/metric ton",
        "category": "coal",
    },
    "PCOALSA": {
        "name": "South Africa Coal Price",
        "frequency": "monthly",
        "unit": "USD/metric ton",
        "category": "coal",
    },

    # ── Metals ────────────────────────────────────────────────
    "PCOPPUSDM": {
        "name": "Copper Price (LME)",
        "frequency": "monthly",
        "unit": "USD/metric ton",
        "category": "metals",
    },
    "GOLDAMGBD228NLBM": {
        "name": "Gold Price (London Fix)",
        "frequency": "daily",
        "unit": "USD/oz",
        "category": "metals",
    },

    # ── Macro Indicators ──────────────────────────────────────
    "VIXCLS": {
        "name": "VIX Volatility Index",
        "frequency": "daily",
        "unit": "index",
        "category": "macro",
    },
    "DTWEXBGS": {
        "name": "US Dollar Index (Trade-Weighted)",
        "frequency": "daily",
        "unit": "index",
        "category": "macro",
    },
    "DGS10": {
        "name": "10-Year Treasury Yield",
        "frequency": "daily",
        "unit": "percent",
        "category": "macro",
    },
    "DGS2": {
        "name": "2-Year Treasury Yield",
        "frequency": "daily",
        "unit": "percent",
        "category": "macro",
    },
    "T10Y2Y": {
        "name": "10Y-2Y Yield Spread",
        "frequency": "daily",
        "unit": "percent",
        "category": "macro",
    },
    "DFEDTARU": {
        "name": "Fed Funds Target Rate (Upper)",
        "frequency": "daily",
        "unit": "percent",
        "category": "rates",
    },

    # ── Inflation & Economic ──────────────────────────────────
    "CPIAUCSL": {
        "name": "CPI (All Urban Consumers)",
        "frequency": "monthly",
        "unit": "index",
        "category": "inflation",
    },
    "PPIACO": {
        "name": "PPI (All Commodities)",
        "frequency": "monthly",
        "unit": "index",
        "category": "inflation",
    },
    "MANEMP": {
        "name": "Manufacturing Employment",
        "frequency": "monthly",
        "unit": "thousands",
        "category": "economic",
    },

    # ── Trade & Shipping ──────────────────────────────────────
    "BOPGSTB": {
        "name": "US Trade Balance (Goods & Services)",
        "frequency": "monthly",
        "unit": "millions USD",
        "category": "trade",
    },

    # ── LatAm FX Rates ────────────────────────────────────────
    "DEXCOUS": {
        "name": "USD/COP (Colombia Peso)",
        "frequency": "daily",
        "unit": "COP per USD",
        "category": "fx",
    },
    "DEXBZUS": {
        "name": "USD/BRL (Brazil Real)",
        "frequency": "daily",
        "unit": "BRL per USD",
        "category": "fx",
    },
    "DEXCHUS": {
        "name": "USD/CNY (Chinese Yuan)",
        "frequency": "daily",
        "unit": "CNY per USD",
        "category": "fx",
    },
}


# ═══════════════════════════════════════════════════════════════
# FRED API CLIENT
# ═══════════════════════════════════════════════════════════════

async def _fred_request(endpoint: str, params: Dict, timeout: int = 15) -> Optional[Dict]:
    """Make a request to the FRED API."""
    if not FRED_API_KEY:
        logger.warning("No FRED_API_KEY set. Use fallback data sources.")
        return None

    params["api_key"] = FRED_API_KEY
    params["file_type"] = "json"

    url = f"{FRED_BASE_URL}/{endpoint}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    text = await resp.text()
                    logger.error(f"FRED API error {resp.status}: {text[:200]}")
                    return None
    except Exception as e:
        logger.error(f"FRED API request failed: {e}")
        return None


async def get_series_data(
    series_id: str,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    limit: int = 100,
    sort_order: str = "desc",
) -> Dict:
    """
    Get observations for a FRED series.

    Args:
        series_id: FRED series ID (e.g., "DCOILWTICO")
        observation_start: Start date (YYYY-MM-DD)
        observation_end: End date (YYYY-MM-DD)
        limit: Max observations to return
        sort_order: "desc" (newest first) or "asc"

    Returns:
        Dict with series info and observations
    """
    if not observation_start:
        observation_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    if not observation_end:
        observation_end = datetime.now().strftime("%Y-%m-%d")

    params = {
        "series_id": series_id,
        "observation_start": observation_start,
        "observation_end": observation_end,
        "limit": str(limit),
        "sort_order": sort_order,
    }

    data = await _fred_request("series/observations", params)

    if not data:
        return await _yfinance_fallback(series_id)

    observations = []
    for obs in data.get("observations", []):
        value = obs.get("value", ".")
        if value != ".":
            try:
                observations.append({
                    "date": obs["date"],
                    "value": float(value),
                })
            except (ValueError, KeyError):
                continue

    series_info = COMMODITY_SERIES.get(series_id, {})

    return {
        "status": "OK",
        "source": "FRED",
        "series_id": series_id,
        "name": series_info.get("name", series_id),
        "unit": series_info.get("unit", ""),
        "category": series_info.get("category", ""),
        "count": len(observations),
        "latest": observations[0] if observations else None,
        "observations": observations[:limit],
    }


async def get_series_info(series_id: str) -> Dict:
    """Get metadata about a FRED series."""
    data = await _fred_request("series", {"series_id": series_id})
    if data and "seriess" in data and data["seriess"]:
        s = data["seriess"][0]
        return {
            "status": "OK",
            "series_id": series_id,
            "title": s.get("title", ""),
            "frequency": s.get("frequency", ""),
            "units": s.get("units", ""),
            "seasonal_adjustment": s.get("seasonal_adjustment", ""),
            "last_updated": s.get("last_updated", ""),
        }
    return {"status": "error", "series_id": series_id}


# ═══════════════════════════════════════════════════════════════
# YFINANCE FALLBACK (when no FRED API key)
# ═══════════════════════════════════════════════════════════════

FRED_TO_YFINANCE = {
    "DCOILWTICO": "CL=F",
    "DCOILBRENTEU": "BZ=F",
    "DHHNGSP": "NG=F",
    "GOLDAMGBD228NLBM": "GC=F",
    "PCOPPUSDM": "HG=F",
    "VIXCLS": "^VIX",
    "DGS10": "^TNX",
    "DGS2": "^IRX",
    "DTWEXBGS": "DX-Y.NYB",
}


async def _yfinance_fallback(series_id: str) -> Dict:
    """Fallback to yfinance when FRED API key is not available."""
    yf_ticker = FRED_TO_YFINANCE.get(series_id)

    if not yf_ticker:
        return {
            "status": "no_fallback",
            "source": "none",
            "series_id": series_id,
            "error": f"No FRED_API_KEY and no yfinance mapping for {series_id}",
            "observations": [],
        }

    try:
        import yfinance as yf
        ticker = yf.Ticker(yf_ticker)
        hist = ticker.history(period="6mo")

        if hist.empty:
            return {
                "status": "error",
                "source": "yfinance_fallback",
                "series_id": series_id,
                "error": f"No data from yfinance for {yf_ticker}",
                "observations": [],
            }

        observations = []
        for date, row in hist.tail(100).iterrows():
            observations.append({
                "date": date.strftime("%Y-%m-%d"),
                "value": round(float(row["Close"]), 4),
            })

        observations.reverse()  # Newest first

        series_info = COMMODITY_SERIES.get(series_id, {})

        return {
            "status": "OK",
            "source": "yfinance_fallback",
            "series_id": series_id,
            "yfinance_ticker": yf_ticker,
            "name": series_info.get("name", series_id),
            "unit": series_info.get("unit", ""),
            "category": series_info.get("category", ""),
            "count": len(observations),
            "latest": observations[0] if observations else None,
            "observations": observations,
        }

    except ImportError:
        return {
            "status": "error",
            "source": "none",
            "series_id": series_id,
            "error": "yfinance not installed",
            "observations": [],
        }
    except Exception as e:
        return {
            "status": "error",
            "source": "yfinance_fallback",
            "series_id": series_id,
            "error": str(e),
            "observations": [],
        }


# ═══════════════════════════════════════════════════════════════
# HIGH-LEVEL FUNCTIONS
# ═══════════════════════════════════════════════════════════════

async def get_macro_snapshot() -> Dict:
    """
    Get a complete macro snapshot relevant to commodity trading.
    Returns latest values for all key indicators.
    """
    critical_series = [
        "DCOILWTICO", "DCOILBRENTEU", "DHHNGSP",       # Energy
        "GOLDAMGBD228NLBM", "PCOPPUSDM",                 # Metals
        "VIXCLS", "DTWEXBGS", "DGS10", "T10Y2Y",       # Macro
        "DEXCOUS", "DEXCHUS",                              # FX
    ]

    tasks = [get_series_data(s, limit=5) for s in critical_series]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    snapshot = {}
    for i, result in enumerate(results):
        series_id = critical_series[i]
        if isinstance(result, Exception):
            snapshot[series_id] = {"status": "error", "error": str(result)}
        elif result.get("status") == "OK" and result.get("latest"):
            snapshot[series_id] = {
                "name": result.get("name", series_id),
                "value": result["latest"]["value"],
                "date": result["latest"]["date"],
                "source": result.get("source", "FRED"),
                "unit": result.get("unit", ""),
            }
        else:
            snapshot[series_id] = result

    # Calculate derived indicators
    try:
        dgs10 = snapshot.get("DGS10", {}).get("value")
        t10y2y = snapshot.get("T10Y2Y", {}).get("value")

        if t10y2y is not None:
            snapshot["_yield_curve_signal"] = {
                "name": "Yield Curve Signal",
                "value": "INVERTED (recession risk)" if t10y2y < 0 else "NORMAL",
                "spread": t10y2y,
            }

        vix = snapshot.get("VIXCLS", {}).get("value")
        if vix is not None:
            if vix > 30:
                regime = "FEAR"
            elif vix > 20:
                regime = "ELEVATED"
            elif vix > 15:
                regime = "NORMAL"
            else:
                regime = "COMPLACENT"
            snapshot["_vix_regime"] = {
                "name": "VIX Regime",
                "value": regime,
                "vix": vix,
            }

    except Exception:
        pass

    return {
        "status": "OK",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api_source": "FRED" if FRED_API_KEY else "yfinance_fallback",
        "snapshot": snapshot,
    }


async def get_coal_data() -> Dict:
    """Get coal-specific price data and context."""
    coal_series = ["PCOALAUUSDM", "PCOALSA"]
    energy_series = ["DCOILWTICO", "DHHNGSP"]

    tasks = [get_series_data(s, limit=24) for s in coal_series + energy_series]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    data = {}
    for i, result in enumerate(results):
        series_id = (coal_series + energy_series)[i]
        if not isinstance(result, Exception) and result.get("status") == "OK":
            data[series_id] = result

    return {
        "status": "OK",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "coal": data,
    }


# ═══════════════════════════════════════════════════════════════
# TOOL WRAPPER (for agent integration)
# ═══════════════════════════════════════════════════════════════

async def fred_tool_handler(params: Dict) -> str:
    """
    Unified tool handler for the Meteoro Agentic System.

    Supported actions:
    - "snapshot": Full macro snapshot
    - "series": Get specific FRED series data
    - "coal": Coal-specific data
    - "info": Series metadata
    """
    action = params.get("action", "snapshot")

    if action == "snapshot":
        result = await get_macro_snapshot()

    elif action == "series":
        series_id = params.get("series_id", "DCOILWTICO")
        limit = params.get("limit", 50)
        result = await get_series_data(series_id, limit=limit)

    elif action == "coal":
        result = await get_coal_data()

    elif action == "info":
        series_id = params.get("series_id", "DCOILWTICO")
        result = await get_series_info(series_id)

    else:
        result = {"status": "error", "error": f"Unknown action: {action}"}

    return json.dumps(result, default=str, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════

async def _test():
    """Quick test of FRED integration."""
    print("=" * 60)
    print("METEORO AGENTIC SYSTEM — FRED API TEST")
    print(f"API Key configured: {'YES' if FRED_API_KEY else 'NO (using yfinance fallback)'}")
    print("=" * 60)

    # Test 1: Macro snapshot
    print("\n[1] Getting macro snapshot...")
    snapshot = await get_macro_snapshot()
    print(f"    Source: {snapshot.get('api_source', 'unknown')}")
    for sid, data in snapshot.get("snapshot", {}).items():
        if isinstance(data, dict) and "value" in data:
            print(f"    {data.get('name', sid)}: {data['value']} {data.get('unit', '')}")

    # Test 2: WTI Oil series
    print("\n[2] Getting WTI Crude Oil data...")
    wti = await get_series_data("DCOILWTICO", limit=5)
    print(f"    Source: {wti.get('source', 'unknown')}")
    if wti.get("latest"):
        print(f"    Latest: ${wti['latest']['value']} ({wti['latest']['date']})")

    # Test 3: Coal data
    print("\n[3] Getting coal data...")
    coal = await get_coal_data()
    for sid, data in coal.get("coal", {}).items():
        if data.get("latest"):
            print(f"    {data.get('name', sid)}: ${data['latest']['value']} ({data['latest']['date']})")

    print("\n" + "=" * 60)
    print("FRED API TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(_test())
