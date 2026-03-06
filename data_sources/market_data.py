#!/usr/bin/env python3
"""
METEORO X — REAL MARKET DATA MODULE
Fetches REAL prices, macro indicators, and news via yfinance + free APIs.
NO API keys needed. This replaces all hardcoded/simulated data.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from functools import lru_cache

logger = logging.getLogger("meteoro.market_data")

# ═══════════════════════════════════════════════════════════════
# COMMODITY TICKER MAPPING
# ═══════════════════════════════════════════════════════════════

COMMODITY_TICKERS = {
    # Energy
    "OIL": {"ticker": "CL=F", "name": "WTI Crude Oil", "unit": "USD/bbl"},
    "BRENT": {"ticker": "BZ=F", "name": "Brent Crude", "unit": "USD/bbl"},
    "NATURAL_GAS": {"ticker": "NG=F", "name": "Natural Gas", "unit": "USD/MMBtu"},
    # Precious Metals
    "GOLD": {"ticker": "GC=F", "name": "Gold", "unit": "USD/oz"},
    "SILVER": {"ticker": "SI=F", "name": "Silver", "unit": "USD/oz"},
    "PLATINUM": {"ticker": "PL=F", "name": "Platinum", "unit": "USD/oz"},
    "PALLADIUM": {"ticker": "PA=F", "name": "Palladium", "unit": "USD/oz"},
    # Base & Industrial Metals
    "COPPER": {"ticker": "HG=F", "name": "Copper", "unit": "USD/lb"},
    "COAL": {"ticker": "MTF=F", "name": "Coal (Newcastle)", "unit": "USD/ton"},
    "NICKEL": {"ticker": "NI=F", "name": "Nickel", "unit": "USD/ton"},
    "IRON": {"ticker": "GWM.AX", "name": "Iron Ore proxy", "unit": "AUD"},
    "LITHIUM": {"ticker": "LTHM", "name": "Lithium proxy (Livent)", "unit": "USD"},
    "COBALT": {"ticker": "GLNCY", "name": "Cobalt proxy (Glencore)", "unit": "USD"},
    # Agriculture
    "COFFEE": {"ticker": "KC=F", "name": "Coffee Arabica", "unit": "cents/lb"},
    "WHEAT": {"ticker": "ZW=F", "name": "Wheat", "unit": "cents/bu"},
    "CORN": {"ticker": "ZC=F", "name": "Corn", "unit": "cents/bu"},
    "SOY": {"ticker": "ZS=F", "name": "Soybeans", "unit": "cents/bu"},
    "SUGAR": {"ticker": "SB=F", "name": "Sugar #11", "unit": "cents/lb"},
    # Shipping
    "SHIPPING": {"ticker": "BDRY", "name": "Baltic Dry Index ETF", "unit": "USD"},
    # General (use S&P GSCI as proxy)
    "GENERAL": {"ticker": "^GSPC", "name": "S&P 500", "unit": "USD"},
}

MACRO_TICKERS = {
    "VIX": "^VIX",
    "DXY": "DX-Y.NYB",
    "SP500": "^GSPC",
    "US10Y": "^TNX",
    "US2Y": "2YY=F",
    "GOLD": "GC=F",
    "BTC": "BTC-USD",
}

LATAM_FX = {
    "USD_COP": "COP=X",
    "USD_BRL": "BRL=X",
    "USD_CLP": "CLP=X",
    "USD_PEN": "PEN=X",
    "USD_MXN": "MXN=X",
    "USD_CNY": "CNY=X",
}


# ═══════════════════════════════════════════════════════════════
# CACHE (avoid hammering yfinance)
# ═══════════════════════════════════════════════════════════════

_cache: Dict[str, Any] = {}
_cache_ttl = 300  # 5 minutes


def _get_cached(key: str) -> Optional[Any]:
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < _cache_ttl:
            return data
    return None


def _set_cache(key: str, data: Any):
    _cache[key] = (data, time.time())


# ═══════════════════════════════════════════════════════════════
# CORE DATA FETCHERS
# ═══════════════════════════════════════════════════════════════

def _fetch_ticker_sync(ticker: str, period: str = "1mo") -> Dict[str, Any]:
    """Fetch real price data for a ticker using yfinance (synchronous)."""
    cache_key = f"ticker:{ticker}:{period}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        hist = t.history(period=period)

        if hist.empty:
            return {"error": f"No data for {ticker}", "ticker": ticker}

        latest = hist.iloc[-1]
        prev = hist.iloc[-2] if len(hist) > 1 else hist.iloc[0]

        price = round(float(latest["Close"]), 4)
        prev_price = round(float(prev["Close"]), 4)
        change = round(price - prev_price, 4)
        change_pct = round((change / prev_price) * 100, 2) if prev_price else 0

        # Calculate technical indicators from history
        closes = hist["Close"].values
        high = round(float(hist["High"].max()), 4)
        low = round(float(hist["Low"].min()), 4)
        avg_volume = int(hist["Volume"].mean()) if "Volume" in hist.columns else 0

        # Simple RSI (14-period)
        rsi = _calc_rsi(closes)

        # Simple moving averages
        sma_10 = round(float(closes[-10:].mean()), 4) if len(closes) >= 10 else price
        sma_20 = round(float(closes[-20:].mean()), 4) if len(closes) >= 20 else price

        # Volatility (20-day annualized)
        if len(closes) >= 5:
            import numpy as np
            returns = np.diff(np.log(closes[-21:])) if len(closes) >= 21 else np.diff(np.log(closes))
            volatility = round(float(np.std(returns) * (252 ** 0.5) * 100), 2)
        else:
            volatility = 0

        # Price history (last 5 days)
        price_history = []
        for date, row in hist.tail(5).iterrows():
            price_history.append({
                "date": date.strftime("%Y-%m-%d"),
                "close": round(float(row["Close"]), 4),
                "volume": int(row["Volume"]) if "Volume" in row else 0,
            })

        result = {
            "ticker": ticker,
            "price": price,
            "prev_close": prev_price,
            "change": change,
            "change_pct": change_pct,
            "high_period": high,
            "low_period": low,
            "rsi_14": rsi,
            "sma_10": sma_10,
            "sma_20": sma_20,
            "volatility_ann_pct": volatility,
            "avg_volume": avg_volume,
            "date": hist.index[-1].strftime("%Y-%m-%d"),
            "price_history": price_history,
            "source": "yfinance",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        _set_cache(cache_key, result)
        return result

    except Exception as e:
        logger.error(f"yfinance fetch error for {ticker}: {e}")
        return {"error": str(e), "ticker": ticker}


def _calc_rsi(closes, period=14) -> float:
    """Calculate RSI from close prices."""
    if len(closes) < period + 1:
        return 50.0
    import numpy as np
    deltas = np.diff(closes[-(period + 1):])
    gains = deltas[deltas > 0].sum() / period
    losses = abs(deltas[deltas < 0].sum()) / period
    if losses == 0:
        return 100.0
    rs = gains / losses
    return round(100 - (100 / (1 + rs)), 2)


async def fetch_commodity_price(commodity: str, period: str = "1mo") -> Dict[str, Any]:
    """
    Fetch REAL price data for a commodity.
    Runs yfinance in a thread to avoid blocking asyncio.
    """
    mapping = COMMODITY_TICKERS.get(commodity.upper(), COMMODITY_TICKERS.get("OIL"))
    ticker = mapping["ticker"]

    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _fetch_ticker_sync, ticker, period)

    data["commodity"] = commodity
    data["commodity_name"] = mapping["name"]
    data["unit"] = mapping["unit"]
    return data


async def fetch_macro_indicators() -> Dict[str, Any]:
    """Fetch REAL macro indicators (VIX, DXY, yields, S&P500)."""
    cache_key = "macro_snapshot"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    loop = asyncio.get_event_loop()

    tasks = []
    keys = list(MACRO_TICKERS.keys())
    for key in keys:
        ticker = MACRO_TICKERS[key]
        tasks.append(loop.run_in_executor(None, _fetch_ticker_sync, ticker, "5d"))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    macro = {}
    for i, key in enumerate(keys):
        r = results[i]
        if isinstance(r, Exception) or (isinstance(r, dict) and "error" in r):
            macro[key] = {"error": str(r) if isinstance(r, Exception) else r.get("error", "unknown")}
        else:
            macro[key] = {
                "value": r.get("price", 0),
                "change_pct": r.get("change_pct", 0),
                "date": r.get("date", ""),
            }

    # Derive signals
    vix_val = macro.get("VIX", {}).get("value", 0)
    if vix_val > 30:
        macro["_regime"] = "FEAR"
    elif vix_val > 20:
        macro["_regime"] = "ELEVATED"
    elif vix_val > 15:
        macro["_regime"] = "NORMAL"
    else:
        macro["_regime"] = "COMPLACENT"

    result = {
        "indicators": macro,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "yfinance",
    }

    _set_cache(cache_key, result)
    return result


async def fetch_latam_fx() -> Dict[str, Any]:
    """Fetch REAL LatAm FX rates."""
    loop = asyncio.get_event_loop()

    tasks = []
    keys = list(LATAM_FX.keys())
    for key in keys:
        ticker = LATAM_FX[key]
        tasks.append(loop.run_in_executor(None, _fetch_ticker_sync, ticker, "5d"))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    fx = {}
    for i, key in enumerate(keys):
        r = results[i]
        if isinstance(r, Exception) or (isinstance(r, dict) and "error" in r):
            fx[key] = {"error": str(r) if isinstance(r, Exception) else r.get("error", "unknown")}
        else:
            fx[key] = {
                "rate": r.get("price", 0),
                "change_pct": r.get("change_pct", 0),
                "date": r.get("date", ""),
            }

    return {
        "fx_rates": fx,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "yfinance",
    }


async def fetch_full_market_context(commodity: str) -> Dict[str, Any]:
    """
    Fetch ALL real data needed for a full swarm analysis.
    This is called once and shared across all agents in the system.
    """
    # Fetch everything in parallel
    commodity_task = fetch_commodity_price(commodity)
    macro_task = fetch_macro_indicators()
    fx_task = fetch_latam_fx()

    results = await asyncio.gather(
        commodity_task, macro_task, fx_task,
        return_exceptions=True,
    )

    commodity_data = results[0] if not isinstance(results[0], Exception) else {"error": str(results[0])}
    macro_data = results[1] if not isinstance(results[1], Exception) else {"error": str(results[1])}
    fx_data = results[2] if not isinstance(results[2], Exception) else {"error": str(results[2])}

    return {
        "commodity": commodity_data,
        "macro": macro_data,
        "fx": fx_data,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "real_market_data",
    }


# ═══════════════════════════════════════════════════════════════
# GDELT — Free geopolitical events (no API key)
# ═══════════════════════════════════════════════════════════════

async def fetch_gdelt_events(commodity: str) -> Dict[str, Any]:
    """Fetch recent geopolitical events from GDELT API (free, no key)."""
    cache_key = f"gdelt:{commodity}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    keyword_map = {
        "OIL": "oil+crude+petroleum+OPEC",
        "COFFEE": "coffee+arabica+robusta+harvest+Colombia+Brazil",
        "GOLD": "gold+bullion+central+bank+reserve",
        "COPPER": "copper+mining+Chile+Peru+Escondida",
        "COAL": "coal+energy+Cerrejon+Newcastle+thermal",
        "WHEAT": "wheat+grain+export+Russia+Ukraine",
        "CORN": "corn+maize+ethanol+biofuel",
        "SOY": "soybean+Brazil+Argentina+harvest+CBOT",
        "NATURAL_GAS": "natural+gas+LNG+pipeline+Europe",
        "SILVER": "silver+precious+metal+solar",
        "LITHIUM": "lithium+battery+EV+mining+Chile+Australia",
        "NICKEL": "nickel+stainless+steel+Indonesia+EV",
        "IRON": "iron+ore+mining+Australia+Brazil+Vale+BHP",
        "COBALT": "cobalt+Congo+EV+battery+mining",
        "PLATINUM": "platinum+catalytic+converter+South+Africa",
        "PALLADIUM": "palladium+catalytic+Russia+automotive",
        "SUGAR": "sugar+ethanol+Brazil+India+harvest",
        "SHIPPING": "shipping+freight+Baltic+dry+bulk+container",
        "GENERAL": "commodities+trading+markets+global",
    }

    query = keyword_map.get(commodity.upper(), commodity)
    url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={query}&mode=ArtList&maxrecords=10&format=json"

    try:
        import httpx
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
            if r.status_code == 200:
                data = r.json()
                articles = data.get("articles", [])
                result = {
                    "commodity": commodity,
                    "articles_found": len(articles),
                    "articles": [
                        {
                            "title": a.get("title", ""),
                            "url": a.get("url", ""),
                            "source": a.get("domain", ""),
                            "date": a.get("seendate", ""),
                            "language": a.get("language", ""),
                            "tone": a.get("tone", 0),
                        }
                        for a in articles[:10]
                    ],
                    "avg_tone": sum(float(a.get("tone", 0)) for a in articles) / max(len(articles), 1),
                    "source": "GDELT",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                _set_cache(cache_key, result)
                return result
            else:
                return {"error": f"GDELT returned {r.status_code}", "articles": []}
    except Exception as e:
        return {"error": str(e), "articles": []}
