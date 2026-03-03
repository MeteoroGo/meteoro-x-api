#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  METEORO AGENTIC SYSTEM — GDELT GEOPOLITICAL MONITOR             ║
║  Real-time geopolitical event detection from GDELT Project       ║
║                                                                  ║
║  FREE. No API key. No rate limits. Updated every 15 minutes.     ║
║                                                                  ║
║  Capabilities:                                                   ║
║    - Query GDELT DOC API for news articles by keyword            ║
║    - Query GDELT GEO API for events by location/actor            ║
║    - Monitor commodity-relevant geopolitical events               ║
║    - Score events by disruption potential                         ║
║    - Track LatAm mining/energy/transport disruptions              ║
║                                                                  ║
║  GDELT indexes 300,000+ articles/day in 65+ languages            ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import aiohttp
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from urllib.parse import quote_plus

logger = logging.getLogger("meteoro.gdelt")

# ═══════════════════════════════════════════════════════════════
# GDELT API ENDPOINTS
# ═══════════════════════════════════════════════════════════════

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_GEO_API = "https://api.gdeltproject.org/api/v2/geo/geo"
GDELT_TV_API = "https://api.gdeltproject.org/api/v2/tv/tv"

# ═══════════════════════════════════════════════════════════════
# COMMODITY-FOCUSED QUERY TEMPLATES
# ═══════════════════════════════════════════════════════════════

COMMODITY_QUERIES = {
    "coal_colombia": [
        "Cerrejon coal strike",
        "Colombia coal export disruption",
        "Drummond coal Colombia",
        "Puerto Bolivar coal",
        "Fenoco railway Colombia",
        "Colombia mining protest blockade",
    ],
    "copper_peru": [
        "Las Bambas copper protest",
        "Peru copper mine blockade",
        "Cerro Verde copper disruption",
        "Antamina copper Peru",
        "Southern Peru copper",
        "Peru mining community protest",
    ],
    "oil_latam": [
        "Ecopetrol production decline",
        "Colombia oil pipeline attack",
        "OPEC production cut",
        "Venezuela oil sanctions",
        "Ecuador oil protest",
        "Brazil Petrobras disruption",
    ],
    "gold_latam": [
        "Buritica gold mine Colombia",
        "Zijin Mining Colombia",
        "Peru illegal gold mining",
        "Colombia gold export",
    ],
    "geopolitical_macro": [
        "China commodity import",
        "Russia energy sanctions",
        "Red Sea shipping disruption",
        "Panama Canal drought",
        "Suez Canal blockage",
        "BRICS commodity trade",
    ],
    "shipping_logistics": [
        "bulk carrier coal delay",
        "port congestion coal",
        "dry bulk shipping rates",
        "Baltic Dry Index surge",
        "Panamax freight rate",
    ],
    "energy_transition": [
        "coal plant closure Europe",
        "renewable energy commodity demand",
        "lithium supply disruption",
        "copper demand electric vehicle",
    ],
}

# Disruption keywords that signal high-impact events
DISRUPTION_SIGNALS = [
    "strike", "protest", "blockade", "explosion", "collapse",
    "sanctions", "embargo", "ban", "shutdown", "closure",
    "attack", "sabotage", "flood", "earthquake", "hurricane",
    "spill", "leak", "accident", "derailment", "fire",
    "halt", "suspend", "freeze", "restrict", "seize",
    "huelga", "bloqueo", "protesta", "paro", "cierre",
    "derrumbe", "explosion", "inundacion", "terremoto",
]

# Country/region relevance for LatAm commodities
LATAM_COUNTRIES = [
    "Colombia", "Peru", "Chile", "Brazil", "Ecuador",
    "Venezuela", "Argentina", "Bolivia", "Mexico",
]


# ═══════════════════════════════════════════════════════════════
# GDELT DOC API — Article Search
# ═══════════════════════════════════════════════════════════════

async def search_gdelt_articles(
    query: str,
    mode: str = "ArtList",
    max_records: int = 25,
    timespan: str = "72h",
    source_lang: Optional[str] = None,
    source_country: Optional[str] = None,
    sort: str = "DateDesc",
    timeout: int = 25,
) -> Dict:
    """
    Search GDELT DOC API for news articles.

    Args:
        query: Search keywords
        mode: ArtList (articles), TimelineVol (volume), ToneChart, etc.
        max_records: Max articles to return (up to 250)
        timespan: Time window (e.g., "24h", "72h", "7d")
        source_lang: Filter by language (e.g., "english", "spanish")
        source_country: Filter by source country
        sort: DateDesc, DateAsc, ToneDesc, ToneAsc
        timeout: Request timeout in seconds

    Returns:
        Dict with articles and metadata
    """
    params = {
        "query": query,
        "mode": mode,
        "maxrecords": str(max_records),
        "timespan": timespan,
        "sort": sort,
        "format": "json",
    }

    if source_lang:
        params["sourcelang"] = source_lang
    if source_country:
        params["sourcecountry"] = source_country

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                GDELT_DOC_API,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status != 200:
                    return {
                        "status": "error",
                        "error": f"GDELT API returned {resp.status}",
                        "query": query,
                        "articles": [],
                    }

                data = await resp.json(content_type=None)

                articles = []
                if "articles" in data:
                    for art in data["articles"][:max_records]:
                        articles.append({
                            "title": art.get("title", ""),
                            "url": art.get("url", ""),
                            "source": art.get("domain", art.get("source", "")),
                            "language": art.get("language", ""),
                            "country": art.get("sourcecountry", ""),
                            "date": art.get("seendate", ""),
                            "tone": art.get("tone", 0),
                            "image": art.get("socialimage", ""),
                        })

                return {
                    "status": "OK",
                    "query": query,
                    "count": len(articles),
                    "articles": articles,
                }

    except asyncio.TimeoutError:
        return {"status": "timeout", "query": query, "articles": []}
    except Exception as e:
        return {"status": "error", "error": str(e), "query": query, "articles": []}


# ═══════════════════════════════════════════════════════════════
# GDELT GEO API — Geographic Event Search
# ═══════════════════════════════════════════════════════════════

async def search_gdelt_geo(
    query: str,
    mode: str = "PointData",
    timespan: str = "72h",
    max_points: int = 50,
    timeout: int = 15,
) -> Dict:
    """
    Search GDELT GEO API for geolocated events.

    Returns events with lat/lon coordinates, useful for
    mapping disruptions near mines, ports, and transport routes.
    """
    params = {
        "query": query,
        "mode": mode,
        "timespan": timespan,
        "maxpoints": str(max_points),
        "format": "GeoJSON",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                GDELT_GEO_API,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status != 200:
                    return {
                        "status": "error",
                        "error": f"GDELT GEO API returned {resp.status}",
                        "query": query,
                        "events": [],
                    }

                data = await resp.json(content_type=None)

                events = []
                if "features" in data:
                    for feat in data["features"][:max_points]:
                        props = feat.get("properties", {})
                        coords = feat.get("geometry", {}).get("coordinates", [0, 0])
                        events.append({
                            "name": props.get("name", ""),
                            "url": props.get("url", ""),
                            "source": props.get("domain", ""),
                            "tone": props.get("tone", 0),
                            "lat": coords[1] if len(coords) > 1 else 0,
                            "lon": coords[0] if len(coords) > 0 else 0,
                            "date": props.get("date", ""),
                            "count": props.get("count", 1),
                        })

                return {
                    "status": "OK",
                    "query": query,
                    "count": len(events),
                    "events": events,
                }

    except asyncio.TimeoutError:
        return {"status": "timeout", "query": query, "events": []}
    except Exception as e:
        return {"status": "error", "error": str(e), "query": query, "events": []}


# ═══════════════════════════════════════════════════════════════
# COMMODITY DISRUPTION SCANNER
# ═══════════════════════════════════════════════════════════════

def _score_disruption(article: Dict) -> float:
    """Score an article's disruption potential (0-1)."""
    title = (article.get("title", "") or "").lower()
    score = 0.0

    # Check disruption keywords
    for keyword in DISRUPTION_SIGNALS:
        if keyword.lower() in title:
            score += 0.15

    # Check LatAm relevance
    for country in LATAM_COUNTRIES:
        if country.lower() in title:
            score += 0.1

    # Tone analysis (very negative = higher disruption)
    tone = article.get("tone", 0)
    if isinstance(tone, (int, float)):
        if tone < -5:
            score += 0.2
        elif tone < -2:
            score += 0.1

    return min(score, 1.0)


async def scan_commodity_disruptions(
    categories: Optional[List[str]] = None,
    timespan: str = "48h",
    min_disruption_score: float = 0.2,
) -> Dict:
    """
    Scan GDELT for commodity-relevant disruptions across all categories.

    Args:
        categories: List of categories to scan (default: all)
        timespan: How far back to look
        min_disruption_score: Minimum score to include (0-1)

    Returns:
        Dict with disruptions organized by category, scored and ranked
    """
    if categories is None:
        categories = list(COMMODITY_QUERIES.keys())

    all_results = {}
    tasks = []

    for category in categories:
        if category not in COMMODITY_QUERIES:
            continue
        queries = COMMODITY_QUERIES[category]
        for q in queries:
            tasks.append((category, q, search_gdelt_articles(
                query=q,
                timespan=timespan,
                max_records=10,
            )))

    # Execute all queries in parallel
    query_results = await asyncio.gather(
        *[t[2] for t in tasks],
        return_exceptions=True,
    )

    # Organize results by category
    for i, result in enumerate(query_results):
        category = tasks[i][0]
        query = tasks[i][1]

        if category not in all_results:
            all_results[category] = {
                "articles": [],
                "disruptions": [],
                "query_count": 0,
            }

        all_results[category]["query_count"] += 1

        if isinstance(result, Exception):
            continue

        if result.get("status") == "OK":
            for article in result.get("articles", []):
                score = _score_disruption(article)
                article["disruption_score"] = score
                article["query"] = query
                all_results[category]["articles"].append(article)

                if score >= min_disruption_score:
                    all_results[category]["disruptions"].append(article)

    # Sort disruptions by score (highest first)
    for category in all_results:
        all_results[category]["disruptions"].sort(
            key=lambda x: x.get("disruption_score", 0),
            reverse=True,
        )
        all_results[category]["total_articles"] = len(all_results[category]["articles"])
        all_results[category]["total_disruptions"] = len(all_results[category]["disruptions"])
        # Keep only top 5 disruptions per category for efficiency
        all_results[category]["disruptions"] = all_results[category]["disruptions"][:5]
        # Remove full articles list to save tokens
        del all_results[category]["articles"]

    return {
        "status": "OK",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "timespan": timespan,
        "categories_scanned": len(all_results),
        "results": all_results,
    }


# ═══════════════════════════════════════════════════════════════
# TARGETED DISRUPTION QUERY
# ═══════════════════════════════════════════════════════════════

async def query_disruption(
    commodity: str,
    region: str = "LatAm",
    timespan: str = "72h",
) -> Dict:
    """
    Targeted disruption query for a specific commodity and region.

    Example: query_disruption("coal", "Colombia")
    """
    query_parts = [commodity]
    if region:
        query_parts.append(region)

    # Add disruption keywords
    disruption_query = f'({commodity}) AND ({region}) AND (strike OR protest OR blockade OR disruption OR halt OR sanction)'

    articles = await search_gdelt_articles(
        query=disruption_query,
        timespan=timespan,
        max_records=25,
    )

    # Also get geographic events
    geo_events = await search_gdelt_geo(
        query=f"{commodity} {region}",
        timespan=timespan,
        max_points=20,
    )

    # Score and rank
    disruptions = []
    for art in articles.get("articles", []):
        score = _score_disruption(art)
        art["disruption_score"] = score
        if score >= 0.15:
            disruptions.append(art)

    disruptions.sort(key=lambda x: x.get("disruption_score", 0), reverse=True)

    return {
        "status": "OK",
        "commodity": commodity,
        "region": region,
        "timespan": timespan,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "disruptions_found": len(disruptions),
        "top_disruptions": disruptions[:10],
        "geo_events": geo_events.get("events", [])[:10],
    }


# ═══════════════════════════════════════════════════════════════
# VOLUME TREND ANALYSIS
# ═══════════════════════════════════════════════════════════════

async def get_news_volume_trend(
    query: str,
    timespan: str = "7d",
) -> Dict:
    """
    Get news volume timeline for a topic.
    Useful for detecting sudden spikes in coverage (early warning).
    """
    params = {
        "query": query,
        "mode": "TimelineVol",
        "timespan": timespan,
        "format": "json",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                GDELT_DOC_API,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    return {"status": "error", "query": query}

                data = await resp.json(content_type=None)

                timeline = []
                if "timeline" in data:
                    for series in data["timeline"]:
                        if "data" in series:
                            for point in series["data"]:
                                timeline.append({
                                    "date": point.get("date", ""),
                                    "volume": point.get("value", 0),
                                })

                # Detect spike (last data point vs average)
                if len(timeline) >= 3:
                    avg_vol = sum(t["volume"] for t in timeline[:-1]) / (len(timeline) - 1) if len(timeline) > 1 else 0
                    latest_vol = timeline[-1]["volume"] if timeline else 0
                    spike_ratio = latest_vol / avg_vol if avg_vol > 0 else 0
                else:
                    spike_ratio = 0

                return {
                    "status": "OK",
                    "query": query,
                    "timeline": timeline[-14:],  # Last 14 data points
                    "spike_ratio": round(spike_ratio, 2),
                    "spike_detected": spike_ratio > 2.0,
                }

    except Exception as e:
        return {"status": "error", "error": str(e), "query": query}


# ═══════════════════════════════════════════════════════════════
# TOOL WRAPPER (for agent integration)
# ═══════════════════════════════════════════════════════════════

async def gdelt_tool_handler(params: Dict) -> str:
    """
    Unified tool handler for the Meteoro Agentic System.

    Supported actions:
    - "scan": Full commodity disruption scan
    - "query": Targeted disruption query
    - "articles": Search articles by keyword
    - "trend": Get news volume trend
    """
    action = params.get("action", "scan")

    if action == "scan":
        categories = params.get("categories")
        timespan = params.get("timespan", "48h")
        result = await scan_commodity_disruptions(
            categories=categories,
            timespan=timespan,
        )

    elif action == "query":
        commodity = params.get("commodity", "coal")
        region = params.get("region", "Colombia")
        timespan = params.get("timespan", "72h")
        result = await query_disruption(commodity, region, timespan)

    elif action == "articles":
        query = params.get("query", "commodity disruption")
        timespan = params.get("timespan", "72h")
        result = await search_gdelt_articles(
            query=query,
            timespan=timespan,
            max_records=params.get("max_records", 15),
        )

    elif action == "trend":
        query = params.get("query", "coal disruption")
        result = await get_news_volume_trend(query, params.get("timespan", "7d"))

    else:
        result = {"status": "error", "error": f"Unknown action: {action}"}

    return json.dumps(result, default=str, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════

async def _test():
    """Quick test of GDELT integration."""
    print("=" * 60)
    print("METEORO AGENTIC SYSTEM — GDELT MONITOR TEST")
    print("=" * 60)

    # Test 1: Article search
    print("\n[1] Searching GDELT for coal disruptions...")
    result = await search_gdelt_articles("Colombia coal mine", timespan="7d", max_records=5)
    print(f"    Status: {result['status']}")
    print(f"    Articles found: {result.get('count', 0)}")
    for art in result.get("articles", [])[:3]:
        print(f"    → {art['title'][:80]}...")
        print(f"      Source: {art['source']} | Tone: {art.get('tone', 'N/A')}")

    # Test 2: Disruption scan
    print("\n[2] Scanning commodity disruptions (coal + copper)...")
    scan = await scan_commodity_disruptions(
        categories=["coal_colombia", "copper_peru"],
        timespan="72h",
    )
    print(f"    Status: {scan['status']}")
    for cat, data in scan.get("results", {}).items():
        print(f"    {cat}: {data['total_disruptions']} disruptions from {data['total_articles']} articles")
        for d in data.get("disruptions", [])[:2]:
            print(f"      → [{d['disruption_score']:.2f}] {d['title'][:70]}...")

    # Test 3: News volume trend
    print("\n[3] Checking news volume trend for 'Peru copper mine'...")
    trend = await get_news_volume_trend("Peru copper mine", timespan="7d")
    print(f"    Spike ratio: {trend.get('spike_ratio', 'N/A')}")
    print(f"    Spike detected: {trend.get('spike_detected', False)}")

    print("\n" + "=" * 60)
    print("GDELT MONITOR TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(_test())
