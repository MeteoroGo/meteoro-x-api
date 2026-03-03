#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  METEORO AGENTIC SYSTEM — AIS VESSEL TRACKER                    ║
║  Maritime Intelligence for Commodity Supply Chain                ║
║                                                                  ║
║  Uses MarineTraffic public data + web scraping                   ║
║  Monitors bulk carrier activity at key commodity ports           ║
║                                                                  ║
║  Key ports monitored:                                            ║
║    - Puerto Bolívar (Colombia) — coal exports                    ║
║    - Santa Marta (Colombia) — coal exports                       ║
║    - Callao (Peru) — copper concentrate                          ║
║    - Matarani (Peru) — copper concentrate                        ║
║    - Richards Bay (South Africa) — thermal coal                  ║
║    - Newcastle (Australia) — thermal coal                        ║
║    - Qinhuangdao (China) — coal imports                          ║
║                                                                  ║
║  Logic: If bulk carriers stop arriving at a coal export port,    ║
║  production disruption is likely BEFORE official announcement.   ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import aiohttp
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

logger = logging.getLogger("meteoro.ais")

# ═══════════════════════════════════════════════════════════════
# PORT CONFIGURATIONS
# ═══════════════════════════════════════════════════════════════

MONITORED_PORTS = {
    # ── Colombia — Coal Export ────────────────────────────────
    "puerto_bolivar": {
        "name": "Puerto Bolívar",
        "country": "Colombia",
        "commodity": "coal",
        "lat": 12.22,
        "lon": -71.96,
        "unlocode": "COPBO",
        "marinetraffic_id": "792",
        "baseline_vessels": 8,  # Normal weekly bulk carrier count
        "vessel_types": ["Bulk Carrier", "General Cargo"],
        "significance": "Cerrejón's exclusive coal export terminal",
    },
    "santa_marta": {
        "name": "Santa Marta / Ciénaga",
        "country": "Colombia",
        "commodity": "coal",
        "lat": 11.25,
        "lon": -74.20,
        "unlocode": "COSMR",
        "marinetraffic_id": "793",
        "baseline_vessels": 6,
        "vessel_types": ["Bulk Carrier"],
        "significance": "Drummond coal exports. Adjacent coal terminals.",
    },

    # ── Peru — Copper Export ──────────────────────────────────
    "callao": {
        "name": "Callao",
        "country": "Peru",
        "commodity": "copper",
        "lat": -12.05,
        "lon": -77.15,
        "unlocode": "PECLL",
        "marinetraffic_id": "746",
        "baseline_vessels": 12,
        "vessel_types": ["Bulk Carrier", "General Cargo", "Container"],
        "significance": "Peru's main port. Copper concentrate exports.",
    },
    "matarani": {
        "name": "Matarani",
        "country": "Peru",
        "commodity": "copper",
        "lat": -17.00,
        "lon": -72.10,
        "unlocode": "PEMAT",
        "marinetraffic_id": "2389",
        "baseline_vessels": 4,
        "vessel_types": ["Bulk Carrier"],
        "significance": "Cerro Verde copper concentrate exports.",
    },

    # ── Benchmark Ports (Coal Reference) ──────────────────────
    "richards_bay": {
        "name": "Richards Bay",
        "country": "South Africa",
        "commodity": "coal",
        "lat": -28.78,
        "lon": 32.09,
        "unlocode": "ZARCB",
        "marinetraffic_id": "383",
        "baseline_vessels": 15,
        "vessel_types": ["Bulk Carrier"],
        "significance": "World's largest coal export terminal. API4 benchmark.",
    },
    "newcastle_au": {
        "name": "Newcastle",
        "country": "Australia",
        "commodity": "coal",
        "lat": -32.92,
        "lon": 151.78,
        "unlocode": "AUNTL",
        "marinetraffic_id": "145",
        "baseline_vessels": 20,
        "vessel_types": ["Bulk Carrier"],
        "significance": "World's largest coal export port. Newcastle/API8 benchmark.",
    },
    "qinhuangdao": {
        "name": "Qinhuangdao",
        "country": "China",
        "commodity": "coal",
        "lat": 39.93,
        "lon": 119.60,
        "unlocode": "CNQHD",
        "marinetraffic_id": "584",
        "baseline_vessels": 25,
        "vessel_types": ["Bulk Carrier"],
        "significance": "China's main coal import terminal. Qinhuangdao spot price benchmark.",
    },
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ═══════════════════════════════════════════════════════════════
# WEB-BASED PORT INTELLIGENCE
# ═══════════════════════════════════════════════════════════════

async def _fetch_port_web_data(port_config: Dict, timeout: int = 12) -> Dict:
    """
    Fetch port activity data via web scraping.
    Uses multiple public sources for redundancy.
    """
    port_name = port_config["name"]
    country = port_config["country"]

    # Strategy 1: Search for recent port news/activity
    from duckduckgo_search import DDGS
    search_results = []

    try:
        ddgs = DDGS(timeout=10)
        query = f"{port_name} {country} port vessel cargo {port_config['commodity']} 2026"
        results = list(ddgs.text(query, max_results=5))
        for r in results:
            search_results.append({
                "title": r.get("title", ""),
                "url": r.get("href", ""),
                "snippet": r.get("body", ""),
            })
    except Exception as e:
        logger.warning(f"DuckDuckGo search failed for {port_name}: {e}")

    # Strategy 2: Check VesselFinder (public data)
    vessel_data = await _check_vesselfinder(port_config)

    return {
        "news": search_results[:3],
        "vessels": vessel_data,
    }


async def _check_vesselfinder(port_config: Dict) -> Dict:
    """Check VesselFinder for public port data."""
    port_name = port_config["name"].replace(" ", "-").lower()
    url = f"https://www.vesselfinder.com/ports/{port_name}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=10),
                ssl=False,
            ) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")

                    # Try to extract vessel count from page
                    vessels_in_port = 0
                    expected_arrivals = 0

                    # Look for vessel count indicators
                    for text_el in soup.find_all(string=True):
                        text = str(text_el).strip().lower()
                        if "vessels in port" in text or "ships in port" in text:
                            import re
                            nums = re.findall(r'\d+', text)
                            if nums:
                                vessels_in_port = int(nums[0])
                        if "expected" in text or "arriving" in text:
                            import re
                            nums = re.findall(r'\d+', text)
                            if nums:
                                expected_arrivals = int(nums[0])

                    return {
                        "source": "vesselfinder",
                        "vessels_in_port": vessels_in_port,
                        "expected_arrivals": expected_arrivals,
                        "status": "OK" if vessels_in_port > 0 else "no_data",
                    }
                return {"source": "vesselfinder", "status": "unavailable"}

    except Exception as e:
        return {"source": "vesselfinder", "status": "error", "error": str(e)}


# ═══════════════════════════════════════════════════════════════
# PORT MONITORING
# ═══════════════════════════════════════════════════════════════

async def monitor_port(port_key: str) -> Dict:
    """
    Monitor activity at a specific port.
    Detects anomalies by comparing current vessel count to baseline.
    """
    if port_key not in MONITORED_PORTS:
        return {"status": "error", "error": f"Unknown port: {port_key}"}

    port = MONITORED_PORTS[port_key]
    web_data = await _fetch_port_web_data(port)

    vessel_count = web_data.get("vessels", {}).get("vessels_in_port", 0)
    baseline = port.get("baseline_vessels", 5)

    # Anomaly detection
    anomaly = "UNKNOWN"
    anomaly_score = 0.0

    if vessel_count > 0:
        ratio = vessel_count / baseline if baseline > 0 else 0
        if ratio < 0.3:
            anomaly = "LOW_TRAFFIC"
            anomaly_score = 0.7
        elif ratio < 0.6:
            anomaly = "BELOW_NORMAL"
            anomaly_score = 0.4
        elif ratio > 2.0:
            anomaly = "CONGESTION"
            anomaly_score = 0.6
        else:
            anomaly = "NORMAL"
            anomaly_score = 0.0
    else:
        # No vessel data available — use news as proxy
        anomaly = "DATA_UNAVAILABLE"
        anomaly_score = 0.0

    return {
        "status": "OK",
        "port": {
            "key": port_key,
            "name": port["name"],
            "country": port["country"],
            "commodity": port["commodity"],
            "significance": port["significance"],
        },
        "traffic": {
            "vessels_detected": vessel_count,
            "baseline": baseline,
            "ratio": round(vessel_count / baseline, 2) if baseline > 0 and vessel_count > 0 else 0,
            "source": web_data.get("vessels", {}).get("source", "unknown"),
        },
        "anomaly": {
            "status": anomaly,
            "score": anomaly_score,
            "interpretation": _interpret_port_anomaly(anomaly, port),
        },
        "news": web_data.get("news", []),
    }


def _interpret_port_anomaly(anomaly: str, port: Dict) -> str:
    """Human-readable interpretation of port anomaly."""
    name = port["name"]
    commodity = port["commodity"]

    interpretations = {
        "LOW_TRAFFIC": f"Significantly fewer vessels at {name}. Possible export disruption for {commodity}. Supply reduction likely.",
        "BELOW_NORMAL": f"Below-normal vessel activity at {name}. Minor {commodity} flow reduction possible.",
        "NORMAL": f"Normal vessel traffic at {name}. {commodity.title()} exports flowing normally.",
        "CONGESTION": f"Port congestion at {name}. Possible loading delays for {commodity}. Check for infrastructure issues.",
        "DATA_UNAVAILABLE": f"Unable to get real-time vessel data for {name}. Using news as proxy.",
        "UNKNOWN": f"Port status unknown for {name}.",
    }

    return interpretations.get(anomaly, f"Anomaly: {anomaly}")


async def scan_all_ports(commodity_filter: Optional[str] = None) -> Dict:
    """
    Scan all monitored ports for activity anomalies.

    Args:
        commodity_filter: Filter by commodity (e.g., "coal", "copper")
    """
    ports_to_scan = {}
    for key, config in MONITORED_PORTS.items():
        if commodity_filter and config["commodity"] != commodity_filter:
            continue
        ports_to_scan[key] = config

    tasks = [monitor_port(key) for key in ports_to_scan]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    ports_data = []
    alerts = []

    for i, result in enumerate(results):
        port_key = list(ports_to_scan.keys())[i]
        if isinstance(result, Exception):
            continue

        port_info = {
            "port_key": port_key,
            "name": result.get("port", {}).get("name", ""),
            "country": result.get("port", {}).get("country", ""),
            "commodity": result.get("port", {}).get("commodity", ""),
            "vessels": result.get("traffic", {}).get("vessels_detected", 0),
            "baseline": result.get("traffic", {}).get("baseline", 0),
            "anomaly": result.get("anomaly", {}).get("status", "UNKNOWN"),
            "score": result.get("anomaly", {}).get("score", 0),
            "interpretation": result.get("anomaly", {}).get("interpretation", ""),
        }
        ports_data.append(port_info)

        if port_info["score"] >= 0.4:
            alerts.append(port_info)

    ports_data.sort(key=lambda x: x.get("score", 0), reverse=True)

    return {
        "status": "OK",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ports_monitored": len(ports_data),
        "alerts_triggered": len(alerts),
        "commodity_filter": commodity_filter,
        "alerts": alerts,
        "all_ports": ports_data,
    }


# ═══════════════════════════════════════════════════════════════
# TOOL WRAPPER (for agent integration)
# ═══════════════════════════════════════════════════════════════

async def ais_tool_handler(params: Dict) -> str:
    """
    Unified tool handler for the Meteoro Agentic System.

    Supported actions:
    - "scan": Scan all ports
    - "port": Monitor specific port
    """
    action = params.get("action", "scan")

    if action == "scan":
        commodity = params.get("commodity")
        result = await scan_all_ports(commodity)

    elif action == "port":
        port_key = params.get("port", "puerto_bolivar")
        result = await monitor_port(port_key)

    else:
        result = {"status": "error", "error": f"Unknown action: {action}"}

    return json.dumps(result, default=str, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════

async def _test():
    """Quick test of AIS tracker."""
    print("=" * 60)
    print("METEORO AGENTIC SYSTEM — AIS VESSEL TRACKER TEST")
    print("=" * 60)

    # Test 1: Monitor Puerto Bolívar
    print("\n[1] Monitoring Puerto Bolívar (coal)...")
    pb = await monitor_port("puerto_bolivar")
    traffic = pb.get("traffic", {})
    print(f"    Vessels: {traffic.get('vessels_detected')} (baseline: {traffic.get('baseline')})")
    anomaly = pb.get("anomaly", {})
    print(f"    Status: {anomaly.get('status')} (score: {anomaly.get('score')})")
    for news in pb.get("news", [])[:2]:
        print(f"    News: {news['title'][:70]}...")

    # Test 2: Scan coal ports
    print("\n[2] Scanning all coal ports...")
    scan = await scan_all_ports(commodity_filter="coal")
    print(f"    Ports monitored: {scan['ports_monitored']}")
    print(f"    Alerts: {scan['alerts_triggered']}")
    for port in scan.get("all_ports", []):
        print(f"    {port['name']} ({port['country']}): {port['anomaly']}")

    print("\n" + "=" * 60)
    print("AIS TRACKER TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(_test())
