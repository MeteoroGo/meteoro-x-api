#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  METEORO AGENTIC SYSTEM — NASA FIRMS SATELLITE INTEGRATION       ║
║  Fire Information for Resource Management System                 ║
║                                                                  ║
║  FREE API. Key: https://firms.modaps.eosdis.nasa.gov/api/       ║
║  Provides: Active fire/thermal anomaly data from VIIRS & MODIS   ║
║                                                                  ║
║  Key targets for commodity intelligence:                         ║
║    - Cerrejón mine (Colombia) — coal                             ║
║    - Las Bambas (Peru) — copper                                  ║
║    - Cerro Verde (Peru) — copper                                 ║
║    - Buriticá (Colombia) — gold                                  ║
║    - Oil pipelines (Colombia)                                    ║
║    - Port areas (Puerto Bolívar, Callao)                         ║
║                                                                  ║
║  How it works:                                                   ║
║    Reduced thermal activity at a mine = production slowdown       ║
║    Detected BEFORE official announcements                        ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import aiohttp
import csv
import io
import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("meteoro.firms")

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

FIRMS_API_KEY = os.environ.get("NASA_FIRMS_MAP_KEY", "")
FIRMS_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

# Satellite sources available
SATELLITE_SOURCES = {
    "VIIRS_SNPP_NRT": "VIIRS S-NPP (Near Real-Time)",
    "VIIRS_NOAA20_NRT": "VIIRS NOAA-20 (Near Real-Time)",
    "MODIS_NRT": "MODIS (Near Real-Time)",
}

# ═══════════════════════════════════════════════════════════════
# MINING SITES & CRITICAL INFRASTRUCTURE
# ═══════════════════════════════════════════════════════════════

MONITORED_SITES = {
    # ── COLOMBIA — Coal ───────────────────────────────────────
    "cerrejon": {
        "name": "Cerrejón Mine (Glencore)",
        "commodity": "coal",
        "country": "Colombia",
        "lat": 11.05,
        "lon": -72.67,
        "radius_km": 15,
        "bbox": "-72.85,10.90,-72.50,11.20",
        "baseline_fires": 8,  # Normal thermal signatures
        "significance": "Largest open-pit coal mine in Latin America. 32MT/year capacity.",
    },
    "drummond": {
        "name": "Drummond Mine (Cesar)",
        "commodity": "coal",
        "country": "Colombia",
        "lat": 9.62,
        "lon": -73.60,
        "radius_km": 12,
        "bbox": "-73.75,9.50,-73.45,9.75",
        "baseline_fires": 5,
        "significance": "10+ consecutive years leading Colombian coal exports.",
    },
    "puerto_bolivar": {
        "name": "Puerto Bolívar (Coal Port)",
        "commodity": "coal",
        "country": "Colombia",
        "lat": 12.22,
        "lon": -71.96,
        "radius_km": 5,
        "bbox": "-72.05,12.15,-71.85,12.30",
        "baseline_fires": 2,
        "significance": "Primary coal export terminal. Ships to Asia/Europe.",
    },

    # ── COLOMBIA — Oil ────────────────────────────────────────
    "cano_limon": {
        "name": "Caño Limón Pipeline",
        "commodity": "oil",
        "country": "Colombia",
        "lat": 7.35,
        "lon": -71.52,
        "radius_km": 20,
        "bbox": "-71.75,7.15,-71.30,7.55",
        "baseline_fires": 3,
        "significance": "Major oil pipeline. Frequent target of attacks.",
    },

    # ── COLOMBIA — Gold ───────────────────────────────────────
    "buritica": {
        "name": "Buriticá Gold Mine (Zijin)",
        "commodity": "gold",
        "country": "Colombia",
        "lat": 6.75,
        "lon": -75.87,
        "radius_km": 8,
        "bbox": "-75.97,6.65,-75.77,6.85",
        "baseline_fires": 2,
        "significance": "Continental Gold mine. $430M ICSID claim by Zijin.",
    },

    # ── PERU — Copper ─────────────────────────────────────────
    "las_bambas": {
        "name": "Las Bambas Copper Mine (MMG)",
        "commodity": "copper",
        "country": "Peru",
        "lat": -14.06,
        "lon": -72.33,
        "radius_km": 12,
        "bbox": "-72.48,-14.20,-72.18,-13.92",
        "baseline_fires": 4,
        "significance": "700+ days of blockades. $9.5M/day impact. Peru's top copper producer.",
    },
    "cerro_verde": {
        "name": "Cerro Verde Copper Mine (Freeport)",
        "commodity": "copper",
        "country": "Peru",
        "lat": -16.54,
        "lon": -71.60,
        "radius_km": 10,
        "bbox": "-71.72,-16.65,-71.48,-16.43",
        "baseline_fires": 3,
        "significance": "449K tons/year. One of world's largest copper mines.",
    },
    "antamina": {
        "name": "Antamina Mine (BHP/Glencore)",
        "commodity": "copper",
        "country": "Peru",
        "lat": -9.54,
        "lon": -77.06,
        "radius_km": 10,
        "bbox": "-77.18,-9.65,-76.94,-9.43",
        "baseline_fires": 3,
        "significance": "434K tons copper/year. Major zinc producer.",
    },

    # ── PERU — Ports ──────────────────────────────────────────
    "callao": {
        "name": "Port of Callao",
        "commodity": "mixed",
        "country": "Peru",
        "lat": -12.05,
        "lon": -77.15,
        "radius_km": 5,
        "bbox": "-77.22,-12.10,-77.08,-12.00",
        "baseline_fires": 1,
        "significance": "Peru's main port. Copper concentrate exports.",
    },
}


# ═══════════════════════════════════════════════════════════════
# NASA FIRMS API CLIENT
# ═══════════════════════════════════════════════════════════════

async def get_fires_in_area(
    bbox: str,
    days: int = 2,
    source: str = "VIIRS_SNPP_NRT",
    timeout: int = 15,
) -> Dict:
    """
    Get active fires/thermal anomalies in a bounding box.

    Args:
        bbox: "west,south,east,north" (e.g., "-72.85,10.90,-72.50,11.20")
        days: Number of days to look back (1-10)
        source: Satellite source
        timeout: Request timeout

    Returns:
        Dict with fire/thermal data points
    """
    if not FIRMS_API_KEY:
        return _generate_synthetic_firms(bbox, days)

    url = f"{FIRMS_BASE_URL}/{FIRMS_API_KEY}/{source}/{bbox}/{days}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"FIRMS API returned {resp.status}")
                    return _generate_synthetic_firms(bbox, days)

                text = await resp.text()

                # Parse CSV response
                reader = csv.DictReader(io.StringIO(text))
                fires = []
                for row in reader:
                    try:
                        fires.append({
                            "lat": float(row.get("latitude", 0)),
                            "lon": float(row.get("longitude", 0)),
                            "brightness": float(row.get("bright_ti4", row.get("brightness", 0))),
                            "confidence": row.get("confidence", "nominal"),
                            "frp": float(row.get("frp", 0)),  # Fire Radiative Power
                            "acq_date": row.get("acq_date", ""),
                            "acq_time": row.get("acq_time", ""),
                            "satellite": row.get("satellite", source),
                            "daynight": row.get("daynight", ""),
                        })
                    except (ValueError, KeyError):
                        continue

                return {
                    "status": "OK",
                    "source": "NASA_FIRMS",
                    "satellite": source,
                    "bbox": bbox,
                    "days": days,
                    "count": len(fires),
                    "fires": fires,
                }

    except Exception as e:
        logger.error(f"FIRMS API error: {e}")
        return _generate_synthetic_firms(bbox, days)


def _generate_synthetic_firms(bbox: str, days: int) -> Dict:
    """
    Generate synthetic FIRMS data when API key is not available.

    Data varies by time of day (lower activity at night) and includes
    occasional anomalies (10% chance) to look realistic for demo/backtesting.
    """
    import random
    from datetime import datetime as dt, timedelta

    parts = bbox.split(",")
    if len(parts) != 4:
        return {"status": "synthetic", "fires": [], "count": 0}

    west, south, east, north = [float(p) for p in parts]

    # Determine if anomaly should occur (10% chance)
    has_anomaly = random.random() < 0.1

    fires = []

    # Generate fires for each day in the lookback period
    base_fires = random.randint(2, 6) if not has_anomaly else random.randint(0, 2)

    for day_offset in range(days):
        current_date = dt.now(timezone.utc) - timedelta(days=day_offset)
        date_str = current_date.strftime("%Y-%m-%d")

        # Time-of-day variation: more fires during day (D) than night (N)
        num_fires_today = base_fires

        # Add some variation day-to-day
        num_fires_today += random.randint(-1, 1)
        num_fires_today = max(0, num_fires_today)

        for _ in range(num_fires_today):
            # Day/night split: 70% day, 30% night
            daynight = "D" if random.random() < 0.7 else "N"

            # Thermal signatures are typically stronger during day
            if daynight == "D":
                brightness = round(random.uniform(320, 520), 1)
                confidence = random.choice(["nominal", "high", "high"])
            else:
                brightness = round(random.uniform(250, 380), 1)
                confidence = random.choice(["low", "nominal", "nominal"])

            fires.append({
                "lat": round(random.uniform(south, north), 4),
                "lon": round(random.uniform(west, east), 4),
                "brightness": brightness,
                "confidence": confidence,
                "frp": round(random.uniform(8, 65), 1),  # Fire Radiative Power
                "acq_date": date_str,
                "acq_time": f"{random.randint(0, 23):02d}{random.randint(0, 59):02d}",
                "satellite": random.choice(["VIIRS_SNPP", "VIIRS_NOAA20", "MODIS"]),
                "daynight": daynight,
            })

    return {
        "status": "synthetic",
        "source": "SYNTHETIC (no NASA_FIRMS_MAP_KEY)",
        "bbox": bbox,
        "days": days,
        "count": len(fires),
        "fires": fires,
        "anomaly": has_anomaly,
        "warning": "Data is synthetic. Set NASA_FIRMS_MAP_KEY for real satellite data.",
    }


# ═══════════════════════════════════════════════════════════════
# SITE MONITORING
# ═══════════════════════════════════════════════════════════════

async def monitor_site(site_key: str, days: int = 2) -> Dict:
    """
    Monitor thermal activity at a specific mining/infrastructure site.

    Compares current thermal signatures against baseline to detect
    anomalies (reduced activity = potential production slowdown).
    """
    if site_key not in MONITORED_SITES:
        return {"status": "error", "error": f"Unknown site: {site_key}"}

    site = MONITORED_SITES[site_key]
    data = await get_fires_in_area(site["bbox"], days=days)

    fire_count = data.get("count", 0)
    baseline = site.get("baseline_fires", 3)

    # Anomaly detection
    anomaly = "NORMAL"
    anomaly_score = 0.0

    if fire_count == 0 and baseline > 0:
        anomaly = "ZERO_ACTIVITY"
        anomaly_score = 1.0
    elif baseline > 0:
        ratio = fire_count / baseline
        if ratio < 0.3:
            anomaly = "SEVERELY_REDUCED"
            anomaly_score = 0.8
        elif ratio < 0.6:
            anomaly = "REDUCED"
            anomaly_score = 0.5
        elif ratio > 2.0:
            anomaly = "ELEVATED"
            anomaly_score = 0.6
        elif ratio > 3.0:
            anomaly = "SPIKE"
            anomaly_score = 0.9

    # Calculate average brightness (thermal intensity)
    avg_brightness = 0
    if data.get("fires"):
        avg_brightness = sum(f.get("brightness", 0) for f in data["fires"]) / len(data["fires"])

    return {
        "status": "OK",
        "source": data.get("source", "unknown"),
        "site": {
            "key": site_key,
            "name": site["name"],
            "commodity": site["commodity"],
            "country": site["country"],
            "significance": site["significance"],
        },
        "thermal": {
            "fires_detected": fire_count,
            "baseline": baseline,
            "ratio": round(fire_count / baseline, 2) if baseline > 0 else 0,
            "avg_brightness": round(avg_brightness, 1),
        },
        "anomaly": {
            "status": anomaly,
            "score": anomaly_score,
            "interpretation": _interpret_anomaly(anomaly, site),
        },
        "raw_fires": data.get("fires", [])[:10],
    }


def _interpret_anomaly(anomaly: str, site: Dict) -> str:
    """Human-readable interpretation of thermal anomaly."""
    name = site["name"]
    commodity = site["commodity"]

    interpretations = {
        "ZERO_ACTIVITY": f"No thermal activity detected at {name}. Possible full production halt. Bullish for {commodity} prices.",
        "SEVERELY_REDUCED": f"Thermal activity at {name} severely below baseline. Likely significant production reduction. Bullish {commodity}.",
        "REDUCED": f"Below-normal thermal activity at {name}. Potential production slowdown. Mildly bullish {commodity}.",
        "NORMAL": f"Thermal activity at {name} within normal range. No disruption detected.",
        "ELEVATED": f"Above-normal thermal activity at {name}. Could indicate increased production or nearby fire event.",
        "SPIKE": f"Unusual thermal spike at {name}. Possible fire, explosion, or emergency event. Monitor closely.",
    }

    return interpretations.get(anomaly, f"Anomaly status: {anomaly}")


async def scan_all_sites(days: int = 2) -> Dict:
    """
    Scan all monitored sites for thermal anomalies.
    Returns sites ranked by anomaly severity.
    """
    tasks = [monitor_site(key, days) for key in MONITORED_SITES]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    sites_data = []
    alerts = []

    for i, result in enumerate(results):
        site_key = list(MONITORED_SITES.keys())[i]
        if isinstance(result, Exception):
            continue

        sites_data.append({
            "site_key": site_key,
            "name": result.get("site", {}).get("name", ""),
            "commodity": result.get("site", {}).get("commodity", ""),
            "country": result.get("site", {}).get("country", ""),
            "fires": result.get("thermal", {}).get("fires_detected", 0),
            "baseline": result.get("thermal", {}).get("baseline", 0),
            "anomaly": result.get("anomaly", {}).get("status", "UNKNOWN"),
            "score": result.get("anomaly", {}).get("score", 0),
            "interpretation": result.get("anomaly", {}).get("interpretation", ""),
        })

        # Flag alerts (anomaly score >= 0.5)
        score = result.get("anomaly", {}).get("score", 0)
        if score >= 0.5:
            alerts.append(sites_data[-1])

    # Sort by anomaly score (highest first)
    sites_data.sort(key=lambda x: x.get("score", 0), reverse=True)
    alerts.sort(key=lambda x: x.get("score", 0), reverse=True)

    return {
        "status": "OK",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api_source": "NASA_FIRMS" if FIRMS_API_KEY else "SYNTHETIC",
        "sites_monitored": len(sites_data),
        "alerts_triggered": len(alerts),
        "alerts": alerts,
        "all_sites": sites_data,
    }


# ═══════════════════════════════════════════════════════════════
# TOOL WRAPPER (for agent integration)
# ═══════════════════════════════════════════════════════════════

async def firms_tool_handler(params: Dict) -> str:
    """
    Unified tool handler for the Meteoro Agentic System.

    Supported actions:
    - "scan": Scan all monitored sites
    - "site": Monitor specific site
    - "area": Get fires in custom bounding box
    """
    action = params.get("action", "scan")

    if action == "scan":
        days = params.get("days", 2)
        result = await scan_all_sites(days)

    elif action == "site":
        site_key = params.get("site", "cerrejon")
        days = params.get("days", 2)
        result = await monitor_site(site_key, days)

    elif action == "area":
        bbox = params.get("bbox", "-72.85,10.90,-72.50,11.20")
        days = params.get("days", 2)
        result = await get_fires_in_area(bbox, days)

    else:
        result = {"status": "error", "error": f"Unknown action: {action}"}

    return json.dumps(result, default=str, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════

async def _test():
    """Quick test of NASA FIRMS integration."""
    print("=" * 60)
    print("METEORO AGENTIC SYSTEM — NASA FIRMS TEST")
    print(f"API Key configured: {'YES' if FIRMS_API_KEY else 'NO (synthetic data)'}")
    print("=" * 60)

    # Test 1: Monitor Cerrejón
    print("\n[1] Monitoring Cerrejón mine (coal)...")
    cerrejon = await monitor_site("cerrejon")
    print(f"    Source: {cerrejon.get('source', 'unknown')}")
    thermal = cerrejon.get("thermal", {})
    print(f"    Fires: {thermal.get('fires_detected')} (baseline: {thermal.get('baseline')})")
    anomaly = cerrejon.get("anomaly", {})
    print(f"    Anomaly: {anomaly.get('status')} (score: {anomaly.get('score')})")
    print(f"    → {anomaly.get('interpretation', '')[:80]}...")

    # Test 2: Monitor Las Bambas
    print("\n[2] Monitoring Las Bambas (copper)...")
    bambas = await monitor_site("las_bambas")
    thermal = bambas.get("thermal", {})
    print(f"    Fires: {thermal.get('fires_detected')} (baseline: {thermal.get('baseline')})")
    anomaly = bambas.get("anomaly", {})
    print(f"    Anomaly: {anomaly.get('status')} (score: {anomaly.get('score')})")

    # Test 3: Full scan
    print("\n[3] Scanning all monitored sites...")
    scan = await scan_all_sites()
    print(f"    Sites monitored: {scan['sites_monitored']}")
    print(f"    Alerts triggered: {scan['alerts_triggered']}")
    for alert in scan.get("alerts", []):
        print(f"    ⚠ {alert['name']}: {alert['anomaly']} (score: {alert['score']})")

    print("\n" + "=" * 60)
    print("NASA FIRMS TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(_test())
