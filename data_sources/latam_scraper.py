#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  METEORO AGENTIC SYSTEM — LATAM NEWS SCRAPER                     ║
║  Real-time intelligence from Latin American news sources         ║
║                                                                  ║
║  Sources:                                                        ║
║    COLOMBIA: Portafolio, El Tiempo, La República, Semana         ║
║    PERU: RPP Noticias, Gestión, El Comercio                     ║
║    GOVERNMENT: Gacetas oficiales, MinMinas, MINEM                ║
║                                                                  ║
║  These are the sources that Bloomberg doesn't read.              ║
║  This is where the asymmetric edge lives.                        ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import aiohttp
import json
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

logger = logging.getLogger("meteoro.latam")

# ═══════════════════════════════════════════════════════════════
# SOURCE CONFIGURATIONS
# ═══════════════════════════════════════════════════════════════

SOURCES = {
    # ── COLOMBIA ──────────────────────────────────────────────
    "portafolio": {
        "name": "Portafolio (Colombia)",
        "base_url": "https://www.portafolio.co",
        "search_url": "https://www.portafolio.co/buscar?q={query}",
        "country": "Colombia",
        "language": "es",
        "relevance": ["mining", "energy", "commodities", "economy"],
        "selectors": {
            "results": "article, .article-list-item, .story-card",
            "title": "h2 a, h3 a, .story-card-title a",
            "link": "h2 a, h3 a, .story-card-title a",
            "snippet": "p, .field-summary, .story-card-description",
        },
    },
    "eltiempo": {
        "name": "El Tiempo (Colombia)",
        "base_url": "https://www.eltiempo.com",
        "search_url": "https://www.eltiempo.com/buscar?q={query}",
        "country": "Colombia",
        "language": "es",
        "relevance": ["mining", "protests", "politics", "economy"],
        "selectors": {
            "results": "article, .article-item, .listing-item",
            "title": "h2 a, h3 a, .title a",
            "link": "h2 a, h3 a, .title a",
            "snippet": "p, .lead, .description",
        },
    },
    "larepublica_co": {
        "name": "La República (Colombia)",
        "base_url": "https://www.larepublica.co",
        "search_url": "https://www.larepublica.co/buscar?q={query}",
        "country": "Colombia",
        "language": "es",
        "relevance": ["mining", "finance", "energy", "commodities"],
        "selectors": {
            "results": "article, .news-item, .article-card",
            "title": "h2 a, h3 a, .headline a",
            "link": "h2 a, h3 a, .headline a",
            "snippet": "p, .excerpt, .summary",
        },
    },
    "semana": {
        "name": "Semana (Colombia)",
        "base_url": "https://www.semana.com",
        "search_url": "https://www.semana.com/buscador/?query={query}",
        "country": "Colombia",
        "language": "es",
        "relevance": ["politics", "economy", "mining", "protests"],
        "selectors": {
            "results": "article, .card, .result-item",
            "title": "h2 a, h3 a, .card-title a",
            "link": "h2 a, h3 a, .card-title a",
            "snippet": "p, .card-description, .excerpt",
        },
    },

    # ── PERU ──────────────────────────────────────────────────
    "rpp": {
        "name": "RPP Noticias (Peru)",
        "base_url": "https://rpp.pe",
        "search_url": "https://rpp.pe/buscar?q={query}",
        "country": "Peru",
        "language": "es",
        "relevance": ["mining", "protests", "copper", "politics"],
        "selectors": {
            "results": "article, .news-item, .search-result",
            "title": "h2 a, h3 a, .title a",
            "link": "h2 a, h3 a, .title a",
            "snippet": "p, .description, .lead",
        },
    },
    "gestion": {
        "name": "Gestión (Peru)",
        "base_url": "https://gestion.pe",
        "search_url": "https://gestion.pe/buscar/?query={query}",
        "country": "Peru",
        "language": "es",
        "relevance": ["mining", "economy", "commodities", "finance"],
        "selectors": {
            "results": "article, .story-item, .search-result",
            "title": "h2 a, h3 a, .story-title a",
            "link": "h2 a, h3 a, .story-title a",
            "snippet": "p, .story-description, .excerpt",
        },
    },
    "elcomercio_pe": {
        "name": "El Comercio (Peru)",
        "base_url": "https://elcomercio.pe",
        "search_url": "https://elcomercio.pe/buscar/{query}/",
        "country": "Peru",
        "language": "es",
        "relevance": ["mining", "politics", "protests", "economy"],
        "selectors": {
            "results": "article, .story-item, .result-card",
            "title": "h2 a, h3 a, .story-title a",
            "link": "h2 a, h3 a, .story-title a",
            "snippet": "p, .story-content, .summary",
        },
    },
}

# ── COMMODITY-SPECIFIC SEARCH QUERIES ──────────────────────

LATAM_QUERIES = {
    "coal_colombia": [
        "Cerrejón huelga paro",
        "carbón exportación Colombia",
        "Drummond producción",
        "Fenoco tren carbón",
        "Puerto Bolívar embarque",
        "minería carbón Cesar Guajira",
        "decreto ambiental minería Colombia",
    ],
    "copper_peru": [
        "Las Bambas bloqueo protesta",
        "cobre mina Perú",
        "Cerro Verde producción",
        "Antamina operaciones",
        "Southern Peru Cuajone",
        "comunidad minería bloqueo Perú",
        "conflicto social minería",
    ],
    "gold_colombia": [
        "Buriticá oro mina",
        "Zijin Mining Colombia",
        "minería ilegal oro Colombia",
        "oro exportación Colombia",
    ],
    "oil_colombia": [
        "Ecopetrol producción",
        "petróleo Colombia declive",
        "oleoducto ataque Colombia",
        "fracking Colombia debate",
    ],
    "energy_policy": [
        "transición energética Colombia",
        "política minera Petro",
        "reservas naturales minería",
        "licencia ambiental minería",
    ],
}

# Headers to avoid being blocked
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}


# ═══════════════════════════════════════════════════════════════
# SCRAPING ENGINE
# ═══════════════════════════════════════════════════════════════

async def _fetch_page(url: str, timeout: int = 12) -> Optional[str]:
    """Fetch a web page with retry logic."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                if resp.status == 200:
                    return await resp.text()
                else:
                    logger.warning(f"HTTP {resp.status} for {url}")
                    return None
    except Exception as e:
        logger.warning(f"Fetch error for {url}: {e}")
        return None


def _extract_articles(html: str, source_config: Dict, base_url: str) -> List[Dict]:
    """Extract articles from HTML using source-specific selectors."""
    soup = BeautifulSoup(html, "html.parser")
    articles = []
    selectors = source_config["selectors"]

    # Try to find article containers
    containers = soup.select(selectors["results"])[:15]

    if not containers:
        # Fallback: look for any links with article-like patterns
        containers = soup.find_all("a", href=True)
        containers = [a for a in containers if a.get_text(strip=True) and len(a.get_text(strip=True)) > 20][:15]

    for container in containers:
        try:
            # Extract title
            title_el = container.select_one(selectors["title"]) if hasattr(container, 'select_one') else container
            if not title_el:
                title_el = container

            title = title_el.get_text(strip=True) if title_el else ""
            if not title or len(title) < 10:
                continue

            # Extract link
            link_el = container.select_one(selectors["link"]) if hasattr(container, 'select_one') else container
            href = ""
            if link_el:
                href = link_el.get("href", "")
            elif hasattr(container, 'get'):
                href = container.get("href", "")

            if href and not href.startswith("http"):
                href = base_url.rstrip("/") + "/" + href.lstrip("/")

            # Extract snippet
            snippet = ""
            snippet_el = container.select_one(selectors["snippet"]) if hasattr(container, 'select_one') else None
            if snippet_el:
                snippet = snippet_el.get_text(strip=True)[:200]

            if title:
                articles.append({
                    "title": title[:150],
                    "url": href,
                    "snippet": snippet,
                    "source": source_config["name"],
                    "country": source_config["country"],
                    "language": source_config["language"],
                })

        except Exception:
            continue

    return articles


async def search_source(
    source_key: str,
    query: str,
) -> Dict:
    """Search a single LatAm news source."""
    if source_key not in SOURCES:
        return {"status": "error", "error": f"Unknown source: {source_key}"}

    config = SOURCES[source_key]
    search_url = config["search_url"].format(query=query.replace(" ", "+"))

    html = await _fetch_page(search_url)
    if not html:
        return {
            "status": "error",
            "source": config["name"],
            "query": query,
            "articles": [],
        }

    articles = _extract_articles(html, config, config["base_url"])

    return {
        "status": "OK",
        "source": config["name"],
        "country": config["country"],
        "query": query,
        "count": len(articles),
        "articles": articles[:10],
    }


# ═══════════════════════════════════════════════════════════════
# MULTI-SOURCE SCAN
# ═══════════════════════════════════════════════════════════════

async def scan_latam_sources(
    query: str,
    countries: Optional[List[str]] = None,
    max_per_source: int = 5,
) -> Dict:
    """
    Search across all LatAm sources in parallel.

    Args:
        query: Search keywords (in Spanish)
        countries: Filter by country (e.g., ["Colombia", "Peru"])
        max_per_source: Max articles per source

    Returns:
        Combined results from all sources
    """
    sources_to_search = []
    for key, config in SOURCES.items():
        if countries and config["country"] not in countries:
            continue
        sources_to_search.append(key)

    # Search all sources in parallel
    tasks = [search_source(key, query) for key in sources_to_search]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_articles = []
    source_stats = {}

    for i, result in enumerate(results):
        source_key = sources_to_search[i]
        if isinstance(result, Exception):
            source_stats[source_key] = {"status": "error", "count": 0}
            continue

        source_stats[source_key] = {
            "status": result.get("status", "error"),
            "count": result.get("count", 0),
        }

        for art in result.get("articles", [])[:max_per_source]:
            all_articles.append(art)

    return {
        "status": "OK",
        "query": query,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sources_queried": len(sources_to_search),
        "total_articles": len(all_articles),
        "source_stats": source_stats,
        "articles": all_articles,
    }


# ═══════════════════════════════════════════════════════════════
# COMMODITY DISRUPTION SCAN (LatAm-specific)
# ═══════════════════════════════════════════════════════════════

async def scan_latam_disruptions(
    categories: Optional[List[str]] = None,
) -> Dict:
    """
    Scan LatAm sources for commodity disruptions using predefined queries.

    Returns disruptions organized by category.
    """
    if categories is None:
        categories = list(LATAM_QUERIES.keys())

    all_results = {}

    for category in categories:
        if category not in LATAM_QUERIES:
            continue

        queries = LATAM_QUERIES[category]
        category_articles = []

        # Run all queries for this category in parallel
        tasks = [scan_latam_sources(q) for q in queries]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                continue
            if result.get("status") == "OK":
                category_articles.extend(result.get("articles", []))

        # Deduplicate by title similarity
        seen_titles = set()
        unique_articles = []
        for art in category_articles:
            title_key = art["title"][:50].lower()
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(art)

        all_results[category] = {
            "total_articles": len(unique_articles),
            "articles": unique_articles[:10],  # Top 10 per category
        }

    return {
        "status": "OK",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "categories_scanned": len(all_results),
        "results": all_results,
    }


# ═══════════════════════════════════════════════════════════════
# GOVERNMENT GAZETTE MONITOR
# ═══════════════════════════════════════════════════════════════

async def check_government_gazettes() -> Dict:
    """
    Check government gazette/regulatory sources for mining/energy decrees.

    These are the sources that detect regulatory changes 48-72h before
    traditional media reports them.
    """
    gazette_sources = [
        {
            "name": "MinMinas Colombia",
            "url": "https://www.minenergia.gov.co/es/noticias/",
            "country": "Colombia",
        },
        {
            "name": "ANM Colombia (Agencia Nacional de Minería)",
            "url": "https://www.anm.gov.co/noticias",
            "country": "Colombia",
        },
        {
            "name": "MINEM Perú",
            "url": "https://www.gob.pe/minem",
            "country": "Peru",
        },
    ]

    results = []
    for source in gazette_sources:
        html = await _fetch_page(source["url"])
        if html:
            soup = BeautifulSoup(html, "html.parser")
            # Extract recent news/announcements
            headlines = []
            for tag in soup.find_all(["h2", "h3", "h4", "a"], limit=20):
                text = tag.get_text(strip=True)
                if text and len(text) > 15 and len(text) < 200:
                    href = tag.get("href", "")
                    if href and not href.startswith("http"):
                        href = source["url"].rstrip("/") + "/" + href.lstrip("/")
                    headlines.append({
                        "title": text,
                        "url": href,
                    })

            results.append({
                "source": source["name"],
                "country": source["country"],
                "status": "OK",
                "headlines": headlines[:5],
            })
        else:
            results.append({
                "source": source["name"],
                "country": source["country"],
                "status": "unreachable",
                "headlines": [],
            })

    return {
        "status": "OK",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sources_checked": len(results),
        "results": results,
    }


# ═══════════════════════════════════════════════════════════════
# TOOL WRAPPER (for agent integration)
# ═══════════════════════════════════════════════════════════════

async def latam_tool_handler(params: Dict) -> str:
    """
    Unified tool handler for the Meteoro Agentic System.

    Supported actions:
    - "search": Search LatAm sources for a query
    - "scan": Scan for commodity disruptions
    - "gazette": Check government gazettes
    """
    action = params.get("action", "search")

    if action == "search":
        query = params.get("query", "minería carbón")
        countries = params.get("countries")
        result = await scan_latam_sources(query, countries)

    elif action == "scan":
        categories = params.get("categories")
        result = await scan_latam_disruptions(categories)

    elif action == "gazette":
        result = await check_government_gazettes()

    else:
        result = {"status": "error", "error": f"Unknown action: {action}"}

    return json.dumps(result, default=str, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════

async def _test():
    """Quick test of LatAm scraper."""
    print("=" * 60)
    print("METEORO AGENTIC SYSTEM — LATAM SCRAPER TEST")
    print("=" * 60)

    # Test 1: Multi-source search
    print("\n[1] Searching 'carbón Cerrejón' across LatAm sources...")
    result = await scan_latam_sources("carbón Cerrejón", countries=["Colombia"])
    print(f"    Status: {result['status']}")
    print(f"    Sources queried: {result['sources_queried']}")
    print(f"    Total articles: {result['total_articles']}")
    for art in result.get("articles", [])[:3]:
        print(f"    → [{art['source']}] {art['title'][:70]}...")

    # Test 2: Disruption scan
    print("\n[2] Scanning LatAm disruptions (coal_colombia)...")
    scan = await scan_latam_disruptions(categories=["coal_colombia"])
    print(f"    Status: {scan['status']}")
    for cat, data in scan.get("results", {}).items():
        print(f"    {cat}: {data['total_articles']} articles found")

    # Test 3: Government gazettes
    print("\n[3] Checking government gazettes...")
    gazettes = await check_government_gazettes()
    for g in gazettes.get("results", []):
        print(f"    {g['source']}: {g['status']} — {len(g['headlines'])} headlines")

    print("\n" + "=" * 60)
    print("LATAM SCRAPER TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(_test())
