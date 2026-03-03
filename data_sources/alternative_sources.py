#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  METEORO X v4.0 — ALTERNATIVE DATA SOURCES                       ║
║  Scrapers de fuentes asiáticas, rusas y no-convencionales         ║
║                                                                    ║
║  Fuentes:                                                          ║
║    - SCMP (South China Morning Post) — edición inglesa             ║
║    - Nikkei Asia — economía asiática                               ║
║    - TASS — agencia rusa (edición inglesa)                         ║
║    - Global Times — perspectiva china (inglés)                     ║
║    - Geopolitical Impact Mapper — evento→tickers                   ║
║                                                                    ║
║  Todas usan requests + BeautifulSoup.                              ║
║  Todas fallan gracefully sin crashear el sistema.                  ║
╚══════════════════════════════════════════════════════════════════╝
"""

import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

# ── Headers rotativos para evitar bloqueos ──
_HEADERS_LIST = [
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    },
]

_header_idx = 0


def _get_headers() -> dict:
    global _header_idx
    h = _HEADERS_LIST[_header_idx % len(_HEADERS_LIST)]
    _header_idx += 1
    return h


def _safe_get(url: str, timeout: int = 15) -> Optional[requests.Response]:
    """GET request with error handling."""
    try:
        resp = requests.get(url, headers=_get_headers(), timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        return resp
    except Exception:
        return None


def _extract_articles(soup: BeautifulSoup, selectors: List[str],
                      max_results: int = 6) -> List[Dict]:
    """Extract articles from HTML using CSS selectors."""
    articles = []
    for selector in selectors:
        elements = soup.select(selector)
        for el in elements[:max_results * 2]:
            title_el = el.select_one("h2, h3, h4, .title, .headline") or el
            link_el = el.select_one("a[href]") or el if el.name == "a" else None
            snippet_el = el.select_one("p, .summary, .description, .excerpt") or None

            title = title_el.get_text(strip=True) if title_el else ""
            url = link_el.get("href", "") if link_el else ""
            snippet = snippet_el.get_text(strip=True)[:200] if snippet_el else ""

            if title and len(title) > 10:
                articles.append({
                    "title": title[:150],
                    "url": url,
                    "snippet": snippet,
                })

            if len(articles) >= max_results:
                break
        if len(articles) >= max_results:
            break

    return articles[:max_results]


# ═══════════════════════════════════════════════════════════════
# SCMP — South China Morning Post
# ═══════════════════════════════════════════════════════════════

def search_scmp(query: str, max_results: int = 5) -> dict:
    """Busca en South China Morning Post (edición inglesa).
    SCMP es la fuente anglófona #1 sobre política y economía china.
    """
    try:
        search_url = f"https://www.scmp.com/search/{requests.utils.quote(query)}"
        resp = _safe_get(search_url)
        if resp is None:
            # Fallback: Google site search
            from meteoro_agent_core import search_web
            web_result = search_web(f"site:scmp.com {query}", max_results=max_results)
            if web_result.get("status") == "OK":
                return {
                    "status": "OK",
                    "source": "SCMP (via web search)",
                    "query": query,
                    "results": web_result.get("results", []),
                    "results_count": web_result.get("results_count", 0),
                }
            return {"status": "ERROR", "source": "SCMP", "query": query,
                    "error": "SCMP no accesible y fallback falló"}

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = _extract_articles(soup, [
            "article", ".search-result", ".article-item",
            ".listing-item", "div[class*='result']",
        ], max_results)

        # Fix relative URLs
        for a in articles:
            if a["url"] and not a["url"].startswith("http"):
                a["url"] = f"https://www.scmp.com{a['url']}"

        return {
            "status": "OK",
            "source": "SCMP",
            "query": query,
            "results": articles,
            "results_count": len(articles),
        }
    except Exception as e:
        return {"status": "ERROR", "source": "SCMP", "query": query,
                "error": str(e)[:200]}


# ═══════════════════════════════════════════════════════════════
# NIKKEI ASIA
# ═══════════════════════════════════════════════════════════════

def search_nikkei_asia(query: str, max_results: int = 5) -> dict:
    """Busca en Nikkei Asia — economía y mercados asiáticos."""
    try:
        search_url = f"https://asia.nikkei.com/search?query={requests.utils.quote(query)}"
        resp = _safe_get(search_url)
        if resp is None:
            from meteoro_agent_core import search_web
            web_result = search_web(f"site:asia.nikkei.com {query}", max_results=max_results)
            if web_result.get("status") == "OK":
                return {
                    "status": "OK",
                    "source": "Nikkei Asia (via web search)",
                    "query": query,
                    "results": web_result.get("results", []),
                    "results_count": web_result.get("results_count", 0),
                }
            return {"status": "ERROR", "source": "Nikkei Asia", "query": query,
                    "error": "Nikkei Asia no accesible"}

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = _extract_articles(soup, [
            "article", ".article-item", ".search-result",
            "div[class*='article']", ".story-card",
        ], max_results)

        for a in articles:
            if a["url"] and not a["url"].startswith("http"):
                a["url"] = f"https://asia.nikkei.com{a['url']}"

        return {
            "status": "OK",
            "source": "Nikkei Asia",
            "query": query,
            "results": articles,
            "results_count": len(articles),
        }
    except Exception as e:
        return {"status": "ERROR", "source": "Nikkei Asia", "query": query,
                "error": str(e)[:200]}


# ═══════════════════════════════════════════════════════════════
# TASS — Agencia Rusa (edición inglesa)
# ═══════════════════════════════════════════════════════════════

def search_tass_energy(query: str, max_results: int = 5) -> dict:
    """Busca en TASS (edición inglesa) — perspectiva rusa sobre energía."""
    try:
        search_url = f"https://tass.com/search?query={requests.utils.quote(query)}"
        resp = _safe_get(search_url)
        if resp is None:
            from meteoro_agent_core import search_web
            web_result = search_web(f"site:tass.com {query} energy", max_results=max_results)
            if web_result.get("status") == "OK":
                return {
                    "status": "OK",
                    "source": "TASS (via web search)",
                    "query": query,
                    "results": web_result.get("results", []),
                    "results_count": web_result.get("results_count", 0),
                }
            return {"status": "ERROR", "source": "TASS", "query": query,
                    "error": "TASS no accesible"}

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = _extract_articles(soup, [
            "article", ".news-item", ".search-result",
            "div[class*='news']", ".tass_pkg_title",
        ], max_results)

        for a in articles:
            if a["url"] and not a["url"].startswith("http"):
                a["url"] = f"https://tass.com{a['url']}"

        return {
            "status": "OK",
            "source": "TASS",
            "query": query,
            "results": articles,
            "results_count": len(articles),
        }
    except Exception as e:
        return {"status": "ERROR", "source": "TASS", "query": query,
                "error": str(e)[:200]}


# ═══════════════════════════════════════════════════════════════
# GLOBAL TIMES — Perspectiva China (inglés)
# ═══════════════════════════════════════════════════════════════

def search_global_times(query: str, max_results: int = 5) -> dict:
    """Busca en Global Times — perspectiva oficial china en inglés."""
    try:
        search_url = f"https://www.globaltimes.cn/search/index.html?query={requests.utils.quote(query)}"
        resp = _safe_get(search_url)
        if resp is None:
            from meteoro_agent_core import search_web
            web_result = search_web(f"site:globaltimes.cn {query}", max_results=max_results)
            if web_result.get("status") == "OK":
                return {
                    "status": "OK",
                    "source": "Global Times (via web search)",
                    "query": query,
                    "results": web_result.get("results", []),
                    "results_count": web_result.get("results_count", 0),
                }
            return {"status": "ERROR", "source": "Global Times", "query": query,
                    "error": "Global Times no accesible"}

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = _extract_articles(soup, [
            "article", ".search-result", ".article-item",
            "div[class*='row']", ".news-list li",
        ], max_results)

        for a in articles:
            if a["url"] and not a["url"].startswith("http"):
                a["url"] = f"https://www.globaltimes.cn{a['url']}"

        return {
            "status": "OK",
            "source": "Global Times",
            "query": query,
            "results": articles,
            "results_count": len(articles),
        }
    except Exception as e:
        return {"status": "ERROR", "source": "Global Times", "query": query,
                "error": str(e)[:200]}


# ═══════════════════════════════════════════════════════════════
# UNIFIED SEARCH — Busca en TODAS las fuentes
# ═══════════════════════════════════════════════════════════════

def search_alternative_sources(query: str, sources: str = "all",
                                max_results_per_source: int = 3) -> dict:
    """
    Busca en múltiples fuentes no-occidentales.

    Args:
        query: Búsqueda en inglés
        sources: "all" | "china" | "russia" | "asia"
        max_results_per_source: Máximo resultados por fuente

    Returns:
        Dict con resultados consolidados de todas las fuentes
    """
    all_results = []
    source_status = {}
    start = time.time()

    source_funcs = {
        "SCMP": search_scmp,
        "Nikkei_Asia": search_nikkei_asia,
        "TASS": search_tass_energy,
        "Global_Times": search_global_times,
    }

    # Filter by region
    if sources == "china":
        source_funcs = {k: v for k, v in source_funcs.items()
                        if k in ("SCMP", "Global_Times")}
    elif sources == "russia":
        source_funcs = {k: v for k, v in source_funcs.items()
                        if k in ("TASS",)}
    elif sources == "asia":
        source_funcs = {k: v for k, v in source_funcs.items()
                        if k in ("SCMP", "Nikkei_Asia")}

    for name, func in source_funcs.items():
        try:
            result = func(query, max_results=max_results_per_source)
            source_status[name] = result.get("status", "ERROR")
            if result.get("status") == "OK":
                for r in result.get("results", []):
                    r["source"] = result.get("source", name)
                    all_results.append(r)
        except Exception as e:
            source_status[name] = f"ERROR: {str(e)[:80]}"

    return {
        "status": "OK" if all_results else "PARTIAL",
        "query": query,
        "sources_queried": list(source_funcs.keys()),
        "source_status": source_status,
        "results": all_results,
        "results_count": len(all_results),
        "duration_s": round(time.time() - start, 2),
    }


# ═══════════════════════════════════════════════════════════════
# GEOPOLITICAL IMPACT MAPPER
# ═══════════════════════════════════════════════════════════════

# Mapping: keyword patterns → affected tickers + expected direction
_GEO_IMPACT_MAP = {
    # Energy
    "sanction russia": {"tickers": ["CL=F", "BZ=F", "NG=F"], "direction": "BULLISH",
                        "reason": "Sanciones a Rusia reducen oferta global de energía"},
    "opec cut": {"tickers": ["CL=F", "BZ=F"], "direction": "BULLISH",
                 "reason": "Recortes OPEC+ reducen oferta de crudo"},
    "opec increase": {"tickers": ["CL=F", "BZ=F"], "direction": "BEARISH",
                      "reason": "Aumento de producción OPEC+ incrementa oferta"},
    "iran deal": {"tickers": ["CL=F", "BZ=F"], "direction": "BEARISH",
                  "reason": "Acuerdo nuclear libera crudo iraní al mercado"},
    "middle east conflict": {"tickers": ["CL=F", "BZ=F", "GC=F"], "direction": "BULLISH",
                              "reason": "Conflicto amenaza suministro + refugio en oro"},
    "strait hormuz": {"tickers": ["CL=F", "BZ=F", "NG=F"], "direction": "BULLISH",
                       "reason": "Bloqueo de Hormuz corta 20% del flujo mundial de crudo"},
    "ukraine": {"tickers": ["NG=F", "CL=F", "GC=F"], "direction": "BULLISH",
                "reason": "Escalada Ucrania = riesgo energético Europa + refugio oro"},

    # Silver / Precious metals
    "silver": {"tickers": ["SI=F", "GC=F"], "direction": "VOLATILE",
               "reason": "Plata: dual demand (industrial + monetario), alta beta vs oro"},
    "silver demand": {"tickers": ["SI=F"], "direction": "BULLISH",
                       "reason": "Aumento de demanda industrial/solar de plata"},
    "solar panel": {"tickers": ["SI=F"], "direction": "BULLISH",
                     "reason": "Paneles solares = 10% de demanda global de plata"},
    "mexico mining": {"tickers": ["SI=F"], "direction": "BULLISH",
                       "reason": "México = 25% producción global de plata, disrupciones alcistas"},
    "peru mining": {"tickers": ["SI=F", "HG=F"], "direction": "BULLISH",
                     "reason": "Perú = productor clave de plata y cobre"},
    "precious metals": {"tickers": ["GC=F", "SI=F"], "direction": "BULLISH",
                         "reason": "Demanda de refugio en metales preciosos"},
    "dollar weakness": {"tickers": ["GC=F", "SI=F"], "direction": "BULLISH",
                         "reason": "Dólar débil favorece metales preciosos"},
    "gold silver ratio": {"tickers": ["SI=F", "GC=F"], "direction": "VOLATILE",
                           "reason": "Ratio oro/plata indica posible convergencia"},

    # Metals
    "china export ban": {"tickers": ["HG=F", "SLX", "SI=F"], "direction": "BULLISH",
                          "reason": "Ban de exportación china reduce oferta de metales"},
    "china stimulus": {"tickers": ["HG=F", "CL=F", "SLX", "SI=F"], "direction": "BULLISH",
                        "reason": "Estímulo chino aumenta demanda de commodities"},
    "china slowdown": {"tickers": ["HG=F", "CL=F", "SLX"], "direction": "BEARISH",
                        "reason": "Desaceleración china reduce demanda global"},
    "rare earth": {"tickers": ["HG=F", "SLX"], "direction": "BULLISH",
                    "reason": "Restricciones en tierras raras elevan precios de metales"},
    "lithium": {"tickers": ["SLX"], "direction": "VOLATILE",
                "reason": "Mercado de litio en transición energética"},

    # Gold / Safe haven
    "fed rate cut": {"tickers": ["GC=F", "SI=F"], "direction": "BULLISH",
                      "reason": "Recorte de tasas debilita dólar, fortalece metales preciosos"},
    "fed rate hike": {"tickers": ["GC=F", "SI=F"], "direction": "BEARISH",
                       "reason": "Subida de tasas fortalece dólar, presiona oro"},
    "inflation": {"tickers": ["GC=F", "SI=F", "CL=F"], "direction": "BULLISH",
                   "reason": "Inflación alta favorece commodities como cobertura"},
    "recession": {"tickers": ["GC=F", "CL=F", "HG=F"], "direction": "MIXED",
                   "reason": "Recesión: bullish oro (refugio), bearish crudo/cobre (demanda)"},

    # Coal
    "coal ban": {"tickers": ["BTU", "HCC", "ARLP"], "direction": "BEARISH",
                  "reason": "Prohibiciones de carbón reducen demanda"},
    "coal shortage": {"tickers": ["BTU", "HCC", "ARLP"], "direction": "BULLISH",
                       "reason": "Escasez de carbón eleva precios"},

    # Natural Gas
    "lng": {"tickers": ["NG=F"], "direction": "BULLISH",
            "reason": "Demanda de GNL en aumento por transición energética"},
    "pipeline": {"tickers": ["NG=F"], "direction": "VOLATILE",
                  "reason": "Eventos de pipeline afectan distribución de gas"},
}


def analyze_geopolitical_impact(event_description: str) -> dict:
    """
    Mapea un evento geopolítico a los tickers afectados y dirección esperada.

    Args:
        event_description: Descripción del evento en inglés

    Returns:
        Dict con tickers afectados, dirección, y razón
    """
    event_lower = event_description.lower()
    impacts = []

    for pattern, impact in _GEO_IMPACT_MAP.items():
        # Check if pattern keywords appear in event
        keywords = pattern.split()
        if all(kw in event_lower for kw in keywords):
            impacts.append({
                "pattern": pattern,
                "tickers": impact["tickers"],
                "direction": impact["direction"],
                "reason": impact["reason"],
            })

    # Consolidate unique tickers
    all_tickers = set()
    for imp in impacts:
        all_tickers.update(imp["tickers"])

    return {
        "status": "OK" if impacts else "NO_MATCH",
        "event": event_description[:200],
        "impacts": impacts,
        "all_affected_tickers": sorted(all_tickers),
        "matches_found": len(impacts),
    }
