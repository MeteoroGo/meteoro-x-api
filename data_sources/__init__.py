"""
Meteoro Agentic System — Data Sources Layer
Asymmetric intelligence from sources the market doesn't read.
"""

# ── Existing: Alternative News Sources (SCMP, Nikkei, TASS, Global Times) ──
from .alternative_sources import (
    search_alternative_sources,
    search_scmp,
    search_nikkei_asia,
    search_tass_energy,
    search_global_times,
    analyze_geopolitical_impact,
)

# ── NEW: GDELT Geopolitical Monitor ──
from .gdelt_monitor import (
    search_gdelt_articles,
    search_gdelt_geo,
    scan_commodity_disruptions,
    query_disruption,
    get_news_volume_trend,
    gdelt_tool_handler,
)

# ── NEW: LatAm News Scrapers ──
from .latam_scraper import (
    scan_latam_sources,
    scan_latam_disruptions,
    check_government_gazettes,
    search_source as search_latam_source,
    latam_tool_handler,
)

# ── NEW: FRED API (Macro Data) ──
from .fred_api import (
    get_series_data as fred_get_series,
    get_macro_snapshot,
    get_coal_data,
    fred_tool_handler,
)

# ── NEW: NASA FIRMS (Satellite Thermal) ──
from .nasa_firms import (
    get_fires_in_area,
    monitor_site as firms_monitor_site,
    scan_all_sites as firms_scan_sites,
    firms_tool_handler,
)

# ── NEW: AIS Vessel Tracker ──
from .ais_tracker import (
    monitor_port,
    scan_all_ports,
    ais_tool_handler,
)
