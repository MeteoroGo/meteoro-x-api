#!/usr/bin/env python3
"""
METEORO X — INDUSTRY KNOWLEDGE GRAPH
=====================================
Comprehensive knowledge base of the global commodity industry:
  - Global exchanges and their commodity specializations
  - Major traders (physical + financial)
  - Major mines with geographic coordinates
  - Refineries and smelters with locations
  - Supply chain relationships (mine → smelter → refinery → end user)

This module gives the agentic system INSTITUTIONAL-GRADE context
that transforms generic price analysis into actionable intelligence.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field


# ═══════════════════════════════════════════════════════════════
# GLOBAL EXCHANGES
# Where commodities are actually priced and traded
# ═══════════════════════════════════════════════════════════════

EXCHANGES = {
    # ── METALS ──────────────────────────────────────────────
    "LME": {
        "name": "London Metal Exchange",
        "city": "London", "country": "UK",
        "lat": 51.5139, "lon": -0.0827,
        "url": "https://www.lme.com",
        "commodities": ["copper", "aluminum", "zinc", "tin", "nickel", "lead", "cobalt"],
        "currency": "USD",
        "importance": "Global benchmark for base metals. Sets the world reference price.",
        "trading_hours_utc": "01:00-19:00",
        "key_contracts": {
            "copper": "LME Copper (Grade A) — 25 tonnes/lot",
            "aluminum": "LME Aluminium — 25 tonnes/lot",
            "zinc": "LME Zinc (SHG) — 25 tonnes/lot",
            "tin": "LME Tin — 5 tonnes/lot",
            "nickel": "LME Nickel — 6 tonnes/lot",
            "lead": "LME Lead — 25 tonnes/lot",
        },
        "warehouse_locations": [
            "Rotterdam", "Antwerp", "Singapore", "Busan",
            "Baltimore", "New Orleans", "Port Klang",
        ],
    },
    "COMEX": {
        "name": "COMEX (CME Group)",
        "city": "New York", "country": "USA",
        "lat": 40.7128, "lon": -74.0060,
        "url": "https://www.cmegroup.com",
        "commodities": ["gold", "silver", "copper", "platinum", "palladium"],
        "currency": "USD",
        "importance": "Primary price discovery for precious metals and US copper benchmark.",
        "trading_hours_utc": "23:00-22:00 (nearly 24h)",
        "key_contracts": {
            "gold": "GC — 100 troy oz",
            "silver": "SI — 5,000 troy oz",
            "copper": "HG — 25,000 lbs",
            "platinum": "PL — 50 troy oz",
        },
    },
    "NYMEX": {
        "name": "NYMEX (CME Group)",
        "city": "New York", "country": "USA",
        "lat": 40.7128, "lon": -74.0060,
        "url": "https://www.cmegroup.com",
        "commodities": ["oil_wti", "natural_gas", "heating_oil", "gasoline", "palladium"],
        "currency": "USD",
        "importance": "Global benchmark for WTI crude oil and North American energy.",
        "key_contracts": {
            "oil_wti": "CL — 1,000 barrels",
            "natural_gas": "NG — 10,000 MMBtu",
        },
    },
    "ICE": {
        "name": "ICE Futures (Intercontinental Exchange)",
        "city": "London / New York", "country": "UK/USA",
        "lat": 51.5074, "lon": -0.1278,
        "url": "https://www.theice.com",
        "commodities": ["brent", "coffee", "cocoa", "cotton", "sugar", "natural_gas_eu"],
        "currency": "USD",
        "importance": "Brent crude benchmark. Primary exchange for soft commodities.",
        "key_contracts": {
            "brent": "B — 1,000 barrels (global oil benchmark)",
            "coffee": "KC — 37,500 lbs Arabica",
            "cocoa": "CC — 10 metric tonnes",
            "cotton": "CT — 50,000 lbs",
            "sugar": "SB — 112,000 lbs",
        },
    },
    "SHFE": {
        "name": "Shanghai Futures Exchange",
        "city": "Shanghai", "country": "China",
        "lat": 31.2304, "lon": 121.4737,
        "url": "https://www.shfe.com.cn",
        "commodities": ["copper", "aluminum", "zinc", "tin", "nickel", "gold", "silver", "rubber", "fuel_oil"],
        "currency": "CNY",
        "importance": "China's primary metals exchange. Critical for Asian price discovery. SHFE copper premium/discount to LME = Chinese demand signal.",
        "key_insight": "SHFE-LME spread widening = Chinese demand surge. Narrowing = demand cooling.",
    },
    "DCE": {
        "name": "Dalian Commodity Exchange",
        "city": "Dalian", "country": "China",
        "lat": 38.9140, "lon": 121.6147,
        "url": "http://www.dce.com.cn",
        "commodities": ["iron_ore", "palm_oil", "coking_coal", "coke", "soybean", "corn"],
        "currency": "CNY",
        "importance": "World's largest iron ore futures market. Key signal for Chinese construction/infrastructure demand.",
    },
    "SGX": {
        "name": "Singapore Exchange",
        "city": "Singapore", "country": "Singapore",
        "lat": 1.2789, "lon": 103.8536,
        "url": "https://www.sgx.com",
        "commodities": ["iron_ore", "rubber", "freight"],
        "currency": "USD",
        "importance": "Asian commodity derivatives hub. SGX iron ore futures = tradeable Asian benchmark.",
    },
    "TSX": {
        "name": "Toronto Stock Exchange",
        "city": "Toronto", "country": "Canada",
        "lat": 43.6510, "lon": -79.3470,
        "url": "https://www.tsx.com",
        "commodities": ["mining_equities", "gold_miners", "copper_miners", "uranium", "lithium"],
        "currency": "CAD",
        "importance": "World's largest exchange for mining equities. Home to ~40% of global publicly listed mining companies. Stock performance = forward-looking commodity demand signal.",
        "key_miners": [
            "Barrick Gold (ABX)", "Teck Resources (TECK.B)", "First Quantum (FM)",
            "Lundin Mining (LUN)", "Ivanhoe Mines (IVN)", "Cameco (CCO) — uranium",
            "Lithium Americas (LAC)", "Hudbay Minerals (HBM)",
        ],
    },
    "BVL": {
        "name": "Bolsa de Valores de Lima",
        "city": "Lima", "country": "Peru",
        "lat": -12.0464, "lon": -77.0428,
        "url": "https://www.bvl.com.pe",
        "commodities": ["copper_miners", "gold_miners", "zinc_miners", "silver_miners"],
        "currency": "PEN",
        "importance": "Peru = world's #2 copper producer, #2 silver, #2 zinc. BVL mining stocks = Peru production sentiment.",
        "key_companies": [
            "Southern Copper (SCCO)", "Buenaventura (BVN)",
            "Cerro Verde (CVERDEC1)", "Volcan Compañía Minera",
        ],
    },
    "SANTIAGO": {
        "name": "Bolsa de Comercio de Santiago",
        "city": "Santiago", "country": "Chile",
        "lat": -33.4489, "lon": -70.6693,
        "url": "https://www.bolsadesantiago.com",
        "commodities": ["copper", "lithium", "molybdenum"],
        "currency": "CLP",
        "importance": "Chile = world's #1 copper producer (~27% global). CLP/copper correlation is one of the strongest in commodities. SQM (lithium) listed here.",
        "key_companies": [
            "SQM (lithium, iodine)", "CAP S.A. (iron/steel)",
            "Antofagasta Minerals (copper)", "ENAMI (state copper)",
        ],
    },
    "CBOT": {
        "name": "Chicago Board of Trade (CME Group)",
        "city": "Chicago", "country": "USA",
        "lat": 41.8781, "lon": -87.6298,
        "url": "https://www.cmegroup.com",
        "commodities": ["wheat", "corn", "soybeans", "rice", "oats"],
        "currency": "USD",
        "importance": "Global benchmark for grains and agricultural commodities.",
    },
    "MCX": {
        "name": "Multi Commodity Exchange of India",
        "city": "Mumbai", "country": "India",
        "lat": 19.0760, "lon": 72.8777,
        "url": "https://www.mcxindia.com",
        "commodities": ["gold", "silver", "copper", "crude_oil", "natural_gas", "cotton"],
        "currency": "INR",
        "importance": "India = world's #2 consumer of gold. MCX gold premium = Indian demand signal.",
    },
}


# ═══════════════════════════════════════════════════════════════
# MAJOR COMMODITY TRADERS
# The companies that physically move and finance commodities
# ═══════════════════════════════════════════════════════════════

MAJOR_TRADERS = {
    # ── TOP-TIER (ABCD + Majors) ────────────────────────────
    "GLENCORE": {
        "name": "Glencore plc",
        "hq": "Baar, Switzerland",
        "lat": 47.1958, "lon": 8.5270,
        "revenue_usd_b": 217,  # 2023 approx
        "type": "integrated_trader_miner",
        "commodities": ["copper", "zinc", "nickel", "cobalt", "coal", "oil", "grain", "aluminum"],
        "key_assets": [
            "Mutanda mine (DRC, cobalt/copper)", "Katanga (DRC, copper/cobalt)",
            "Mount Isa (Australia, copper/zinc)", "Collahuasi (Chile, copper 44% stake)",
            "Cerrejón (Colombia, coal 33%)", "Prodeco (Colombia, coal)",
        ],
        "competitive_edge": "World's largest commodity trader by revenue. Owns mines, smelters, AND trading book. Unmatched physical + financial integration.",
        "trading_offices": ["Baar", "London", "Singapore", "Houston", "Beijing", "Sydney"],
    },
    "TRAFIGURA": {
        "name": "Trafigura Group",
        "hq": "Singapore / Geneva",
        "lat": 1.2789, "lon": 103.8536,
        "revenue_usd_b": 244,  # 2022
        "type": "physical_trader",
        "commodities": ["oil", "refined_products", "copper", "zinc", "lead", "iron_ore", "coal", "alumina"],
        "key_assets": [
            "Nyrstar (zinc/lead smelting)", "Impala Terminals (port infrastructure)",
            "Porto Sudeste (Brazil, iron ore port)", "MATSA Mining (Spain, copper/zinc)",
            "Puma Energy (downstream fuels)",
        ],
        "competitive_edge": "Second largest independent oil trader. Massive logistics network via Impala Terminals. Controls port infrastructure in strategic locations.",
        "trading_offices": ["Singapore", "Geneva", "Houston", "Mumbai", "Johannesburg", "Shanghai"],
    },
    "VITOL": {
        "name": "Vitol Group",
        "hq": "Geneva, Switzerland",
        "lat": 46.2044, "lon": 6.1432,
        "revenue_usd_b": 505,  # 2022 — largest private company by revenue
        "type": "physical_trader",
        "commodities": ["oil", "natural_gas", "LNG", "refined_products", "power", "carbon"],
        "competitive_edge": "World's largest independent energy trader. Trades ~8 million barrels of oil per day (~8% of global demand). Massive storage and shipping fleet.",
        "trading_offices": ["Geneva", "Houston", "Singapore", "London", "Dubai"],
    },
    "CARGILL": {
        "name": "Cargill Inc.",
        "hq": "Minneapolis, USA",
        "lat": 44.9778, "lon": -93.2650,
        "revenue_usd_b": 177,
        "type": "agri_trader",
        "commodities": ["grain", "soybeans", "corn", "wheat", "palm_oil", "sugar", "cocoa", "cotton", "beef"],
        "competitive_edge": "World's largest private company by revenue in most years. Dominant in agricultural supply chains from farm to table. Major presence in Brazil, Argentina, Indonesia.",
        "trading_offices": ["Minneapolis", "Geneva", "São Paulo", "Singapore", "Shanghai"],
    },
    "ADM": {
        "name": "Archer Daniels Midland",
        "hq": "Chicago, USA",
        "lat": 41.8781, "lon": -87.6298,
        "revenue_usd_b": 93,
        "type": "agri_trader",
        "commodities": ["soybeans", "corn", "wheat", "cocoa", "palm_oil", "ethanol"],
        "competitive_edge": "ABCD group member. Massive grain storage and processing network. Controls key chokepoints in US grain logistics.",
    },
    "BUNGE": {
        "name": "Bunge Global SA",
        "hq": "Chesterfield, USA (formerly São Paulo)",
        "lat": 38.6631, "lon": -90.5771,
        "revenue_usd_b": 59,
        "type": "agri_trader",
        "commodities": ["soybeans", "wheat", "corn", "sugar", "vegetable_oils"],
        "competitive_edge": "ABCD group. Dominant in South American grain origination. Merging with Viterra (Glencore agri unit) — would create #1 global agri trader.",
    },
    "LOUIS_DREYFUS": {
        "name": "Louis Dreyfus Company",
        "hq": "Rotterdam, Netherlands",
        "lat": 51.9244, "lon": 4.4777,
        "revenue_usd_b": 60,
        "type": "agri_trader",
        "commodities": ["coffee", "cotton", "grains", "rice", "sugar", "soybeans", "juice"],
        "competitive_edge": "ABCD group. Largest cotton merchant globally. Strong in coffee and Brazilian sugar.",
    },
    "MERCURIA": {
        "name": "Mercuria Energy Group",
        "hq": "Geneva, Switzerland",
        "lat": 46.2044, "lon": 6.1432,
        "revenue_usd_b": 135,
        "type": "physical_trader",
        "commodities": ["oil", "natural_gas", "metals", "coal", "power", "carbon"],
        "competitive_edge": "Top-5 energy trader. Strong in metals trading. Growing in renewables and carbon credits.",
    },
    "GUNVOR": {
        "name": "Gunvor Group",
        "hq": "Geneva, Switzerland",
        "lat": 46.2044, "lon": 6.1432,
        "revenue_usd_b": 82,
        "type": "physical_trader",
        "commodities": ["oil", "refined_products", "natural_gas", "LNG", "coal", "carbon"],
        "competitive_edge": "Strong Russian/CIS origin crude trading (historically). Owns refineries in Europe.",
    },
    "OPEN_MINERAL": {
        "name": "Open Mineral AG",
        "hq": "Zug, Switzerland",
        "lat": 47.1724, "lon": 8.5173,
        "revenue_usd_b": 1.5,
        "type": "digital_trader",
        "commodities": ["copper", "zinc", "lead", "nickel", "tin", "iron_ore", "manganese"],
        "competitive_edge": "Digital-first commodity trading platform. AI-powered matching of buyers/sellers. Disrupting traditional trading model by reducing intermediary costs. Fast-growing startup challenging incumbents.",
        "key_innovation": "Technology platform that digitizes physical commodity trading — connects miners directly to smelters/refiners.",
    },
    "KOCH_MINERALS": {
        "name": "Koch Minerals & Trading",
        "hq": "Wichita, USA",
        "lat": 37.6872, "lon": -97.3301,
        "revenue_usd_b": 40,
        "type": "diversified_trader",
        "commodities": ["oil", "natural_gas", "metals", "fertilizer", "sulfur"],
        "competitive_edge": "Part of Koch Industries. Integrated across refining, pipelines, and trading. Strong in US energy infrastructure.",
    },
    "CASTLETON": {
        "name": "Castleton Commodities International",
        "hq": "Stamford, USA",
        "lat": 41.0534, "lon": -73.5387,
        "revenue_usd_b": 35,
        "type": "physical_trader",
        "commodities": ["oil", "natural_gas", "metals", "power"],
        "competitive_edge": "Boutique but aggressive. Known for sophisticated analytics and quant-driven trading.",
    },
    "IXM": {
        "name": "IXM (CMOC Group)",
        "hq": "Geneva, Switzerland",
        "lat": 46.2044, "lon": 6.1432,
        "revenue_usd_b": 15,
        "type": "metals_trader",
        "commodities": ["copper", "zinc", "lead", "nickel", "cobalt", "tungsten", "molybdenum"],
        "competitive_edge": "Chinese-owned metals trader (CMOC Group). Direct access to Chinese demand. Strong in African copper/cobalt through parent's mines.",
    },
}


# ═══════════════════════════════════════════════════════════════
# MAJOR MINES — Global production sources
# ═══════════════════════════════════════════════════════════════

MAJOR_MINES = {
    # ── COPPER ──────────────────────────────────────────────
    "ESCONDIDA": {
        "name": "Escondida",
        "commodity": "copper",
        "country": "Chile", "region": "Atacama Desert",
        "lat": -24.2667, "lon": -69.0667,
        "owner": "BHP (57.5%), Rio Tinto (30%), JECO (12.5%)",
        "production_ktpa": 1200,  # thousand tonnes per annum
        "pct_global": 5.0,
        "status": "active",
        "key_fact": "World's largest copper mine. Any disruption here moves global copper prices.",
    },
    "COLLAHUASI": {
        "name": "Collahuasi",
        "commodity": "copper",
        "country": "Chile", "region": "Tarapacá",
        "lat": -20.9833, "lon": -68.7167,
        "owner": "Anglo American (44%), Glencore (44%), Mitsui (12%)",
        "production_ktpa": 600,
        "pct_global": 2.5,
        "status": "active",
    },
    "CERRO_VERDE": {
        "name": "Cerro Verde",
        "commodity": "copper",
        "country": "Peru", "region": "Arequipa",
        "lat": -16.5369, "lon": -71.5975,
        "owner": "Freeport-McMoRan (53.6%), SMM (21%), Buenaventura (19.6%)",
        "production_ktpa": 500,
        "pct_global": 2.0,
        "status": "active",
    },
    "ANTAMINA": {
        "name": "Antamina",
        "commodity": "copper",
        "country": "Peru", "region": "Ancash",
        "lat": -9.5333, "lon": -77.0500,
        "owner": "BHP (33.75%), Glencore (33.75%), Teck (22.5%), Mitsubishi (10%)",
        "production_ktpa": 450,
        "pct_global": 1.8,
        "status": "active",
        "key_fact": "Also major zinc producer. Located at 4,300m altitude.",
    },
    "LAS_BAMBAS": {
        "name": "Las Bambas",
        "commodity": "copper",
        "country": "Peru", "region": "Apurímac",
        "lat": -14.0544, "lon": -72.3281,
        "owner": "MMG Limited (62.5%, Chinese-owned)",
        "production_ktpa": 300,
        "pct_global": 1.2,
        "status": "active",
        "key_fact": "Frequent community protests block transport route. Social license risk = supply disruption signal.",
    },
    "KAMOA_KAKULA": {
        "name": "Kamoa-Kakula",
        "commodity": "copper",
        "country": "DRC", "region": "Lualaba Province",
        "lat": -10.7725, "lon": 26.1072,
        "owner": "Ivanhoe Mines (39.6%), Zijin Mining (39.6%), Crystal River (0.8%), DRC govt (20%)",
        "production_ktpa": 400,
        "pct_global": 1.6,
        "status": "expanding",
        "key_fact": "Fastest-growing major copper mine. Will become top-3 globally by 2026. Chinese + Canadian ownership = geopolitical complexity.",
    },
    "CHUQUICAMATA": {
        "name": "Chuquicamata",
        "commodity": "copper",
        "country": "Chile", "region": "Antofagasta",
        "lat": -22.3167, "lon": -68.9333,
        "owner": "Codelco (100% state-owned)",
        "production_ktpa": 320,
        "pct_global": 1.3,
        "status": "active",
        "key_fact": "One of oldest and deepest open-pit mines. Transitioning to underground. Codelco = Chilean state copper company.",
    },
    "EL_TENIENTE": {
        "name": "El Teniente",
        "commodity": "copper",
        "country": "Chile", "region": "O'Higgins",
        "lat": -34.0900, "lon": -70.3800,
        "owner": "Codelco (100%)",
        "production_ktpa": 450,
        "pct_global": 1.8,
        "status": "active",
        "key_fact": "World's largest underground copper mine.",
    },
    # ── GOLD ────────────────────────────────────────────────
    "MURUNTAU": {
        "name": "Muruntau",
        "commodity": "gold",
        "country": "Uzbekistan", "region": "Navoi",
        "lat": 41.5100, "lon": 64.5700,
        "owner": "Navoi Mining & Metallurgy (state-owned)",
        "production_ktpa": 0.066,  # ~66 tonnes/year = 2.1M oz
        "pct_global": 1.8,
        "status": "active",
        "key_fact": "World's largest gold mine by production. Secretive, state-controlled. Output not traded on open market.",
    },
    "NEVADA_GOLD": {
        "name": "Nevada Gold Mines (JV)",
        "commodity": "gold",
        "country": "USA", "region": "Nevada",
        "lat": 40.8568, "lon": -117.5016,
        "owner": "Barrick Gold (61.5%), Newmont (38.5%)",
        "production_ktpa": 0.097,
        "pct_global": 2.6,
        "status": "active",
        "key_fact": "Largest gold mining complex in the world by reserves. Multiple mines consolidated.",
    },
    "GRASBERG": {
        "name": "Grasberg",
        "commodity": "gold",
        "country": "Indonesia", "region": "Papua",
        "lat": -4.0553, "lon": 137.1167,
        "owner": "PT Freeport Indonesia (51.2% Indonesian govt via MIND ID), Freeport-McMoRan (48.8%)",
        "production_ktpa": 0.028,
        "pct_global": 0.8,
        "status": "active",
        "key_fact": "Also one of world's largest copper mines. Located at 4,200m in remote Papua. Geopolitical sensitivity (Indonesia nationalization).",
    },
    # ── LITHIUM ─────────────────────────────────────────────
    "SALAR_ATACAMA": {
        "name": "Salar de Atacama",
        "commodity": "lithium",
        "country": "Chile", "region": "Antofagasta",
        "lat": -23.5000, "lon": -68.3000,
        "owner": "SQM (50% of salar), Albemarle (other half)",
        "production_ktpa": 180,  # LCE
        "pct_global": 25.0,
        "status": "active",
        "key_fact": "World's highest-grade lithium brine source. Chile nationalization risk (2023 announced state control). Critical for EV supply chain.",
    },
    "SALAR_UYUNI": {
        "name": "Salar de Uyuni",
        "commodity": "lithium",
        "country": "Bolivia", "region": "Potosí",
        "lat": -20.1338, "lon": -67.4891,
        "owner": "Yacimientos de Litio Bolivianos (state) + CATL JV",
        "production_ktpa": 15,
        "pct_global": 2.0,
        "status": "developing",
        "key_fact": "World's largest lithium reserve but barely developed. CATL (Chinese EV battery giant) JV signals China securing supply.",
    },
    "GREENBUSHES": {
        "name": "Greenbushes",
        "commodity": "lithium",
        "country": "Australia", "region": "Western Australia",
        "lat": -33.8600, "lon": 116.0600,
        "owner": "Tianqi Lithium (51%), IGO (49%)",
        "production_ktpa": 160,
        "pct_global": 22.0,
        "status": "active",
        "key_fact": "World's largest hard-rock lithium mine. Chinese-controlled (Tianqi). Australia = #1 lithium producer by volume.",
    },
    # ── IRON ORE ────────────────────────────────────────────
    "CARAJAS": {
        "name": "Carajás (S11D)",
        "commodity": "iron_ore",
        "country": "Brazil", "region": "Pará",
        "lat": -6.0773, "lon": -50.1611,
        "owner": "Vale S.A. (100%)",
        "production_ktpa": 90000,  # 90 Mt/year
        "pct_global": 3.5,
        "status": "active",
        "key_fact": "World's largest iron ore mine. Highest grade (65%+ Fe). Vale = Brazilian mining giant. Brumadinho dam disaster (2019) still affects reputation.",
    },
    "PILBARA": {
        "name": "Pilbara Iron Ore Operations",
        "commodity": "iron_ore",
        "country": "Australia", "region": "Western Australia",
        "lat": -22.0000, "lon": 118.5000,
        "owner": "BHP, Rio Tinto, Fortescue Metals (multiple mines)",
        "production_ktpa": 800000,  # ~800 Mt combined
        "pct_global": 35.0,
        "status": "active",
        "key_fact": "Australia's Pilbara = single largest iron ore producing region globally. Ships primarily to China. Port Hedland throughput = China demand gauge.",
    },
    # ── COBALT ──────────────────────────────────────────────
    "MUTANDA": {
        "name": "Mutanda",
        "commodity": "cobalt",
        "country": "DRC", "region": "Lualaba",
        "lat": -10.8000, "lon": 25.9000,
        "owner": "Glencore (100%)",
        "production_ktpa": 25,
        "pct_global": 14.0,
        "status": "care_and_maintenance",
        "key_fact": "Was world's largest cobalt mine. Glencore suspended operations 2019, partial restart. DRC = 70% of global cobalt.",
    },
    # ── COAL ────────────────────────────────────────────────
    "CERREJON": {
        "name": "Cerrejón",
        "commodity": "coal",
        "country": "Colombia", "region": "La Guajira",
        "lat": 11.0833, "lon": -72.6667,
        "owner": "Glencore (100%, acquired from BHP/Anglo American)",
        "production_ktpa": 23000,
        "pct_global": 0.3,
        "status": "active",
        "key_fact": "Largest open-pit coal mine in Latin America. Has own railroad and port (Puerto Bolívar). Key Colombian export asset.",
    },
    # ── COFFEE ──────────────────────────────────────────────
    "COLOMBIAN_COFFEE_AXIS": {
        "name": "Colombian Coffee Axis (Eje Cafetero)",
        "commodity": "coffee",
        "country": "Colombia", "region": "Caldas/Quindío/Risaralda",
        "lat": 4.8133, "lon": -75.6961,
        "owner": "Federación Nacional de Cafeteros (cooperative of 540K+ farmers)",
        "production_ktpa": 850,  # ~14M bags
        "pct_global": 8.0,
        "status": "active",
        "key_fact": "Colombia = #3 Arabica producer. Premium washed Arabica commands highest prices. Climate change is pushing production to higher altitudes.",
    },
}


# ═══════════════════════════════════════════════════════════════
# MAJOR REFINERIES & SMELTERS
# Where raw materials become tradeable products
# ═══════════════════════════════════════════════════════════════

REFINERIES_SMELTERS = {
    # ── COPPER SMELTERS/REFINERIES ──────────────────────────
    "GUIXI": {
        "name": "Guixi Smelter (Jiangxi Copper)",
        "type": "copper_smelter",
        "commodity": "copper",
        "country": "China", "region": "Jiangxi",
        "lat": 28.2833, "lon": 117.2167,
        "owner": "Jiangxi Copper Co.",
        "capacity_ktpa": 1000,
        "pct_global_refining": 4.0,
        "key_fact": "World's largest copper smelter. Chinese copper smelter TC/RC negotiations set annual terms for global concentrate market.",
    },
    "BIRLA_COPPER": {
        "name": "Birla Copper (Hindalco)",
        "type": "copper_smelter",
        "commodity": "copper",
        "country": "India", "region": "Gujarat",
        "lat": 21.7051, "lon": 72.1019,
        "owner": "Hindalco Industries (Aditya Birla Group)",
        "capacity_ktpa": 500,
        "pct_global_refining": 2.0,
    },
    "HAMBURG_AURUBIS": {
        "name": "Aurubis Hamburg",
        "type": "copper_refinery",
        "commodity": "copper",
        "country": "Germany", "region": "Hamburg",
        "lat": 53.5188, "lon": 9.9961,
        "owner": "Aurubis AG",
        "capacity_ktpa": 450,
        "pct_global_refining": 1.8,
        "key_fact": "Europe's largest copper refiner. Benchmark for European copper premium.",
    },
    "CHUQUI_SMELTER": {
        "name": "Chuquicamata Smelter",
        "type": "copper_smelter",
        "commodity": "copper",
        "country": "Chile", "region": "Antofagasta",
        "lat": -22.3167, "lon": -68.9333,
        "owner": "Codelco (100%)",
        "capacity_ktpa": 600,
        "pct_global_refining": 2.4,
        "key_fact": "Codelco's integrated smelter. Chile processes ~30% of its copper domestically.",
    },
    "FREEPORT_SMELTER": {
        "name": "Manyar Smelter (Freeport Indonesia)",
        "type": "copper_smelter",
        "commodity": "copper",
        "country": "Indonesia", "region": "East Java",
        "lat": -7.1156, "lon": 112.6500,
        "owner": "PT Freeport Indonesia",
        "capacity_ktpa": 300,
        "pct_global_refining": 1.2,
        "key_fact": "New smelter built to comply with Indonesia's ore export ban. Signals resource nationalism trend.",
    },
    # ── ZINC SMELTERS ───────────────────────────────────────
    "NYRSTAR_BALEN": {
        "name": "Nyrstar Balen",
        "type": "zinc_smelter",
        "commodity": "zinc",
        "country": "Belgium", "region": "Antwerp",
        "lat": 51.1750, "lon": 5.1678,
        "owner": "Trafigura (via Nyrstar acquisition)",
        "capacity_ktpa": 280,
        "pct_global_refining": 2.0,
        "key_fact": "Trafigura acquired Nyrstar for vertical integration. European zinc supply depends on energy costs — electricity price = margin driver.",
    },
    # ── ALUMINUM SMELTERS ───────────────────────────────────
    "RUSAL_KRASNOYARSK": {
        "name": "Krasnoyarsk Aluminium Smelter (KrAZ)",
        "type": "aluminum_smelter",
        "commodity": "aluminum",
        "country": "Russia", "region": "Krasnoyarsk",
        "lat": 56.0153, "lon": 92.8932,
        "owner": "Rusal (EN+ Group)",
        "capacity_ktpa": 1000,
        "pct_global_refining": 1.5,
        "key_fact": "One of world's largest aluminum smelters. Rusal = #2 global aluminum producer. Sanctions risk = supply disruption.",
    },
    "ALCOA_PINJARRA": {
        "name": "Pinjarra Refinery (Alcoa)",
        "type": "alumina_refinery",
        "commodity": "aluminum",
        "country": "Australia", "region": "Western Australia",
        "lat": -32.6306, "lon": 115.8706,
        "owner": "Alcoa Corporation",
        "capacity_ktpa": 4200,  # alumina
        "pct_global_refining": 3.0,
        "key_fact": "World's largest alumina refinery. Bauxite → alumina → aluminum supply chain starts here.",
    },
    # ── OIL REFINERIES ──────────────────────────────────────
    "JAMNAGAR": {
        "name": "Jamnagar Refinery",
        "type": "oil_refinery",
        "commodity": "oil",
        "country": "India", "region": "Gujarat",
        "lat": 22.2886, "lon": 69.0746,
        "owner": "Reliance Industries",
        "capacity_bpd": 1400000,
        "key_fact": "World's largest oil refinery complex. India buying Russian crude at discount → Jamnagar refining it → exporting refined products to Europe. This arbitrage = massive geopolitical trade signal.",
    },
    "ROTTERDAM_SHELL": {
        "name": "Shell Pernis Refinery",
        "type": "oil_refinery",
        "commodity": "oil",
        "country": "Netherlands", "region": "Rotterdam",
        "lat": 51.8844, "lon": 4.3868,
        "owner": "Shell plc",
        "capacity_bpd": 404000,
        "key_fact": "Europe's largest oil refinery. Rotterdam = Europe's oil trading hub. ARA (Amsterdam-Rotterdam-Antwerp) stocks = European oil demand gauge.",
    },
}


# ═══════════════════════════════════════════════════════════════
# SUPPLY CHAIN RELATIONSHIPS
# Who supplies whom — the competitive intelligence layer
# ═══════════════════════════════════════════════════════════════

SUPPLY_CHAINS = {
    "copper": {
        "flow": "Mine → Concentrate → Smelter → Refined Cathode → Fabricator → End User",
        "key_metrics": {
            "TC/RC": "Treatment Charge / Refining Charge — paid by miners to smelters. Low TC/RC = tight concentrate market = bullish for copper miners. High TC/RC = oversupply of concentrate.",
            "LME_premium": "Physical premium over LME price. Rising = tight physical market. Falling = ample supply.",
            "SHFE_LME_arb": "SHFE/LME price ratio. >8.0 = profitable to import to China = strong demand. <7.5 = weak Chinese demand.",
        },
        "top_producers_countries": [
            {"country": "Chile", "pct": 27, "key_mines": ["Escondida", "Collahuasi", "El Teniente", "Chuquicamata"]},
            {"country": "Peru", "pct": 10, "key_mines": ["Cerro Verde", "Antamina", "Las Bambas"]},
            {"country": "DRC", "pct": 10, "key_mines": ["Kamoa-Kakula", "Mutanda", "Tenke Fungurume"]},
            {"country": "China", "pct": 8, "key_mines": ["Dexing", "Zijinshan"]},
            {"country": "USA", "pct": 6, "key_mines": ["Morenci", "Bingham Canyon"]},
        ],
        "top_consumers": [
            {"entity": "China", "pct": 54, "use": "Construction, electronics, EV, grid"},
            {"entity": "Europe", "pct": 15, "use": "Automotive, construction, cable"},
            {"entity": "USA", "pct": 8, "use": "Construction, electronics, defense"},
            {"entity": "Japan", "pct": 4, "use": "Electronics, automotive"},
        ],
        "major_traders": ["Glencore", "Trafigura", "IXM/CMOC", "Open Mineral", "Freeport-McMoRan (own trading)"],
    },
    "gold": {
        "flow": "Mine → Doré bars → Refinery → Bars/Coins → Central Banks / Jewelers / ETFs",
        "key_metrics": {
            "LBMA_fix": "London Bullion Market Association gold price fix — THE benchmark. Set twice daily.",
            "COMEX_OI": "COMEX Open Interest — high OI = active speculation. OI drops + price rises = short squeeze.",
            "ETF_flows": "GLD/IAU ETF inflows = institutional demand. Outflows = risk appetite returning.",
            "central_bank_buying": "PBOC, RBI, Turkey buying gold = de-dollarization signal. #1 demand driver since 2022.",
        },
        "top_producers_countries": [
            {"country": "China", "pct": 10, "note": "Produces but barely exports"},
            {"country": "Australia", "pct": 9},
            {"country": "Russia", "pct": 9, "note": "Sanctions limit Western market access"},
            {"country": "Canada", "pct": 6},
            {"country": "USA", "pct": 6},
            {"country": "Ghana", "pct": 4},
            {"country": "Peru", "pct": 4},
        ],
        "top_refiners": [
            "Valcambi (Switzerland)", "PAMP (Switzerland)", "Argor-Heraeus (Switzerland)",
            "Metalor (Switzerland)", "Perth Mint (Australia)", "Royal Canadian Mint",
        ],
        "major_traders": ["HSBC", "JP Morgan", "UBS", "Standard Chartered", "Glencore"],
    },
    "oil": {
        "flow": "Wellhead → Pipeline/Tanker → Refinery → Products → Distribution → End User",
        "key_metrics": {
            "Brent_WTI_spread": "Brent - WTI spread. Widening = Atlantic Basin tight. Narrowing = US oversupply.",
            "crack_spread": "Refining margin = product price - crude price. High = strong demand for products.",
            "contango_backwardation": "Contango = oversupply, storage play. Backwardation = immediate demand, tight market.",
            "OPEC_spare_capacity": "OPEC spare capacity <2 Mbpd = geopolitical risk premium. >4 Mbpd = buffer exists.",
            "floating_storage": "Oil on tankers not moving. Rising = demand weak. Falling = market tightening.",
        },
        "top_producers_countries": [
            {"country": "USA", "pct": 20, "note": "Shale revolution — swing producer"},
            {"country": "Saudi Arabia", "pct": 12, "note": "OPEC leader, de facto price setter"},
            {"country": "Russia", "pct": 12, "note": "Sanctions redirect to China/India"},
            {"country": "Canada", "pct": 6, "note": "Oil sands"},
            {"country": "Iraq", "pct": 5},
        ],
        "major_traders": ["Vitol", "Trafigura", "Gunvor", "Mercuria", "Glencore", "Koch"],
    },
    "lithium": {
        "flow": "Brine/Spodumene → Lithium Carbonate/Hydroxide → Cathode → Battery Cell → EV/Storage",
        "key_metrics": {
            "spodumene_price": "Australian spodumene spot price = upstream signal. Lagged 3-6 months to carbonate.",
            "carbonate_hydroxide_spread": "LCE vs LiOH spread. LiOH premium = NMC battery demand. Converging = LFP dominance (CATL).",
            "china_spot": "Wuxi/SMM China spot price = real-time demand signal. Most liquid market.",
        },
        "top_producers_countries": [
            {"country": "Australia", "pct": 47, "note": "Hard rock spodumene"},
            {"country": "Chile", "pct": 24, "note": "Brine — lowest cost but nationalization risk"},
            {"country": "China", "pct": 15, "note": "Lepidolite + brine processing"},
            {"country": "Argentina", "pct": 6, "note": "Brine — 'lithium triangle' with Chile/Bolivia"},
        ],
        "top_consumers": [
            {"entity": "CATL", "pct": 34, "note": "World's #1 battery maker (Chinese)"},
            {"entity": "BYD", "pct": 16, "note": "#2 battery/EV maker (Chinese)"},
            {"entity": "LG Energy Solution", "pct": 14, "note": "Korean"},
            {"entity": "Panasonic", "pct": 7, "note": "Japanese, supplies Tesla"},
            {"entity": "Samsung SDI", "pct": 5, "note": "Korean"},
        ],
        "major_traders": ["Glencore", "Trafigura", "Traxys", "Mitsui", "Sinomine"],
    },
    "coffee": {
        "flow": "Farm → Wet Mill → Dry Mill → Export → Roaster → Retail",
        "key_metrics": {
            "NYC_C_price": "ICE Arabica futures (KC) — global benchmark. >$2/lb = high. <$1.5/lb = farmer pain.",
            "certified_stocks": "ICE certified stocks = deliverable supply. Declining = tightening. Rising = bearish.",
            "differentials": "Country differential vs NYC. Colombia +$0.30 = premium quality. Brazil -$0.10 = discount.",
            "robusta_spread": "Arabica-Robusta spread widening = quality premium. Narrowing = substitution risk.",
        },
        "top_producers_countries": [
            {"country": "Brazil", "pct": 35, "note": "Cerrado + Minas Gerais. Both Arabica and Robusta."},
            {"country": "Vietnam", "pct": 17, "note": "Robusta dominant. #1 Robusta producer."},
            {"country": "Colombia", "pct": 8, "note": "Washed Arabica premium. FNC cooperative."},
            {"country": "Indonesia", "pct": 7, "note": "Sumatra, Sulawesi. Unique processing."},
            {"country": "Ethiopia", "pct": 4, "note": "Origin of Arabica. Specialty grade."},
        ],
        "major_traders": ["Louis Dreyfus", "ECOM Agroindustrial", "Volcafe (ED&F Man)", "Olam", "Sucafina"],
    },
}


# ═══════════════════════════════════════════════════════════════
# EXCHANGE TICKERS — Enhanced mapping for multi-exchange data
# ═══════════════════════════════════════════════════════════════

EXCHANGE_TICKERS = {
    "copper": {
        "LME": {"proxy_ticker": "HG=F", "note": "COMEX copper correlates ~0.98 with LME"},
        "COMEX": {"ticker": "HG=F", "contract": "25,000 lbs"},
        "SHFE": {"proxy_ticker": "HG=F", "note": "SHFE copper follows LME with CNY adjustment"},
        "miners_TSX": ["FM.TO", "LUN.TO", "HBM.TO", "IVN.TO", "TECK-B.TO"],
        "miners_BVL": ["SCCO", "BVN"],
        "miners_SANTIAGO": ["ANTOFAGASTA.L"],
    },
    "gold": {
        "COMEX": {"ticker": "GC=F", "contract": "100 troy oz"},
        "LBMA": {"proxy_ticker": "GC=F", "note": "LBMA fix tracked via COMEX"},
        "MCX": {"proxy_ticker": "GC=F", "note": "MCX premium over COMEX = Indian demand"},
        "miners_TSX": ["ABX.TO", "K.TO", "AEM.TO", "WPM.TO"],
        "etfs": ["GLD", "IAU", "SGOL"],
    },
    "lithium": {
        "direct": {"proxy_ticker": "LTHM", "note": "No liquid lithium futures yet"},
        "miners_TSX": ["LAC.TO"],
        "miners_ASX": ["PLS.AX", "AKE.AX", "MIN.AX"],
        "miners_SANTIAGO": ["SQM"],
        "processors": ["ALB", "LTHM"],
    },
    "oil": {
        "NYMEX": {"ticker": "CL=F", "contract": "1,000 barrels WTI"},
        "ICE": {"ticker": "BZ=F", "contract": "1,000 barrels Brent"},
        "key_spreads": {
            "Brent_WTI": "BZ=F minus CL=F",
            "crack_321": "3 gasoline + 2 heating oil + 1 crude",
        },
    },
    "iron_ore": {
        "SGX": {"proxy_ticker": "GWM.AX", "note": "SGX iron ore futures via proxy"},
        "DCE": {"proxy_ticker": "GWM.AX", "note": "DCE iron ore most liquid globally"},
        "miners_ASX": ["BHP.AX", "RIO.AX", "FMG.AX"],
        "miners_BOVESPA": ["VALE3.SA"],
    },
    "coal": {
        "ICE_Newcastle": {"ticker": "MTF=F", "contract": "Newcastle thermal coal"},
        "miners_ASX": ["WHC.AX", "NHC.AX"],
    },
    "coffee": {
        "ICE_Arabica": {"ticker": "KC=F", "contract": "37,500 lbs"},
        "ICE_Robusta": {"ticker": "RC=F", "contract": "10 metric tonnes"},
    },
}


# ═══════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS — Used by agents and API
# ═══════════════════════════════════════════════════════════════

def get_commodity_context(commodity: str) -> Dict[str, Any]:
    """
    Build comprehensive industry context for a commodity.
    This is injected into agent prompts to give them institutional knowledge.
    """
    commodity_lower = commodity.lower().replace("_", " ")

    # Find relevant supply chain
    chain = None
    for key, sc in SUPPLY_CHAINS.items():
        if key in commodity_lower or commodity_lower in key:
            chain = sc
            break

    # Find relevant mines
    relevant_mines = []
    for mine_id, mine in MAJOR_MINES.items():
        if mine["commodity"].lower() in commodity_lower or commodity_lower in mine["commodity"].lower():
            relevant_mines.append(mine)

    # Find relevant refineries/smelters
    relevant_plants = []
    for plant_id, plant in REFINERIES_SMELTERS.items():
        if plant["commodity"].lower() in commodity_lower or commodity_lower in plant["commodity"].lower():
            relevant_plants.append(plant)

    # Find relevant exchanges
    relevant_exchanges = []
    for ex_id, ex in EXCHANGES.items():
        for comm in ex["commodities"]:
            if commodity_lower in comm or comm in commodity_lower:
                relevant_exchanges.append({"id": ex_id, **ex})
                break

    # Find relevant traders
    relevant_traders = []
    for trader_id, trader in MAJOR_TRADERS.items():
        for comm in trader["commodities"]:
            if commodity_lower in comm or comm in commodity_lower:
                relevant_traders.append({"id": trader_id, **trader})
                break

    # Exchange tickers
    tickers = EXCHANGE_TICKERS.get(commodity_lower, {})

    return {
        "supply_chain": chain,
        "mines": relevant_mines,
        "refineries_smelters": relevant_plants,
        "exchanges": relevant_exchanges,
        "major_traders": relevant_traders,
        "exchange_tickers": tickers,
    }


def build_agent_context_prompt(commodity: str) -> str:
    """
    Build a concise industry context string to inject into agent prompts.
    Keeps it under 500 tokens to not blow up costs.
    """
    ctx = get_commodity_context(commodity)

    parts = []

    # Supply chain overview
    if ctx["supply_chain"]:
        sc = ctx["supply_chain"]
        parts.append(f"SUPPLY CHAIN: {sc['flow']}")
        if "key_metrics" in sc:
            metrics = "; ".join(f"{k}: {v}" for k, v in list(sc["key_metrics"].items())[:3])
            parts.append(f"KEY METRICS: {metrics}")
        if "top_producers_countries" in sc:
            producers = ", ".join(
                f"{p['country']} ({p['pct']}%)" for p in sc["top_producers_countries"][:5]
            )
            parts.append(f"TOP PRODUCERS: {producers}")
        if "top_consumers" in sc:
            consumers = ", ".join(
                f"{c['entity']} ({c['pct']}%)" for c in sc["top_consumers"][:4]
            )
            parts.append(f"TOP CONSUMERS: {consumers}")

    # Major mines
    if ctx["mines"]:
        mines_str = ", ".join(
            f"{m['name']} ({m['country']}, {m.get('pct_global', '?')}% global, owner: {m['owner'][:30]})"
            for m in ctx["mines"][:5]
        )
        parts.append(f"MAJOR MINES: {mines_str}")

    # Refineries
    if ctx["refineries_smelters"]:
        plants_str = ", ".join(
            f"{p['name']} ({p['country']}, {p.get('capacity_ktpa', '?')} ktpa)"
            for p in ctx["refineries_smelters"][:4]
        )
        parts.append(f"SMELTERS/REFINERIES: {plants_str}")

    # Traders
    if ctx["major_traders"]:
        traders_str = ", ".join(t["name"] for t in ctx["major_traders"][:5])
        parts.append(f"MAJOR TRADERS: {traders_str}")

    # Exchanges
    if ctx["exchanges"]:
        exchanges_str = ", ".join(f"{e['id']} ({e['city']})" for e in ctx["exchanges"][:4])
        parts.append(f"EXCHANGES: {exchanges_str}")

    return "\n".join(parts) if parts else "No specific industry context available for this commodity."


def get_all_exchanges_summary() -> List[Dict[str, Any]]:
    """Return summary of all exchanges for the frontend/API."""
    return [
        {
            "id": ex_id,
            "name": ex["name"],
            "city": ex["city"],
            "country": ex["country"],
            "lat": ex["lat"],
            "lon": ex["lon"],
            "commodities": ex["commodities"],
        }
        for ex_id, ex in EXCHANGES.items()
    ]


def get_all_traders_summary() -> List[Dict[str, Any]]:
    """Return summary of all major traders for the frontend/API."""
    return [
        {
            "id": trader_id,
            "name": t["name"],
            "hq": t["hq"],
            "lat": t["lat"],
            "lon": t["lon"],
            "revenue_usd_b": t["revenue_usd_b"],
            "type": t["type"],
            "commodities": t["commodities"],
        }
        for trader_id, t in MAJOR_TRADERS.items()
    ]


def get_all_mines_summary() -> List[Dict[str, Any]]:
    """Return summary of all mines for the globe visualization."""
    return [
        {
            "id": mine_id,
            "name": m["name"],
            "commodity": m["commodity"],
            "country": m["country"],
            "lat": m["lat"],
            "lon": m["lon"],
            "pct_global": m.get("pct_global", 0),
            "owner": m["owner"],
            "status": m["status"],
        }
        for mine_id, m in MAJOR_MINES.items()
    ]


def get_all_plants_summary() -> List[Dict[str, Any]]:
    """Return summary of all refineries/smelters for the globe."""
    return [
        {
            "id": plant_id,
            "name": p["name"],
            "type": p["type"],
            "commodity": p["commodity"],
            "country": p["country"],
            "lat": p["lat"],
            "lon": p["lon"],
            "owner": p["owner"],
            "capacity_ktpa": p.get("capacity_ktpa", 0),
        }
        for plant_id, p in REFINERIES_SMELTERS.items()
    ]
