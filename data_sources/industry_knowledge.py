#!/usr/bin/env python3
"""
METEORO X v12 — INDUSTRY KNOWLEDGE GRAPH
==========================================
Comprehensive knowledge base of the global commodity industry:
  - Global exchanges and their commodity specializations
  - Major traders (physical + financial)
  - Major mines with geographic coordinates
  - Refineries and smelters with locations
  - Shipping companies (navieras) and maritime fleet
  - Major ports with throughput and bottleneck risks
  - Logistics companies (rail, trucking, pipelines)
  - Inspection & QA companies (the trust layer)
  - End clients / consumers (who buys what)
  - Supply chain relationships (mine → port → ship → smelter → client)

This module gives the agentic system INSTITUTIONAL-GRADE context
that transforms generic price analysis into actionable intelligence.
The complete supply chain — from mine mouth to end consumer — is mapped.

ENTITIES: 12 exchanges | 14 traders | 20+ mines | 10+ smelters
          18 shipping cos | 25 ports | 12 logistics | 10 QA | 19 clients
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
# SHIPPING COMPANIES (NAVIERAS)
# Who moves commodities across oceans — maritime intelligence
# ═══════════════════════════════════════════════════════════════

SHIPPING_COMPANIES = {
    "MAERSK": {
        "name": "A.P. Moller-Maersk A/S",
        "hq": "Copenhagen, Denmark",
        "lat": 55.6761, "lon": 12.5683,
        "type": "container",
        "fleet_size": 730,
        "dwt_million": 15.8,
        "commodities": ["containerized_goods", "general_cargo", "breakbulk", "reefer"],
        "key_routes": ["Asia-Europe", "Trans-Pacific", "Trans-Atlantic", "Intra-Asia"],
        "competitive_edge": "Vertically integrated supply chain control; terminal operations influence container availability and freight rate volatility.",
        "key_fact": "Maersk's alliances control ~40% Asia-Europe capacity; vessel cascading announcements signal 6-month rate shifts.",
    },
    "MSC": {
        "name": "Mediterranean Shipping Company",
        "hq": "Geneva, Switzerland",
        "lat": 46.2044, "lon": 6.1432,
        "type": "container",
        "fleet_size": 750,
        "dwt_million": 16.2,
        "commodities": ["containerized_goods", "general_cargo", "automotive"],
        "key_routes": ["Asia-Europe", "Intra-Asia", "Americas-Europe", "South African routes"],
        "competitive_edge": "Family-owned private structure allows aggressive capacity injections during soft markets; less transparent planning creates pricing opportunities.",
        "key_fact": "MSC mega-ship orders signal sustained demand; port call delays correlate with 3-5 day rate premiums in secondary markets.",
    },
    "CMA_CGM": {
        "name": "CMA CGM Group",
        "hq": "Marseille, France",
        "lat": 43.2965, "lon": 5.3698,
        "type": "container",
        "fleet_size": 550,
        "dwt_million": 12.1,
        "commodities": ["containerized_goods", "general_cargo", "reefer"],
        "key_routes": ["Asia-Europe", "Trans-Pacific", "Intra-Asia", "Far East-Africa"],
        "competitive_edge": "Strong Suez corridor presence; CEVA logistics integration enables premium rates on temperature-controlled agricultural commodities.",
        "key_fact": "Controls ~18% of Africa-Asia flows; contract tonnage often trades 8-12% above spot on South America ore/grain routes.",
    },
    "COSCO_SHIPPING": {
        "name": "China COSCO Shipping Holdings",
        "hq": "Beijing, China",
        "lat": 39.9042, "lon": 116.4074,
        "type": "container",
        "fleet_size": 850,
        "dwt_million": 17.9,
        "commodities": ["containerized_goods", "general_cargo", "bulk_coal", "grain"],
        "key_routes": ["Intra-Asia", "Asia-Europe", "Asia-Americas", "Belt and Road corridors"],
        "competitive_edge": "State-backed capacity discipline enables sub-cost pricing to capture market share; political mandate maintains Chinese port volume.",
        "key_fact": "COSCO rate cuts precede Chinese commodity imports by 2-3 weeks; Shanghai-Rotterdam offerings = leading indicator of Chinese demand.",
    },
    "STAR_BULK": {
        "name": "Star Bulk Carriers Corp.",
        "hq": "Stamford, USA",
        "lat": 41.0534, "lon": -73.5387,
        "type": "dry_bulk",
        "fleet_size": 130,
        "dwt_million": 8.7,
        "commodities": ["iron_ore", "coal", "grain", "minor_bulks", "fertilizer"],
        "key_routes": ["Brazil-China", "Australia-China", "Black Sea-Asia", "Capesize triangles"],
        "competitive_edge": "Efficient cost structure enables profitability at BDI 900-1000 where peers struggle; first to deploy on emerging routes.",
        "key_fact": "Star Bulk employment decisions lead BDI by 3-4 weeks; vessel idling signals capesize spot contraction 5-8% within 2 weeks.",
    },
    "PACIFIC_BASIN": {
        "name": "Pacific Basin Shipping Limited",
        "hq": "Hong Kong",
        "lat": 22.2793, "lon": 114.1633,
        "type": "dry_bulk",
        "fleet_size": 235,
        "dwt_million": 2.1,
        "commodities": ["grain", "minor_bulks", "fertilizer", "phosphate", "bauxite"],
        "key_routes": ["Handysize triangles", "Intra-Asia minor bulk", "South American grain"],
        "competitive_edge": "Handysize specialization captures 60-80% margin premium on niche minor bulk trades where larger vessels are inefficient.",
        "key_fact": "Crew scheduling patterns signal South American grain harvest liftings 4 weeks early; deployment correlates 0.92 with Baltic Handysize Index.",
    },
    "OLDENDORFF": {
        "name": "Oldendorff Carriers GmbH",
        "hq": "Lübeck, Germany",
        "lat": 53.8645, "lon": 10.6863,
        "type": "dry_bulk",
        "fleet_size": 620,
        "dwt_million": 11.3,
        "commodities": ["coal", "grain", "iron_ore", "minor_bulks", "fertilizer"],
        "key_routes": ["Atlantic triangles", "Coal corridors", "Baltic-Black Sea", "North Atlantic"],
        "competitive_edge": "Family-owned 150+ years; long-term contract relationships with Rio Tinto, Vale that younger companies cannot access.",
        "key_fact": "Oldendorff contract rates with Vale signal 3-month capesize sentiment; 12-month contract willingness = strongest rate floor indicator.",
    },
    "FRONTLINE": {
        "name": "Frontline Ltd.",
        "hq": "Oslo, Norway",
        "lat": 59.9139, "lon": 10.7522,
        "type": "tanker",
        "fleet_size": 68,
        "dwt_million": 8.1,
        "commodities": ["crude_oil", "refined_products", "LPG", "chemicals"],
        "key_routes": ["Middle East-Asia", "Atlantic crude", "Refined product triangles", "US Gulf-Europe"],
        "competitive_edge": "Supertanker focus (ULCV) provides 15-20% cost advantage on long-haul crude routes.",
        "key_fact": "Frontline ULCV deployment signals 6-month crude shipping outlook; Fujairah anchoring = rate compression expected.",
    },
    "EURONAV_CMB": {
        "name": "Euronav NV / CMB",
        "hq": "Antwerp, Belgium",
        "lat": 51.3213, "lon": 4.3961,
        "type": "tanker",
        "fleet_size": 72,
        "dwt_million": 8.9,
        "commodities": ["crude_oil", "refined_products", "chemicals", "vegetable_oils"],
        "key_routes": ["Atlantic crude", "Intra-Europe refined", "Asia-Europe chemicals"],
        "competitive_edge": "CMB's industrial chemical integration creates captive backhaul contracts securing 70%+ utilization vs spot at 45-55%.",
        "key_fact": "Crude-to-product tanker ratio signals belief in refined product vs crude spreads 8 weeks forward.",
    },
    "IMPALA_TERMINALS": {
        "name": "Impala Terminals (Trafigura)",
        "hq": "Rotterdam, Netherlands",
        "lat": 51.9225, "lon": 4.2597,
        "type": "port_operator",
        "fleet_size": 12,
        "dwt_million": 2.3,
        "commodities": ["copper", "zinc", "coal", "grain", "oil_products"],
        "key_routes": ["African mineral exports", "Commodity storage arbitrage", "Blending hub"],
        "competitive_edge": "0.8M tonnes storage capacity enables 5-15 day contango/backwardation arbs; owns logistical chokepoints.",
        "key_fact": "Storage utilization >70% signals compressed commodity premiums 2-3 weeks forward. Trafigura's logistics arm.",
    },
    "DP_WORLD": {
        "name": "DP World PLC",
        "hq": "Dubai, UAE",
        "lat": 25.2048, "lon": 55.2708,
        "type": "port_operator",
        "fleet_size": 8,
        "dwt_million": 1.1,
        "commodities": ["containerized_goods", "breakbulk", "general_cargo", "minerals"],
        "key_routes": ["Suez Canal transit hub", "Intra-Asia container", "African export gateway"],
        "competitive_edge": "Jebel Ali port (world's 5th largest) throughput constraints create 2-5 day vessel delays.",
        "key_fact": "Wait-time announcements signal Suez geopolitical risk; dwell times >5 days indicate Red Sea route avoidance.",
    },
    "PSA_INTERNATIONAL": {
        "name": "PSA International (Singapore)",
        "hq": "Singapore",
        "lat": 1.3521, "lon": 103.8198,
        "type": "port_operator",
        "fleet_size": 6,
        "dwt_million": 0.9,
        "commodities": ["containerized_goods", "bunker_fuel", "refined_products", "LNG"],
        "key_routes": ["Malacca Strait chokepoint", "Intra-Asia transshipment", "LNG bunkering"],
        "competitive_edge": "Singapore centrality captures 25% Malacca Strait transshipment; bunker fuel control provides 2-5% price advantages.",
        "key_fact": "PSA throughput >40M TEU validates Asian demand; dwell time increases precede Malacca bunker premium spikes.",
    },
    "VALE_FLEET": {
        "name": "Vale S.A. (VLOC Fleet)",
        "hq": "Rio de Janeiro, Brazil",
        "lat": -22.9068, "lon": -43.1729,
        "type": "dry_bulk",
        "fleet_size": 66,
        "dwt_million": 9.8,
        "commodities": ["iron_ore", "manganese_ore", "nickel_laterite"],
        "key_routes": ["Brazil-China", "Brazil-India", "Brazil-Japan", "Brazil-South Korea"],
        "competitive_edge": "Self-operating fleet reduces cost $2-4/t vs spot market; direct control enables supply timing manipulation.",
        "key_fact": "VLOC deployment to Chinese ports lags ore demand by 18-22 days; berthing patterns = leading indicator of actual Chinese steel mill consumption.",
    },
    "BW_GROUP": {
        "name": "BW Group Limited",
        "hq": "Singapore",
        "lat": 1.3521, "lon": 103.8198,
        "type": "tanker",
        "fleet_size": 180,
        "dwt_million": 11.2,
        "commodities": ["LPG", "LNG", "crude_oil", "refined_products", "petrochemicals"],
        "key_routes": ["Middle East-Asia LPG", "Atlantic LNG", "Intra-Asia products", "US-Europe propane"],
        "competitive_edge": "LPG fleet dominance (80+ vessels) enables 30-40% margin advantage on spot LPG routes.",
        "key_fact": "BW LPG positions at Singapore signal Asian heating demand 2-3 weeks forward; charter-in decisions correlate to US propane export parity.",
    },
    "CHINA_MERCHANTS": {
        "name": "China Merchants Heavy Industry",
        "hq": "Beijing, China",
        "lat": 39.9042, "lon": 116.4074,
        "type": "dry_bulk",
        "fleet_size": 420,
        "dwt_million": 7.4,
        "commodities": ["coal", "iron_ore", "grain", "bauxite", "phosphate"],
        "key_routes": ["Domestic China coal", "Southeast Asia coal", "East Africa-China", "South America grain"],
        "competitive_edge": "State SOE status enables below-cost rate offerings to support domestic coal/grain supply chains.",
        "key_fact": "Deployment to Australian coal ports signals 4-week advance warning of Chinese thermal coal demand shifts.",
    },
    "SITC_INTL": {
        "name": "SITC International Holdings",
        "hq": "Shanghai, China",
        "lat": 31.2304, "lon": 121.4737,
        "type": "container",
        "fleet_size": 95,
        "dwt_million": 1.8,
        "commodities": ["containerized_goods", "general_cargo"],
        "key_routes": ["Intra-Asia feedering", "ASEAN-China short-sea", "Domestic China container"],
        "competitive_edge": "Dominant feeder network captures 40%+ Shanghai-secondary port intra-Asia container volume; 15-20% cost advantage on short routes.",
        "key_fact": "SITC rate discounting on Shanghai-SEA routes leads container overcapacity signals by 10-14 days.",
    },
}


# ═══════════════════════════════════════════════════════════════
# MAJOR PORTS — Strategic commodity chokepoints
# Where supply chains can be disrupted or accelerated
# ═══════════════════════════════════════════════════════════════

MAJOR_PORTS = {
    # ── LATIN AMERICA ─────────────────────────────────────────
    "PUERTO_BOLIVAR": {
        "name": "Puerto Bolívar",
        "country": "Colombia", "region": "La Guajira",
        "lat": 12.1717, "lon": -71.3919,
        "type": "bulk_terminal",
        "operator": "Cerrejón",
        "annual_throughput_mt": 30,
        "commodities": ["thermal_coal"],
        "max_vessel_size": "Capesize",
        "connected_to": ["Cerrejón mine", "Caribbean shipping lanes"],
        "key_fact": "Largest coal exporter in Americas. Port congestion immediately tightens global coal supply and supports European/Asian prices.",
        "bottleneck_risk": "Tropical storms Jun-Nov. La Niña patterns. Labor disputes halt 30M+ tonnes/year.",
    },
    "SANTA_MARTA": {
        "name": "Santa Marta",
        "country": "Colombia", "region": "Magdalena",
        "lat": 11.2432, "lon": -74.2309,
        "type": "bulk_terminal",
        "operator": "Prodeco (Glencore)",
        "annual_throughput_mt": 26,
        "commodities": ["thermal_coal"],
        "max_vessel_size": "Capesize",
        "connected_to": ["Prodeco mines", "Atlantic shipping", "US Gulf routes"],
        "key_fact": "Primary Colombian coal export hub. Disruptions ripple to utility coal contracts in US/Europe within 2-3 weeks.",
        "bottleneck_risk": "Caribbean hurricane season. Political risk with leftist government. Environmental pressures on expansion.",
    },
    "BUENAVENTURA": {
        "name": "Buenaventura",
        "country": "Colombia", "region": "Valle del Cauca",
        "lat": 3.8853, "lon": -77.3160,
        "type": "multi_purpose",
        "operator": "SPB (Sociedad Portuaria Regional)",
        "annual_throughput_mt": 18,
        "commodities": ["coffee", "sugar", "cocoa", "bananas"],
        "max_vessel_size": "Panamax",
        "connected_to": ["Eje Cafetero", "Cauca Valley agriculture", "Pacific Asia routes"],
        "key_fact": "Colombia's only major Pacific port. Coffee exports (~1.3M t/y) set global soft commodity tone. Congestion pushes Colombian coffee forward curves.",
        "bottleneck_risk": "Pacific swell limits loading. Rainy season landslides. Port capacity ceiling ~18-20M t. Rising access fees.",
    },
    "CALLAO": {
        "name": "Callao",
        "country": "Peru", "region": "Lima",
        "lat": -12.0464, "lon": -77.1528,
        "type": "multi_purpose",
        "operator": "DP World / APM Terminals",
        "annual_throughput_mt": 25,
        "commodities": ["copper_concentrates", "zinc_concentrates", "iron_ore", "molybdenum"],
        "max_vessel_size": "Post-Panamax",
        "connected_to": ["Antamina", "Cerro Verde", "Shougang"],
        "key_fact": "Peru's leading concentrate exporter. Copper concentrate timing triggers 6-8 week smelter arrival windows in China.",
        "bottleneck_risk": "El Niño port closures. Political instability. Shallow harbor (14-15m draft). Frequent dock labor disputes.",
    },
    "MATARANI_ILO": {
        "name": "Matarani / Ilo",
        "country": "Peru", "region": "Arequipa/Moquegua",
        "lat": -17.0064, "lon": -71.6126,
        "type": "bulk_terminal",
        "operator": "TPMS / Ilo Port Authority",
        "annual_throughput_mt": 12,
        "commodities": ["copper_concentrates", "copper_cathodes", "molybdenum"],
        "max_vessel_size": "Panamax",
        "connected_to": ["Toquepala mine", "Southern Peru mining cluster"],
        "key_fact": "Southern Peru's backup port for copper. Limited dredging; used when Callao congested.",
        "bottleneck_risk": "Aging infrastructure. 8-10 day turnaround vs 3-4 at Callao. Political activism in Arequipa.",
    },
    "ANTOFAGASTA_PORT": {
        "name": "Antofagasta",
        "country": "Chile", "region": "Antofagasta",
        "lat": -23.6345, "lon": -70.4067,
        "type": "bulk_terminal",
        "operator": "Puerto de Antofagasta",
        "annual_throughput_mt": 20,
        "commodities": ["copper_concentrates", "copper_cathodes", "molybdenum", "lithium_compounds"],
        "max_vessel_size": "Capesize",
        "connected_to": ["Escondida", "Codelco mines", "SQM lithium"],
        "key_fact": "Chile = 28% global copper; Antofagasta handles ~20% of Chilean copper exports. Lithium hydroxide exports emerging (5-8% of throughput).",
        "bottleneck_risk": "Seismic activity (2014 quake = 6-week closure). Environmental permit freezes on expansion. Limited to 2-3 Capesize berths.",
    },
    "SAN_ANTONIO": {
        "name": "San Antonio / Valparaíso",
        "country": "Chile", "region": "Valparaíso",
        "lat": -33.5857, "lon": -71.6096,
        "type": "multi_purpose",
        "operator": "Puerto San Antonio / EPV",
        "annual_throughput_mt": 28,
        "commodities": ["copper_cathodes", "fruit", "wine", "wood_pulp"],
        "max_vessel_size": "Panamax",
        "connected_to": ["Central Chilean mines", "Agricultural Central Valley"],
        "key_fact": "Chile's containerization hub. Fresh fruit exports (3M t/y) require strict schedule integrity. Copper cathode liquidity driven by spot sales timing.",
        "bottleneck_risk": "Southern Pacific storms May-Aug. Shallow harbor. Frequent labor strikes. Reefer container shortages.",
    },
    "SANTOS": {
        "name": "Santos",
        "country": "Brazil", "region": "São Paulo",
        "lat": -23.9608, "lon": -46.3244,
        "type": "multi_purpose",
        "operator": "Multiple (BTP, TCP, Ultrafértil)",
        "annual_throughput_mt": 147,
        "commodities": ["soybeans", "soybean_meal", "sugar", "ethanol", "coffee", "iron_ore"],
        "max_vessel_size": "Post-Panamax",
        "connected_to": ["Mato Grosso soy belt", "Central West Brazil", "São Paulo refining"],
        "key_fact": "World's largest soy exporter (40M t/y) and sugar (25M t/y). Export timing Feb-Jun sets global meal/oil prices. Congestion = 1-2 week backup.",
        "bottleneck_risk": "Summer rains Jan-Mar flood interior routes. Dockworker strikes paralyze 140M+ t/y. Constant dredging needed.",
    },
    "TUBARAO": {
        "name": "Tubarão / Vitória",
        "country": "Brazil", "region": "Espírito Santo",
        "lat": -20.2696, "lon": -40.2803,
        "type": "bulk_terminal",
        "operator": "Vale S.A.",
        "annual_throughput_mt": 115,
        "commodities": ["iron_ore", "pellets", "manganese_ore"],
        "max_vessel_size": "Capesize",
        "connected_to": ["Carajás mine", "Vale mining complex", "Asian steel mills"],
        "key_fact": "Vale's primary iron ore export hub (~100M t/y). Capesize scheduling driven by berth availability. Disruptions take 8-12 weeks to propagate to Asian steel.",
        "bottleneck_risk": "South Atlantic swell Feb-May. Vale labor disputes. Pellet plant outages create 2-3 week export pauses.",
    },
    # ── ASIA ──────────────────────────────────────────────────
    "QINGDAO": {
        "name": "Qingdao",
        "country": "China", "region": "Shandong",
        "lat": 36.0671, "lon": 120.3826,
        "type": "multi_purpose",
        "operator": "Qingdao Port Group",
        "annual_throughput_mt": 740,
        "commodities": ["iron_ore", "coal", "rubber", "crude_oil", "containers"],
        "max_vessel_size": "Capesize",
        "connected_to": ["Northern China steelmakers", "Japan/S.Korea routes"],
        "key_fact": "Critical iron ore inbound hub (90M+ t/y). Iron ore prices set daily based on Qingdao congestion & stockpiles. Winter ice Dec-Feb restricts 10-15% capacity.",
        "bottleneck_risk": "Winter Bohai Bay ice Nov-Mar. Spring Festival shutdown 1-2 weeks. Pollution alerts restrict operations.",
    },
    "SHANGHAI_YANGSHAN": {
        "name": "Shanghai / Yangshan",
        "country": "China", "region": "Shanghai",
        "lat": 30.9176, "lon": 121.8871,
        "type": "multi_purpose",
        "operator": "Shanghai International Port Group (SIPG)",
        "annual_throughput_mt": 840,
        "commodities": ["containers", "iron_ore", "coal", "crude_oil", "chemicals", "grain"],
        "max_vessel_size": "Post-Panamax",
        "connected_to": ["Central China industrial zone", "Suez route", "Pacific routes"],
        "key_fact": "World's largest container port; drives global shipping rate cycles. Iron ore discharge (15-20M t/month) benchmarks Asian prices.",
        "bottleneck_risk": "Typhoon season Jun-Sept. Suez disruptions redirect volume here. 24-48h closures affect $5B+ daily flow.",
    },
    "NINGBO_ZHOUSHAN": {
        "name": "Ningbo-Zhoushan",
        "country": "China", "region": "Zhejiang",
        "lat": 30.0283, "lon": 121.7661,
        "type": "multi_purpose",
        "operator": "Ningbo-Zhoushan Port Group",
        "annual_throughput_mt": 850,
        "commodities": ["iron_ore", "coal", "crude_oil", "containers", "metals"],
        "max_vessel_size": "Capesize",
        "connected_to": ["Eastern China mills", "Pacific shipping lanes"],
        "key_fact": "World's #1 port by tonnage. Floating storage creates price reference for 'port stocks.' Backups signal demand weakness across Chinese industry.",
        "bottleneck_risk": "Typhoons Jul-Sept. Congestion stretches turnaround to 10-15 days. Domestic policy shifts reduce demand unpredictably.",
    },
    "SINGAPORE_PORT": {
        "name": "Singapore",
        "country": "Singapore", "region": "Strait of Malacca",
        "lat": 1.3521, "lon": 103.8198,
        "type": "oil_terminal",
        "operator": "PSA International",
        "annual_throughput_mt": 680,
        "commodities": ["crude_oil", "refined_products", "LNG", "metals", "containers"],
        "max_vessel_size": "VLCC",
        "connected_to": ["Middle East routes", "Asian refineries", "LNG terminals"],
        "key_fact": "World's largest bunkering hub (40-50M t/y fuel oil). Floating storage (12-15M t) drives arbitrage to Asian refineries.",
        "bottleneck_risk": "Malacca Strait piracy/geopolitical risk. Limited storage forces diversion to Fujairah. Tidal restrictions for ULCV.",
    },
    "PORT_HEDLAND": {
        "name": "Port Hedland",
        "country": "Australia", "region": "Western Australia",
        "lat": -22.7197, "lon": 118.5845,
        "type": "bulk_terminal",
        "operator": "BHP Billiton / private operators",
        "annual_throughput_mt": 250,
        "commodities": ["iron_ore", "manganese_ore", "salt"],
        "max_vessel_size": "Capesize",
        "connected_to": ["Pilbara iron ore mines", "BHP operations", "Asian steel mills"],
        "key_fact": "Australia's largest port; 60% iron ore to China (150-170M t/y to Asia). BHP production cuts directly lower global iron ore prices. 12-14 week lead time to smelters.",
        "bottleneck_risk": "Cyclone season Nov-Mar (5-10 day closures). Tidal ranges up to 10m limit simultaneous loading. BHP labor disputes halt exports.",
    },
    # ── EUROPE ────────────────────────────────────────────────
    "ROTTERDAM": {
        "name": "Rotterdam",
        "country": "Netherlands", "region": "South Holland",
        "lat": 51.9225, "lon": 4.2797,
        "type": "multi_purpose",
        "operator": "Port of Rotterdam Authority",
        "annual_throughput_mt": 475,
        "commodities": ["crude_oil", "refined_products", "metals", "agricultural", "containers"],
        "max_vessel_size": "Post-Panamax",
        "connected_to": ["Suez canal", "NW European refineries", "Rhine inland waterways"],
        "key_fact": "Europe's largest port. Crude oil import hub (200M+ bbl/y). ARA stocks = European oil demand gauge. Floating storage creates Suez-Rotterdam spread trading.",
        "bottleneck_risk": "North Sea storms. Rhine barge transport critical; low water summers restrict flow. EU sanctions create sudden rerouting.",
    },
    "ANTWERP_BRUGES": {
        "name": "Antwerp-Bruges",
        "country": "Belgium", "region": "Scheldt Estuary",
        "lat": 51.3397, "lon": 4.2806,
        "type": "multi_purpose",
        "operator": "Port Authority of Antwerp",
        "annual_throughput_mt": 290,
        "commodities": ["crude_oil", "chemicals", "metals", "containers"],
        "max_vessel_size": "Post-Panamax",
        "connected_to": ["Chemical belt", "European refineries"],
        "key_fact": "Europe's chemicals hub (15M t/y specialty chemicals). Bruges breakbulk handles niche metals (cobalt, tin) for Asian rerouting.",
        "bottleneck_risk": "Scheldt siltation. North Sea storms. Belgian labor strikes. Low summer water on Meuse/Rhine.",
    },
    "HAMBURG_PORT": {
        "name": "Hamburg",
        "country": "Germany", "region": "Elbe River",
        "lat": 53.5511, "lon": 9.9769,
        "type": "multi_purpose",
        "operator": "Port of Hamburg Authority",
        "annual_throughput_mt": 310,
        "commodities": ["copper_blister", "copper_concentrate", "metals", "containers", "agricultural"],
        "max_vessel_size": "Panamax",
        "connected_to": ["Aurubis smelter", "Central European industry", "Suez routes"],
        "key_fact": "Aurubis copper smelter (1M+ t/y capacity) drives Cu concentrate arrival timing. Delays raise European refined copper premiums vs Asian.",
        "bottleneck_risk": "Elbe River silt/drought. Rhine/Danube connection critical for inland distribution. German dock labor expensive & unionized.",
    },
    # ── AFRICA & MIDDLE EAST ──────────────────────────────────
    "DURBAN": {
        "name": "Durban",
        "country": "South Africa", "region": "KwaZulu-Natal",
        "lat": -29.8587, "lon": 31.0218,
        "type": "multi_purpose",
        "operator": "Transnet (TNPA)",
        "annual_throughput_mt": 95,
        "commodities": ["coal", "chromite_ore", "manganese_ore", "ferrochrome", "containers"],
        "max_vessel_size": "Panamax",
        "connected_to": ["Mpumalanga coalfields", "Chrome/Mn mines", "Indian Ocean routes"],
        "key_fact": "Chrome & manganese ore exports (8-12M t/y) set global alloy prices. Any disruption impacts European/Chinese stainless steel costs.",
        "bottleneck_risk": "Power cuts/load shedding paralyze operations. Transnet infrastructure aging. Frequent labor unrest.",
    },
    "RICHARDS_BAY": {
        "name": "Richards Bay",
        "country": "South Africa", "region": "KwaZulu-Natal",
        "lat": -28.7828, "lon": 32.0831,
        "type": "bulk_terminal",
        "operator": "Transnet / private operators",
        "annual_throughput_mt": 85,
        "commodities": ["thermal_coal"],
        "max_vessel_size": "Capesize",
        "connected_to": ["Mpumalanga coalfields", "Atlantic coal routes"],
        "key_fact": "World's #2 thermal coal exporter (50-60M t/y). Richards Bay pricing sets global benchmarks (API2, API4). 2-3 week delays directly raise European coal futures.",
        "bottleneck_risk": "Load shedding reduces ops 30-50%. Frequent labor strikes. Infrastructure deterioration accelerating.",
    },
    "FUJAIRAH": {
        "name": "Fujairah",
        "country": "UAE", "region": "Gulf of Oman",
        "lat": 25.1266, "lon": 56.3373,
        "type": "oil_terminal",
        "operator": "Fujairah Terminals LLC",
        "annual_throughput_mt": 190,
        "commodities": ["crude_oil", "refined_products", "oil_storage"],
        "max_vessel_size": "VLCC",
        "connected_to": ["Middle East oil production", "Strait of Hormuz", "Singapore arb route"],
        "key_fact": "World's 3rd largest crude floating storage hub (20-25M bbl). Daily prices signal Strait of Hormuz geopolitical risk premium.",
        "bottleneck_risk": "Strait of Hormuz tanker traffic risk. Heat stress 50+°C reduces throughput 5-10%. Storage saturation delays Asia outlets.",
    },
    "JEBEL_ALI": {
        "name": "Jebel Ali / Dubai",
        "country": "UAE", "region": "Dubai",
        "lat": 24.9774, "lon": 55.1864,
        "type": "multi_purpose",
        "operator": "DP World",
        "annual_throughput_mt": 260,
        "commodities": ["containers", "precious_metals", "gold", "diamonds", "metals"],
        "max_vessel_size": "Post-Panamax",
        "connected_to": ["Middle East Gulf routes", "Indian Ocean lanes"],
        "key_fact": "World's largest gold re-export hub (500-600 t/y); sets global gold physical premium. Redirected from London during geopolitical episodes.",
        "bottleneck_risk": "Extreme summer heat. Geopolitical isolation from Iran sanctions restricts certain flows.",
    },
    # ── NORTH AMERICA ─────────────────────────────────────────
    "HOUSTON": {
        "name": "Houston Ship Channel",
        "country": "USA", "region": "Texas",
        "lat": 29.7519, "lon": -94.9789,
        "type": "oil_terminal",
        "operator": "Port of Houston Authority",
        "annual_throughput_mt": 280,
        "commodities": ["crude_oil", "refined_products", "LNG", "chemicals", "petrochemicals"],
        "max_vessel_size": "VLCC",
        "connected_to": ["Texas refining complex", "Chemical Coast", "Gulf LNG terminals"],
        "key_fact": "US crude oil export hub (3-4M bbl/d). Refining capacity 6.5M bbl/d nearby. Channel throughput directly impacts US energy competitiveness.",
        "bottleneck_risk": "Hurricane season Jun-Nov (1-2 week closures, petroleum prices spike 5-10%). Chemical spills disrupt 3-7 days.",
    },
    "NEW_ORLEANS": {
        "name": "New Orleans / South Louisiana",
        "country": "USA", "region": "Louisiana",
        "lat": 29.6105, "lon": -90.0788,
        "type": "bulk_terminal",
        "operator": "Port of South Louisiana",
        "annual_throughput_mt": 380,
        "commodities": ["grain", "soybeans", "coal", "crude_oil", "fertilizer"],
        "max_vessel_size": "Capesize",
        "connected_to": ["Midwest grain belt", "Mississippi River system", "Great Lakes"],
        "key_fact": "North America's grain export gateway (40-60M t/y). Mississippi River navigation season sets US export prices. Drought = throughput collapse.",
        "bottleneck_risk": "Hurricane season 3-5 weeks/year closures. Low water reduces barge capacity 40-50%. Grain elevator limits create 4-6 week backlogs.",
    },
    "VANCOUVER": {
        "name": "Vancouver",
        "country": "Canada", "region": "British Columbia",
        "lat": 49.2827, "lon": -123.1207,
        "type": "multi_purpose",
        "operator": "Port of Vancouver Authority",
        "annual_throughput_mt": 180,
        "commodities": ["thermal_coal", "potash", "grain", "containers", "sulfur"],
        "max_vessel_size": "Post-Panamax",
        "connected_to": ["Western Canada resources", "Asia Pacific routes"],
        "key_fact": "Canada's primary Pacific export hub. Coal exports 30-40M t/y price-referenced globally. Potash exports critical (Russian sanctions trade-offs).",
        "bottleneck_risk": "Winter ice/snow reduces 10-15% Dec-Feb. Rail strikes disrupt coal/grain. Mountain terrain limits rail expansion.",
    },
}


# ═══════════════════════════════════════════════════════════════
# LOGISTICS COMPANIES — Rail, road, pipeline operators
# The arteries of commodity supply chains
# ═══════════════════════════════════════════════════════════════

LOGISTICS_COMPANIES = {
    "FERROCARRIL_CERREJON": {
        "name": "Ferrocarril del Cerrejón",
        "hq": "Valledupar, Colombia",
        "lat": 10.4806, "lon": -73.2470,
        "type": "rail",
        "commodities": ["coal"],
        "key_corridors": ["Cerrejón mine to Puerto Bolívar"],
        "competitive_edge": "Only rail connection for world's largest open-pit coal mine; exclusive monopoly on Cerrejón export logistics.",
        "key_fact": "Moves ~30M t/y Colombian thermal coal; disruption = global coal shortage.",
    },
    "VALE_CARAJAS_RAIL": {
        "name": "VALE Carajás Railroad (EFC)",
        "hq": "Rio de Janeiro, Brazil",
        "lat": -22.9068, "lon": -43.1729,
        "type": "rail",
        "commodities": ["iron_ore", "manganese"],
        "key_corridors": ["Carajás mine to São Luís port", "NE Brazil mining corridor"],
        "competitive_edge": "World's longest single-track heavy-haul railway (890km); integrated with Vale mine operations.",
        "key_fact": "Transports 150M t/y iron ore; Vale controls mining + rail + port vertically.",
    },
    "PILBARA_RAILWAYS": {
        "name": "Pilbara Railways (BHP/Rio Tinto)",
        "hq": "Perth, Australia",
        "lat": -22.3494, "lon": 118.5313,
        "type": "rail",
        "commodities": ["iron_ore"],
        "key_corridors": ["Interior Pilbara mines to Port Hedland"],
        "competitive_edge": "World's longest automated heavy-haul trains; driverless operation reduces costs 20%+.",
        "key_fact": "Combined ~400M t/y; autonomous trains = benchmark for mining logistics efficiency.",
    },
    "AURIZON": {
        "name": "Aurizon Holdings",
        "hq": "Brisbane, Australia",
        "lat": -27.4698, "lon": 153.0251,
        "type": "multimodal",
        "commodities": ["coal", "iron_ore", "agriculture", "minerals"],
        "key_corridors": ["Queensland coal belt to ports", "Bowen Basin network"],
        "competitive_edge": "Australia's largest coal rail operator; 2,700km network.",
        "key_fact": "Moves 200M t/y coal; essential middleman between miners and export terminals.",
    },
    "BNSF_RAILWAY": {
        "name": "BNSF Railway",
        "hq": "Fort Worth, Texas, USA",
        "lat": 32.7555, "lon": -97.3308,
        "type": "rail",
        "commodities": ["coal", "grain", "minerals", "containers"],
        "key_corridors": ["Powder River Basin to West Coast", "Midwest grain to Gulf ports"],
        "competitive_edge": "North America's largest freight railway; 32,500 miles; coal monopoly through Powder River Basin.",
        "key_fact": "500M+ t/y; controls North American coal export to Asia.",
    },
    "UNION_PACIFIC": {
        "name": "Union Pacific Railroad",
        "hq": "Omaha, Nebraska, USA",
        "lat": 41.2619, "lon": -95.9018,
        "type": "rail",
        "commodities": ["coal", "grain", "chemicals", "minerals"],
        "key_corridors": ["Powder River Basin to Pacific", "Midwest grain belt", "Transcontinental"],
        "competitive_edge": "Largest US railroad; 32,400 miles; integrated with grain storage terminals.",
        "key_fact": "450M+ t/y; competes with BNSF on coal; dominates US domestic coal logistics.",
    },
    "FERROMEX": {
        "name": "Ferrocarril Mexicano (Ferromex)",
        "hq": "Mexico City, Mexico",
        "lat": 19.4326, "lon": -99.1332,
        "type": "rail",
        "commodities": ["minerals", "grain", "containers"],
        "key_corridors": ["Northern Mexico to ports", "US-Mexico transcontinental"],
        "competitive_edge": "Mexico's largest rail operator; 20,400km; Grupo México ownership = integrated mining + logistics.",
        "key_fact": "Controls mineral export from Mexico's mining heartland; essential for North American commodity flows.",
    },
    "TRANSNET": {
        "name": "Transnet Freight Rail (South Africa)",
        "hq": "Johannesburg, South Africa",
        "lat": -26.2023, "lon": 28.0436,
        "type": "rail",
        "commodities": ["coal", "chromium", "iron_ore"],
        "key_corridors": ["Mpumalanga coal belt to Richards Bay", "Kalahari chrome corridor"],
        "competitive_edge": "South Africa's state-owned rail monopoly; coal dominance through Richards Bay export.",
        "key_fact": "200M t/y coal; controls African coal supply to power plants; chrome export monopoly for SA.",
    },
    "RZD": {
        "name": "Russian Railways (RZD)",
        "hq": "Moscow, Russia",
        "lat": 55.7558, "lon": 37.6173,
        "type": "multimodal",
        "commodities": ["coal", "oil", "grain", "minerals", "metals"],
        "key_corridors": ["Trans-Siberian Railway", "Siberian coal to Pacific", "Grain to Black Sea"],
        "competitive_edge": "World's second-longest rail network; monopoly on Siberian commodity export.",
        "key_fact": "1B+ t/y; Russia's commodity monopoly; coal export control = geopolitical leverage.",
    },
    "BOLLORE": {
        "name": "Bolloré Transport & Logistics",
        "hq": "Paris, France",
        "lat": 48.8566, "lon": 2.3522,
        "type": "freight_forwarding",
        "commodities": ["all_commodities", "mining_logistics"],
        "key_corridors": ["Sub-Saharan Africa freight", "West/Central Africa ports"],
        "competitive_edge": "Africa's largest integrated logistics provider; 45+ country presence; mining supply chain specialist.",
        "key_fact": "Essential middleman for African mineral exports; controls freight flows for artisanal miners.",
    },
    "DHL_FORWARDING": {
        "name": "DHL Global Forwarding",
        "hq": "Bonn, Germany",
        "lat": 50.7285, "lon": 7.0921,
        "type": "freight_forwarding",
        "commodities": ["all_commodities", "specialty_cargo"],
        "key_corridors": ["Global commodity routes", "Port-to-port forwarding"],
        "competitive_edge": "Global freight forwarding leader; 220+ countries; real-time tracking.",
        "key_fact": "Traders use DHL for price certainty and supply chain visibility on non-bulk trades.",
    },
}


# ═══════════════════════════════════════════════════════════════
# INSPECTION & QA COMPANIES — The trust layer
# No commodity trades without inspection certificates
# ═══════════════════════════════════════════════════════════════

INSPECTION_QA = {
    "SGS": {
        "name": "SGS S.A.",
        "hq": "Geneva, Switzerland",
        "lat": 46.2044, "lon": 6.1432,
        "type": "inspection",
        "services": ["weight_survey", "quality_analysis", "sampling", "loading_supervision", "pre_shipment_inspection", "assay_laboratory", "cargo_tracking"],
        "commodities_coverage": ["coal", "iron_ore", "copper", "lithium", "oil", "grain", "coffee", "minerals"],
        "global_presence": 140,
        "competitive_edge": "World's #1 inspection company; gold-standard for trading contracts; ISO 17025 accreditation.",
        "key_fact": "90% of commodity trades reference SGS inspection = market price discovery depends on them.",
    },
    "BUREAU_VERITAS": {
        "name": "Bureau Veritas",
        "hq": "Paris, France",
        "lat": 48.8566, "lon": 2.3522,
        "type": "inspection",
        "services": ["weight_survey", "quality_analysis", "sampling", "loading_supervision", "pre_shipment_inspection", "cargo_tracking"],
        "commodities_coverage": ["coal", "iron_ore", "metals", "grain", "oils", "minerals"],
        "global_presence": 130,
        "competitive_edge": "#2 global inspector; specializes in coal/iron ore; strong Asia-Pacific port presence.",
        "key_fact": "Primary SGS competitor; traders negotiate between SGS/BV for inspection terms.",
    },
    "INTERTEK": {
        "name": "Intertek Group",
        "hq": "London, UK",
        "lat": 51.5074, "lon": -0.1278,
        "type": "inspection",
        "services": ["weight_survey", "quality_analysis", "sampling", "pre_shipment_inspection", "assay_laboratory", "cargo_tracking"],
        "commodities_coverage": ["coal", "metals", "minerals", "grain", "oil"],
        "global_presence": 100,
        "competitive_edge": "#3 inspector globally; coal quality expert; rapid lab turnaround; strong in SE Asia.",
        "key_fact": "Growing market share in thermal coal inspection; key for Powder River Basin exports.",
    },
    "ALS_LIMITED": {
        "name": "ALS Limited",
        "hq": "Brisbane, Australia",
        "lat": -27.4698, "lon": 153.0251,
        "type": "assay",
        "services": ["assay_laboratory", "quality_analysis", "sampling"],
        "commodities_coverage": ["iron_ore", "copper", "gold", "lithium", "rare_earths"],
        "global_presence": 60,
        "competitive_edge": "Australia's #1 mining assay lab; ore-grade specialist; rapid precious metals analysis.",
        "key_fact": "Traders use ALS for mine-mouth assays; controls ore quality certainty for Australian exports.",
    },
    "CCIC": {
        "name": "China Certification & Inspection Group",
        "hq": "Beijing, China",
        "lat": 39.9042, "lon": 116.4074,
        "type": "inspection",
        "services": ["weight_survey", "quality_analysis", "sampling", "pre_shipment_inspection"],
        "commodities_coverage": ["iron_ore", "coal", "oil", "grain", "minerals"],
        "global_presence": 80,
        "competitive_edge": "China's state inspection monopoly; required for imports into China; integrated with customs.",
        "key_fact": "Mandatory for all commodity trades into China; biggest volume inspector for iron ore/coal globally.",
    },
    "ALEX_STEWART": {
        "name": "Alex Stewart International",
        "hq": "London, UK",
        "lat": 51.5074, "lon": -0.1278,
        "type": "assay",
        "services": ["assay_laboratory", "weight_survey", "sampling", "quality_analysis"],
        "commodities_coverage": ["copper", "gold", "silver", "zinc", "nickel"],
        "global_presence": 35,
        "competitive_edge": "Metals trading specialist; fastest assay turnaround for precious metals; London hub credibility.",
        "key_fact": "Copper traders prefer Alex Stewart; assay = price, so turnaround speed = money.",
    },
    "ALFRED_KNIGHT": {
        "name": "Alfred H. Knight",
        "hq": "London, UK",
        "lat": 51.5074, "lon": -0.1278,
        "type": "inspection",
        "services": ["weight_survey", "sampling", "quality_analysis", "pre_shipment_inspection"],
        "commodities_coverage": ["iron_ore", "metals", "minerals", "coal"],
        "global_presence": 50,
        "competitive_edge": "Independent London-based surveyor; neutral party for buyer/seller disputes.",
        "key_fact": "Used when SGS/BV conflicts arise; arbitration choice for commodity disputes.",
    },
    "CESMEC": {
        "name": "CESMEC (Centro de Estudios y Servicios de Minería)",
        "hq": "Santiago, Chile",
        "lat": -33.4489, "lon": -70.6693,
        "type": "assay",
        "services": ["assay_laboratory", "quality_analysis", "sampling"],
        "commodities_coverage": ["copper", "lithium", "molybdenum", "gold"],
        "global_presence": 5,
        "competitive_edge": "Chile's mining lab specialist; integrated with Chilean mining ministry; copper/lithium expert.",
        "key_fact": "Required for Chilean mine-mouth assays; traders use CESMEC for Chilean copper certification.",
    },
    "COTECNA": {
        "name": "Cotecna Inspection S.A.",
        "hq": "Geneva, Switzerland",
        "lat": 46.2044, "lon": 6.1432,
        "type": "inspection",
        "services": ["weight_survey", "quality_analysis", "sampling", "pre_shipment_inspection"],
        "commodities_coverage": ["all_commodities", "agricultural", "minerals", "metals", "oil"],
        "global_presence": 120,
        "competitive_edge": "Swiss independence; trade facilitation specialist; developing market specialist.",
        "key_fact": "Cotecna = dispute resolution trust; used in emerging market commodity trades.",
    },
    "TUV_SUD": {
        "name": "TÜV SÜD",
        "hq": "Munich, Germany",
        "lat": 48.1351, "lon": 11.5820,
        "type": "certification",
        "services": ["quality_analysis", "certification", "sampling"],
        "commodities_coverage": ["minerals", "metals", "chemicals"],
        "global_presence": 75,
        "competitive_edge": "German quality standard; certification focus; EU regulation alignment; ESG/conflict mineral traceability.",
        "key_fact": "Critical for EU commodity imports; responsible sourcing audits = premium market access.",
    },
}


# ═══════════════════════════════════════════════════════════════
# END CLIENTS — Who ultimately BUYS what mines and traders sell
# The demand side of every commodity equation
# ═══════════════════════════════════════════════════════════════

END_CLIENTS = {
    # ── COPPER / LITHIUM / BATTERY ────────────────────────────
    "CATL": {
        "name": "CATL (Contemporary Amperex Technology)",
        "hq": "Ningde, China",
        "lat": 26.6545, "lon": 120.3218,
        "type": "battery_maker",
        "commodities_consumed": ["copper", "lithium", "cobalt", "nickel"],
        "annual_consumption": {"copper_kt": 250, "lithium_kt": 180, "cobalt_kt": 12, "nickel_kt": 80},
        "key_suppliers": ["Codelco", "Ganfeng Lithium", "Glencore", "Rio Tinto"],
        "key_fact": "World's #1 EV battery maker; buying power = commodity price setter; 10-year supply contracts lock trader relationships. Consumes 30% of world lithium.",
    },
    "BYD": {
        "name": "BYD Company Limited",
        "hq": "Shenzhen, China",
        "lat": 22.5431, "lon": 114.0579,
        "type": "battery_maker",
        "commodities_consumed": ["lithium", "cobalt", "copper", "iron"],
        "annual_consumption": {"lithium_kt": 120, "cobalt_kt": 8, "copper_kt": 150},
        "key_suppliers": ["Ganfeng", "Glencore", "Codelco", "Vale"],
        "key_fact": "China's #1 EV + battery maker; competes with CATL on commodity cost; supply negotiations set benchmark prices.",
    },
    "LG_ENERGY": {
        "name": "LG Energy Solution",
        "hq": "Seoul, South Korea",
        "lat": 37.5665, "lon": 126.9780,
        "type": "battery_maker",
        "commodities_consumed": ["lithium", "cobalt", "nickel"],
        "annual_consumption": {"lithium_kt": 80, "cobalt_kt": 6, "nickel_kt": 50},
        "key_suppliers": ["Ganfeng", "Glencore/Trafigura", "Indonesian nickel laterite miners"],
        "key_fact": "Korean EV battery challenger; higher quality = pays premium prices; sets battery-grade lithium/cobalt standards.",
    },
    "TESLA_PANASONIC": {
        "name": "Tesla / Panasonic Battery Partnership",
        "hq": "Nevada, USA / Osaka, Japan",
        "lat": 39.1433, "lon": -119.7674,
        "type": "battery_maker",
        "commodities_consumed": ["lithium", "cobalt", "nickel"],
        "annual_consumption": {"lithium_kt": 95, "cobalt_kt": 5, "nickel_kt": 60},
        "key_suppliers": ["Albemarle", "Ganfeng", "Glencore"],
        "key_fact": "Tesla's vertical integration = direct mining relationships; Panasonic partnership = battery-grade commodity lock-ins.",
    },
    # ── COPPER FABRICATION & UTILITIES ────────────────────────
    "AURUBIS_BUYER": {
        "name": "Aurubis AG",
        "hq": "Hamburg, Germany",
        "lat": 53.5511, "lon": 10.0046,
        "type": "smelter_buyer",
        "commodities_consumed": ["copper", "precious_metals"],
        "annual_consumption": {"copper_kt": 1000},
        "key_suppliers": ["Codelco", "Antofagasta", "Freeport-McMoRan", "Glencore", "Trafigura"],
        "key_fact": "Europe's #1 copper smelter/refiner; capacity utilization = demand signal. TC/RC negotiations set annual global concentrate market terms.",
    },
    "STATE_GRID_CHINA": {
        "name": "State Grid Corporation of China (SGCC)",
        "hq": "Beijing, China",
        "lat": 39.9042, "lon": 116.4074,
        "type": "utility",
        "commodities_consumed": ["copper", "coal"],
        "annual_consumption": {"copper_kt": 5000, "coal_mt": 500},
        "key_suppliers": ["Codelco", "Mongolian miners", "Australian coal exporters"],
        "key_fact": "World's largest utility; 5M t/y copper buying = global price anchor. Traders watch SGCC procurement cycles.",
    },
    # ── IRON ORE / STEEL ──────────────────────────────────────
    "CHINA_BAOWU": {
        "name": "China Baowu Steel Group",
        "hq": "Shanghai, China",
        "lat": 31.2304, "lon": 121.4737,
        "type": "manufacturer",
        "commodities_consumed": ["iron_ore"],
        "annual_consumption": {"iron_ore_mt": 100},
        "key_suppliers": ["Vale", "Rio Tinto", "BHP", "Fortescue"],
        "key_fact": "World's #1 steel producer (100M t/y); iron ore buying = global commodity price benchmark.",
    },
    "ARCELORMITTAL": {
        "name": "ArcelorMittal",
        "hq": "Luxembourg",
        "lat": 49.6116, "lon": 6.1319,
        "type": "manufacturer",
        "commodities_consumed": ["iron_ore", "coal"],
        "annual_consumption": {"iron_ore_mt": 75, "coal_mt": 20},
        "key_suppliers": ["Vale", "Rio Tinto", "BHP", "Fortescue"],
        "key_fact": "Global steel #2; European + American buyer; long-term contracts = price stability signals.",
    },
    "NIPPON_STEEL": {
        "name": "Nippon Steel Corporation",
        "hq": "Tokyo, Japan",
        "lat": 35.6762, "lon": 139.6503,
        "type": "manufacturer",
        "commodities_consumed": ["iron_ore", "coal"],
        "annual_consumption": {"iron_ore_mt": 60, "coal_mt": 15},
        "key_suppliers": ["Vale", "Rio Tinto", "BHP", "Australian coal"],
        "key_fact": "Japan's largest steelmaker; premium buyer (quality over price); sets technical specifications for ore grade.",
    },
    # ── OIL ───────────────────────────────────────────────────
    "SINOPEC": {
        "name": "Sinopec Group",
        "hq": "Beijing, China",
        "lat": 39.9042, "lon": 116.4074,
        "type": "manufacturer",
        "commodities_consumed": ["oil", "natural_gas"],
        "annual_consumption": {"oil_mbpd": 8},
        "key_suppliers": ["Saudi Aramco", "Russian Energy Ministry", "Iraqi OPEC", "Trafigura"],
        "key_fact": "China's largest refiner; refining capacity = oil price setter for Asia. Geopolitical supply chain indicator.",
    },
    "INDIAN_OIL": {
        "name": "Indian Oil Corporation (IOC)",
        "hq": "New Delhi, India",
        "lat": 28.6139, "lon": 77.2090,
        "type": "manufacturer",
        "commodities_consumed": ["oil", "natural_gas"],
        "annual_consumption": {"oil_mbpd": 1.4},
        "key_suppliers": ["OPEC countries", "Russian traders", "African exporters"],
        "key_fact": "India's largest refiner; spot buyer = price discovery for Asian refining demand.",
    },
    # ── COFFEE ────────────────────────────────────────────────
    "NESTLE": {
        "name": "Nestlé S.A.",
        "hq": "Vevey, Switzerland",
        "lat": 46.4612, "lon": 6.8373,
        "type": "food_processor",
        "commodities_consumed": ["coffee", "cocoa", "sugar"],
        "annual_consumption": {"coffee_kt": 700, "cocoa_kt": 500},
        "key_suppliers": ["Brazilian farms", "Colombian growers", "Vietnamese robusta", "African co-ops"],
        "key_fact": "World's #1 coffee buyer (700k t/y); Nestlé contracts = coffee price anchor. Supply chain transparency = ESG premium.",
    },
    "STARBUCKS": {
        "name": "Starbucks Corporation",
        "hq": "Seattle, USA",
        "lat": 47.6062, "lon": -122.3321,
        "type": "food_processor",
        "commodities_consumed": ["coffee"],
        "annual_consumption": {"coffee_kt": 180},
        "key_suppliers": ["Direct relationships 30+ countries", "Brazilian suppliers", "Colombian co-ops"],
        "key_fact": "Premium buyer; pays 30-50% above market for certified arabica; direct farmer relationships = brand loyalty signals.",
    },
    "JDE_PEETS": {
        "name": "JDE Peet's (Jacobs Douwe Egberts)",
        "hq": "Amsterdam, Netherlands",
        "lat": 52.3676, "lon": 4.9041,
        "type": "food_processor",
        "commodities_consumed": ["coffee"],
        "annual_consumption": {"coffee_kt": 350},
        "key_suppliers": ["Brazilian estates", "Indonesian robusta", "Vietnamese suppliers"],
        "key_fact": "Europe's largest coffee buyer; price-sensitive on volume buys; commodity desk-driven purchasing.",
    },
    # ── COAL / POWER ──────────────────────────────────────────
    "KEPCO": {
        "name": "Korea Electric Power Corporation",
        "hq": "Naju, South Korea",
        "lat": 35.0181, "lon": 126.7945,
        "type": "utility",
        "commodities_consumed": ["coal", "natural_gas"],
        "annual_consumption": {"coal_mt": 100},
        "key_suppliers": ["Indonesian coal", "Australian thermal coal", "South African coal"],
        "key_fact": "South Korea's largest utility; thermal coal dependent; buying = Asia Pacific price discovery.",
    },
    "JERA": {
        "name": "JERA Co., Inc.",
        "hq": "Tokyo, Japan",
        "lat": 35.6762, "lon": 139.6503,
        "type": "utility",
        "commodities_consumed": ["coal", "natural_gas", "oil"],
        "annual_consumption": {"coal_mt": 90, "lng_bcm": 80},
        "key_suppliers": ["Australian coal", "Indonesian coal", "Qatar LNG", "Australian LNG"],
        "key_fact": "Japan's largest power utility; coal + LNG buyer sets Asian fuel mix pricing.",
    },
    "ESKOM": {
        "name": "Eskom Holdings SOC",
        "hq": "Johannesburg, South Africa",
        "lat": -26.2023, "lon": 28.0436,
        "type": "utility",
        "commodities_consumed": ["coal"],
        "annual_consumption": {"coal_mt": 150},
        "key_suppliers": ["Domestic South African mines", "Transnet (monopoly transport)"],
        "key_fact": "South Africa's utility monopoly; domestic coal supply = no international competition; supply crisis = Africa power shortage signal.",
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

def _commodity_match(commodity_lower: str, candidate: str) -> bool:
    """Check if a commodity name matches a candidate string."""
    candidate_lower = candidate.lower()
    return commodity_lower in candidate_lower or candidate_lower in commodity_lower


def get_commodity_context(commodity: str) -> Dict[str, Any]:
    """
    Build comprehensive industry context for a commodity.
    This is injected into agent prompts to give them institutional knowledge.
    Now includes the full supply chain: mines → ports → ships → smelters → clients.
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
        if _commodity_match(commodity_lower, mine["commodity"]):
            relevant_mines.append(mine)

    # Find relevant refineries/smelters
    relevant_plants = []
    for plant_id, plant in REFINERIES_SMELTERS.items():
        if _commodity_match(commodity_lower, plant["commodity"]):
            relevant_plants.append(plant)

    # Find relevant exchanges
    relevant_exchanges = []
    for ex_id, ex in EXCHANGES.items():
        for comm in ex["commodities"]:
            if _commodity_match(commodity_lower, comm):
                relevant_exchanges.append({"id": ex_id, **ex})
                break

    # Find relevant traders
    relevant_traders = []
    for trader_id, trader in MAJOR_TRADERS.items():
        for comm in trader["commodities"]:
            if _commodity_match(commodity_lower, comm):
                relevant_traders.append({"id": trader_id, **trader})
                break

    # Find relevant ports
    relevant_ports = []
    for port_id, port in MAJOR_PORTS.items():
        for comm in port["commodities"]:
            if _commodity_match(commodity_lower, comm):
                relevant_ports.append({"id": port_id, **port})
                break

    # Find relevant shipping companies
    relevant_shipping = []
    for ship_id, ship in SHIPPING_COMPANIES.items():
        for comm in ship["commodities"]:
            if _commodity_match(commodity_lower, comm):
                relevant_shipping.append({"id": ship_id, **ship})
                break

    # Find relevant end clients
    relevant_clients = []
    for client_id, client in END_CLIENTS.items():
        for comm in client["commodities_consumed"]:
            if _commodity_match(commodity_lower, comm):
                relevant_clients.append({"id": client_id, **client})
                break

    # Find relevant inspection/QA companies
    relevant_qa = []
    for qa_id, qa in INSPECTION_QA.items():
        for comm in qa["commodities_coverage"]:
            if _commodity_match(commodity_lower, comm) or comm == "all_commodities":
                relevant_qa.append({"id": qa_id, **qa})
                break

    # Exchange tickers
    tickers = EXCHANGE_TICKERS.get(commodity_lower, {})

    return {
        "supply_chain": chain,
        "mines": relevant_mines,
        "refineries_smelters": relevant_plants,
        "exchanges": relevant_exchanges,
        "major_traders": relevant_traders,
        "ports": relevant_ports,
        "shipping": relevant_shipping,
        "end_clients": relevant_clients,
        "inspection_qa": relevant_qa,
        "exchange_tickers": tickers,
    }


def build_agent_context_prompt(commodity: str) -> str:
    """
    Build a concise industry context string to inject into agent prompts.
    Now includes the FULL supply chain: mine → port → ship → smelter → client → QA.
    Optimized to stay under 800 tokens.
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

    # Key ports
    if ctx["ports"]:
        ports_str = ", ".join(
            f"{p['name']} ({p['country']}, {p.get('annual_throughput_mt', '?')}Mt/y)"
            for p in ctx["ports"][:5]
        )
        parts.append(f"KEY PORTS: {ports_str}")

    # Shipping
    if ctx["shipping"]:
        ship_str = ", ".join(s["name"] for s in ctx["shipping"][:4])
        parts.append(f"SHIPPING: {ship_str}")

    # End clients
    if ctx["end_clients"]:
        clients_str = ", ".join(
            f"{c['name']} ({c['type']})"
            for c in ctx["end_clients"][:4]
        )
        parts.append(f"END BUYERS: {clients_str}")

    # Traders
    if ctx["major_traders"]:
        traders_str = ", ".join(t["name"] for t in ctx["major_traders"][:5])
        parts.append(f"MAJOR TRADERS: {traders_str}")

    # Exchanges
    if ctx["exchanges"]:
        exchanges_str = ", ".join(f"{e['id']} ({e['city']})" for e in ctx["exchanges"][:4])
        parts.append(f"EXCHANGES: {exchanges_str}")

    # QA/Inspection
    if ctx["inspection_qa"]:
        qa_str = ", ".join(q["name"] for q in ctx["inspection_qa"][:3])
        parts.append(f"INSPECTION/QA: {qa_str}")

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


def get_all_ports_summary() -> List[Dict[str, Any]]:
    """Return summary of all major ports for the globe visualization."""
    return [
        {
            "id": port_id,
            "name": p["name"],
            "country": p["country"],
            "lat": p["lat"],
            "lon": p["lon"],
            "type": p["type"],
            "annual_throughput_mt": p.get("annual_throughput_mt", 0),
            "commodities": p["commodities"],
            "max_vessel_size": p.get("max_vessel_size", ""),
            "bottleneck_risk": p.get("bottleneck_risk", ""),
        }
        for port_id, p in MAJOR_PORTS.items()
    ]


def get_all_shipping_summary() -> List[Dict[str, Any]]:
    """Return summary of all shipping companies."""
    return [
        {
            "id": ship_id,
            "name": s["name"],
            "hq": s["hq"],
            "lat": s["lat"],
            "lon": s["lon"],
            "type": s["type"],
            "fleet_size": s.get("fleet_size", 0),
            "dwt_million": s.get("dwt_million", 0),
            "commodities": s["commodities"],
        }
        for ship_id, s in SHIPPING_COMPANIES.items()
    ]


def get_all_logistics_summary() -> List[Dict[str, Any]]:
    """Return summary of all logistics companies."""
    return [
        {
            "id": log_id,
            "name": l["name"],
            "hq": l["hq"],
            "lat": l["lat"],
            "lon": l["lon"],
            "type": l["type"],
            "commodities": l["commodities"],
            "key_corridors": l.get("key_corridors", []),
        }
        for log_id, l in LOGISTICS_COMPANIES.items()
    ]


def get_all_qa_summary() -> List[Dict[str, Any]]:
    """Return summary of all inspection/QA companies."""
    return [
        {
            "id": qa_id,
            "name": q["name"],
            "hq": q["hq"],
            "lat": q["lat"],
            "lon": q["lon"],
            "type": q["type"],
            "services": q["services"],
            "global_presence": q.get("global_presence", 0),
        }
        for qa_id, q in INSPECTION_QA.items()
    ]


def get_all_clients_summary() -> List[Dict[str, Any]]:
    """Return summary of all end clients/consumers."""
    return [
        {
            "id": client_id,
            "name": c["name"],
            "hq": c["hq"],
            "lat": c["lat"],
            "lon": c["lon"],
            "type": c["type"],
            "commodities_consumed": c["commodities_consumed"],
        }
        for client_id, c in END_CLIENTS.items()
    ]


def get_knowledge_graph_stats() -> Dict[str, int]:
    """Return counts of all entities in the knowledge graph."""
    return {
        "exchanges": len(EXCHANGES),
        "traders": len(MAJOR_TRADERS),
        "mines": len(MAJOR_MINES),
        "smelters_refineries": len(REFINERIES_SMELTERS),
        "shipping_companies": len(SHIPPING_COMPANIES),
        "ports": len(MAJOR_PORTS),
        "logistics_companies": len(LOGISTICS_COMPANIES),
        "inspection_qa": len(INSPECTION_QA),
        "end_clients": len(END_CLIENTS),
        "supply_chains": len(SUPPLY_CHAINS),
        "total_entities": (
            len(EXCHANGES) + len(MAJOR_TRADERS) + len(MAJOR_MINES) +
            len(REFINERIES_SMELTERS) + len(SHIPPING_COMPANIES) + len(MAJOR_PORTS) +
            len(LOGISTICS_COMPANIES) + len(INSPECTION_QA) + len(END_CLIENTS)
        ),
    }
