#!/usr/bin/env python3
"""
METEORO X v12 — AUTONOMOUS CORRESPONDENTS NETWORK
===================================================
AI correspondents deployed in commodity-critical countries.
Each correspondent monitors local press, official gazettes,
social media, industry publications, and government portals 24/7.

This replaces what Bloomberg achieves with 2,700 human journalists
using autonomous AI agents that never sleep, read every language,
and detect signals hours before Western media picks them up.

ARCHITECTURE:
  Each country has a correspondent configuration with:
  - Local news sources (official gazettes, newspapers, portals)
  - Key commodities to monitor
  - Critical entities (mines, companies, politicians)
  - Alert triggers (strikes, protests, policy changes, disasters)
  - Language and cultural context

  Correspondents are activated by the swarm when analyzing
  commodities from their region. They provide LOCAL context
  that no global LLM has — the kind of information that
  a trader on the ground in Lima or Santiago would know.

USAGE:
  from data_sources.correspondents import (
      get_correspondent_for_country,
      get_correspondents_for_commodity,
      build_correspondent_prompt,
      CORRESPONDENTS,
  )
"""

from typing import Dict, List, Any, Optional


# ═══════════════════════════════════════════════════════════════
# CORRESPONDENT NETWORK — 12 Countries
# Each correspondent is an intelligence brief builder
# ═══════════════════════════════════════════════════════════════

CORRESPONDENTS = {
    # ═══════ LATIN AMERICA ═══════════════════════════════════
    "COLOMBIA": {
        "name": "Corresponsal Colombia",
        "country": "Colombia",
        "capital": "Bogotá",
        "lat": 4.7110, "lon": -74.0721,
        "language": "es",
        "commodities": ["coal", "coffee", "gold", "oil", "emeralds", "nickel"],
        "critical_entities": {
            "mines": ["Cerrejón (coal, La Guajira)", "Prodeco/Glencore (coal)", "Buriticá (gold, Antioquia)", "Cerromatoso (nickel, Córdoba)"],
            "companies": ["Ecopetrol (oil)", "FNC (Federación Nacional de Cafeteros)", "Glencore Colombia", "Drummond"],
            "ports": ["Puerto Bolívar", "Santa Marta", "Buenaventura", "Cartagena"],
            "politicians": ["Presidente", "MinMinas (Ministerio de Minas)", "ANM (Agencia Nacional de Minería)", "ANH (Agencia Nacional de Hidrocarburos)"],
        },
        "news_sources": [
            {"name": "El Tiempo", "url": "https://www.eltiempo.com", "type": "newspaper", "focus": "general"},
            {"name": "Portafolio", "url": "https://www.portafolio.co", "type": "business", "focus": "economy/commodities"},
            {"name": "La República", "url": "https://www.larepublica.co", "type": "business", "focus": "markets/mining"},
            {"name": "Minería Pan-Americana", "url": "https://www.mineriapanamericana.com", "type": "industry", "focus": "mining"},
            {"name": "Diario Oficial Colombia", "url": "https://www.suin-juriscol.gov.co", "type": "gazette", "focus": "regulation"},
            {"name": "ANM Portal", "url": "https://www.anm.gov.co", "type": "government", "focus": "mining_permits"},
        ],
        "alert_triggers": [
            "Bloqueos de vías a minas (road blockades to mines)",
            "Huelgas en puertos carbón (coal port strikes)",
            "Regulación ambiental nuevas minas (environmental regulation)",
            "Precio interno del café (domestic coffee pricing)",
            "Producción Ecopetrol (oil production reports)",
            "Protestas comunidades mineras (community protests)",
            "Lluvias/inundaciones que afectan logística (flooding)",
            "Reforma tributaria minera (mining tax reform)",
        ],
        "key_intelligence": "Colombia is #5 thermal coal exporter globally and #3 Arabica coffee. President Petro's leftist government has signaled transition away from fossil fuels. Community protests at mines are frequent and can halt production for weeks. Coffee production shifting to higher altitudes due to climate change.",
    },

    "PERU": {
        "name": "Corresponsal Perú",
        "country": "Peru",
        "capital": "Lima",
        "lat": -12.0464, "lon": -77.0428,
        "language": "es",
        "commodities": ["copper", "gold", "zinc", "silver", "tin", "molybdenum", "lead"],
        "critical_entities": {
            "mines": ["Cerro Verde (copper)", "Antamina (copper/zinc)", "Las Bambas (copper)", "Yanacocha (gold)", "Quellaveco (copper, new)"],
            "companies": ["Southern Copper/Grupo México", "Freeport-McMoRan", "Buenaventura", "MMG Limited", "Anglo American"],
            "ports": ["Callao", "Matarani", "Ilo"],
            "politicians": ["Presidente", "MINEM (Ministerio de Energía y Minas)", "OEFA (Organismo de Evaluación Ambiental)", "SUNAT (tax authority)"],
        },
        "news_sources": [
            {"name": "RPP Noticias", "url": "https://rpp.pe", "type": "newspaper", "focus": "general/breaking"},
            {"name": "Gestión", "url": "https://gestion.pe", "type": "business", "focus": "economy/mining"},
            {"name": "El Comercio", "url": "https://elcomercio.pe", "type": "newspaper", "focus": "politics/economy"},
            {"name": "Diario El Peruano", "url": "https://diariooficial.elperuano.pe", "type": "gazette", "focus": "official_decrees"},
            {"name": "ProActivo", "url": "https://proactivo.com.pe", "type": "industry", "focus": "mining_specific"},
            {"name": "SNMPE", "url": "https://www.snmpe.org.pe", "type": "industry_association", "focus": "mining_energy"},
        ],
        "alert_triggers": [
            "Bloqueo corredor Las Bambas (road blockade copper corridor)",
            "Protestas comunidades Apurímac/Cusco (community protests)",
            "Permisos ambientales nuevos proyectos (environmental permits)",
            "Producción mensual MINEM (monthly production reports)",
            "Inestabilidad política/cambio de gobierno (political instability)",
            "Huelgas sindicales mineros (miner union strikes)",
            "Conflictos agua mina vs agricultura (water conflict)",
            "Exportaciones Callao (Callao port exports)",
        ],
        "key_intelligence": "Peru is #2 copper, #2 silver, #2 zinc producer globally. Political instability is chronic — 6 presidents in 5 years. Las Bambas corridor blockades are recurring (community demands road improvements). Quellaveco (Anglo American) ramping up adds ~300kt copper/year. Water conflicts between mining and agriculture are increasing.",
    },

    "CHILE": {
        "name": "Corresponsal Chile",
        "country": "Chile",
        "capital": "Santiago",
        "lat": -33.4489, "lon": -70.6693,
        "language": "es",
        "commodities": ["copper", "lithium", "molybdenum", "iodine", "rhenium"],
        "critical_entities": {
            "mines": ["Escondida (BHP, copper)", "Chuquicamata (Codelco)", "El Teniente (Codelco)", "Salar de Atacama (SQM/Albemarle, lithium)"],
            "companies": ["Codelco (state)", "SQM", "Albemarle", "BHP Chile", "Antofagasta Minerals"],
            "ports": ["Antofagasta", "San Antonio", "Valparaíso", "Mejillones"],
            "politicians": ["Presidente", "Ministerio de Minería", "Cochilco (Comisión Chilena del Cobre)", "Sernageomin"],
        },
        "news_sources": [
            {"name": "Diario Financiero", "url": "https://www.df.cl", "type": "business", "focus": "markets/mining"},
            {"name": "El Mercurio", "url": "https://www.emol.com", "type": "newspaper", "focus": "general"},
            {"name": "Mining Press Chile", "url": "https://miningpress.com", "type": "industry", "focus": "mining"},
            {"name": "Diario Oficial Chile", "url": "https://www.diariooficial.interior.gob.cl", "type": "gazette", "focus": "regulation"},
            {"name": "Cochilco Reports", "url": "https://www.cochilco.cl", "type": "government", "focus": "copper_statistics"},
        ],
        "alert_triggers": [
            "Producción Codelco mensual (Codelco monthly output)",
            "Negociaciones laborales Escondida (Escondida labor talks)",
            "Nacionalización litio (lithium nationalization policy)",
            "Sequía/restricciones agua Atacama (drought/water restrictions)",
            "Reforma regalías mineras (mining royalty reform)",
            "Sismos en zona minera (earthquakes in mining zone)",
            "Exportaciones cobre/litio (copper/lithium exports)",
            "TC/RC negociaciones anuales (smelter treatment charges)",
        ],
        "key_intelligence": "Chile is #1 copper producer (27% global) and #2 lithium. Codelco (state-owned) faces declining grades and rising costs. Escondida labor negotiations every 3 years can halt 5% of global copper. Lithium nationalization announced 2023 but implementation uncertain. Water scarcity in Atacama is structural constraint on production growth.",
    },

    "BRAZIL": {
        "name": "Corresponsal Brasil",
        "country": "Brazil",
        "capital": "Brasília",
        "lat": -15.7975, "lon": -47.8919,
        "language": "pt",
        "commodities": ["iron_ore", "soybeans", "coffee", "sugar", "corn", "niobium", "oil", "gold"],
        "critical_entities": {
            "mines": ["Carajás/S11D (Vale, iron ore)", "Quadrilátero Ferrífero (Vale, iron ore)", "CBMM (niobium, Araxá)"],
            "companies": ["Vale S.A.", "Petrobras", "Cosan/Raízen (sugar/ethanol)", "Suzano (pulp)", "JBS (beef)"],
            "ports": ["Santos", "Tubarão/Vitória", "Paranaguá", "São Luís"],
            "politicians": ["Presidente", "Ministério de Minas e Energia", "ANM (mining regulator)", "Ibama (environmental)"],
        },
        "news_sources": [
            {"name": "Valor Econômico", "url": "https://valor.globo.com", "type": "business", "focus": "markets/commodities"},
            {"name": "Folha de S.Paulo", "url": "https://www.folha.uol.com.br", "type": "newspaper", "focus": "general"},
            {"name": "Reuters Brasil", "url": "https://www.reuters.com/subjects/brazil", "type": "wire", "focus": "breaking"},
            {"name": "Diário Oficial da União", "url": "https://www.in.gov.br", "type": "gazette", "focus": "regulation"},
            {"name": "NotíciasAgricolas", "url": "https://www.noticiasagricolas.com.br", "type": "industry", "focus": "agriculture"},
        ],
        "alert_triggers": [
            "Produção Vale ferro mensal (Vale monthly iron ore production)",
            "Safra soja/milho (soy/corn harvest reports)",
            "Chuvas Santos/Paranaguá (rain disrupting port logistics)",
            "Barragens/segurança (dam safety — Brumadinho legacy)",
            "BRL desvalorização (Real depreciation — export competitiveness)",
            "Petrobras produção/preço (Petrobras output/pricing)",
            "Desmatamento Amazônia (deforestation sanctions risk)",
            "Exportações China ferro (iron ore exports to China)",
        ],
        "key_intelligence": "Brazil is #1 iron ore, #1 soy, #1 coffee, #1 sugar exporter. Vale is world's largest iron ore producer. Brumadinho dam disaster (2019) permanently changed Brazil mining regulation. BRL weakness = export competitiveness for commodities. Amazon deforestation is ESG risk for soy/beef. Santos port congestion during harvest season directly moves global soy prices.",
    },

    # ═══════ ASIA ════════════════════════════════════════════
    "CHINA": {
        "name": "Corresponsal China",
        "country": "China",
        "capital": "Beijing",
        "lat": 39.9042, "lon": 116.4074,
        "language": "zh",
        "commodities": ["copper", "iron_ore", "coal", "lithium", "aluminum", "nickel", "gold", "rare_earths", "soybeans"],
        "critical_entities": {
            "companies": ["CATL", "BYD", "Baowu Steel", "Jiangxi Copper", "Zijin Mining", "CMOC Group", "Sinopec", "CNOOC"],
            "exchanges": ["SHFE", "DCE", "Zhengzhou Commodity Exchange"],
            "ports": ["Qingdao", "Shanghai/Yangshan", "Ningbo-Zhoushan", "Guangzhou"],
            "institutions": ["PBOC", "NDRC (National Development Reform Commission)", "State Reserve Bureau", "MIIT (Industry Ministry)"],
        },
        "news_sources": [
            {"name": "Shanghai Metals Market (SMM)", "url": "https://www.metal.com", "type": "industry", "focus": "metals_pricing"},
            {"name": "Mysteel", "url": "https://www.mysteel.com", "type": "industry", "focus": "steel/iron_ore"},
            {"name": "South China Morning Post", "url": "https://www.scmp.com", "type": "newspaper", "focus": "business/policy"},
            {"name": "Caixin Global", "url": "https://www.caixinglobal.com", "type": "business", "focus": "economy/markets"},
            {"name": "Xinhua", "url": "https://www.xinhuanet.com", "type": "state_media", "focus": "government_policy"},
            {"name": "Wuxi Stainless Steel Exchange", "url": "N/A", "type": "exchange", "focus": "nickel_stainless"},
        ],
        "alert_triggers": [
            "PBOC rate decisions / RRR cuts (monetary easing)",
            "Property sector stimulus (construction = metals demand)",
            "Strategic reserve releases/purchases (copper, oil, grains)",
            "Export controls on rare earths / critical minerals",
            "SHFE warehouse stock changes (bullish if declining)",
            "PMI manufacturing data (demand indicator)",
            "Smelter maintenance shutdowns (seasonal)",
            "Trade tensions / tariffs on imports",
            "EV sales data monthly (lithium/copper demand)",
        ],
        "key_intelligence": "China consumes 54% of global copper, 70% of iron ore, 60% of coal. PBOC/NDRC policy moves are THE demand signal. SMM/Mysteel are the primary Chinese metals pricing sources — more accurate than LME for Chinese demand. Strategic reserve buying is secretive but trackable via port inventory changes. Property sector = 30% of GDP, directly drives metals demand.",
    },

    "INDONESIA": {
        "name": "Corresponsal Indonesia",
        "country": "Indonesia",
        "capital": "Jakarta",
        "lat": -6.2088, "lon": 106.8456,
        "language": "id",
        "commodities": ["coal", "nickel", "palm_oil", "tin", "copper", "gold", "bauxite"],
        "critical_entities": {
            "mines": ["Grasberg (Freeport, copper/gold)", "Batu Hijau (Amman Mineral)", "Weda Bay (nickel, Tsingshan)"],
            "companies": ["PT Freeport Indonesia", "Adaro Energy (coal)", "Bumi Resources (coal)", "Tsingshan (nickel/steel)"],
            "ports": ["Tanjung Priok (Jakarta)", "Balikpapan (coal)", "Bontang (LNG)"],
            "politicians": ["Presiden", "ESDM (Energy/Mining Ministry)", "BPS (statistics bureau)"],
        },
        "news_sources": [
            {"name": "Jakarta Post", "url": "https://www.thejakartapost.com", "type": "newspaper", "focus": "general"},
            {"name": "Kontan", "url": "https://kontan.co.id", "type": "business", "focus": "markets"},
            {"name": "CNBC Indonesia", "url": "https://www.cnbcindonesia.com", "type": "business", "focus": "economy"},
        ],
        "alert_triggers": [
            "Ore export ban enforcement (nickel, bauxite)",
            "DMO (Domestic Market Obligation) for coal",
            "Nickel smelter investments/capacity (Chinese-funded)",
            "Palm oil export levy/ban changes",
            "Coal production quotas ESDM",
            "Freeport smelter construction updates",
        ],
        "key_intelligence": "Indonesia banned raw nickel ore exports (2020) to force domestic smelting — created global nickel supply shock. Chinese companies (Tsingshan, Huayou) building massive nickel-to-battery processing. Coal export DMO policy can cut exports 20% overnight. Grasberg transitioning to underground = potential output reduction.",
    },

    "AUSTRALIA": {
        "name": "Corresponsal Australia",
        "country": "Australia",
        "capital": "Canberra",
        "lat": -35.2809, "lon": 149.1300,
        "language": "en",
        "commodities": ["iron_ore", "coal", "lithium", "gold", "copper", "nickel", "LNG", "uranium"],
        "critical_entities": {
            "mines": ["Pilbara (BHP/Rio Tinto/FMG, iron ore)", "Greenbushes (lithium)", "Olympic Dam (BHP, copper/uranium)", "Bowen Basin (coal)"],
            "companies": ["BHP", "Rio Tinto", "Fortescue Metals", "Pilbara Minerals", "IGO", "Newcrest/Newmont"],
            "ports": ["Port Hedland", "Gladstone", "Newcastle (coal)", "Dampier"],
            "institutions": ["RBA (Reserve Bank)", "ABARE (agricultural statistics)", "DISR (Dept Industry Science Resources)"],
        },
        "news_sources": [
            {"name": "Australian Financial Review", "url": "https://www.afr.com", "type": "business", "focus": "markets/mining"},
            {"name": "Mining Weekly Australia", "url": "https://www.miningweekly.com", "type": "industry", "focus": "mining"},
            {"name": "The Australian", "url": "https://www.theaustralian.com.au", "type": "newspaper", "focus": "general"},
        ],
        "alert_triggers": [
            "Cyclone warnings Pilbara (iron ore port disruption)",
            "China-Australia trade relations/bans",
            "Lithium spot prices (spodumene)",
            "RBA rate decisions (AUD impact on mining costs)",
            "Port Hedland throughput monthly data",
            "BHP/Rio Tinto quarterly production reports",
        ],
        "key_intelligence": "Australia is #1 iron ore, #1 lithium, #4 gold producer. Pilbara cyclone season (Nov-Mar) disrupts 250Mt+ iron ore exports. China-Australia relations directly affect coal/wine/barley trade (bans lifted 2023-24). AUD weakness = higher AUD-denominated commodity margins for miners.",
    },

    "SOUTH_AFRICA": {
        "name": "Corresponsal Sudáfrica",
        "country": "South Africa",
        "capital": "Pretoria",
        "lat": -25.7461, "lon": 28.1881,
        "language": "en",
        "commodities": ["coal", "platinum", "palladium", "chrome", "manganese", "gold", "iron_ore"],
        "critical_entities": {
            "companies": ["Eskom (utility)", "Transnet (logistics)", "Anglo American Platinum", "Impala Platinum", "Sibanye-Stillwater"],
            "mines": ["Bushveld Complex (PGMs)", "Mpumalanga coalfields", "Sishen (iron ore)"],
            "ports": ["Richards Bay", "Durban"],
            "institutions": ["DMRE (Dept Mineral Resources)", "Eskom", "SARS (tax)"],
        },
        "news_sources": [
            {"name": "Mining Weekly SA", "url": "https://www.miningweekly.com/page/sa-news", "type": "industry", "focus": "mining"},
            {"name": "Business Day", "url": "https://www.businesslive.co.za", "type": "business", "focus": "economy"},
            {"name": "TimesLive", "url": "https://www.timeslive.co.za", "type": "newspaper", "focus": "general"},
        ],
        "alert_triggers": [
            "Load shedding stages (Eskom power cuts)",
            "Transnet rail derailments/strikes",
            "Platinum/palladium price vs production cost",
            "Mining charter/BEE policy changes",
            "Richards Bay coal terminal utilization",
            "ZAR depreciation (mining cost impact)",
        ],
        "key_intelligence": "South Africa produces 70% of global platinum, 35% of palladium, 80% of chrome. Eskom load shedding (power cuts) directly reduces mining output 10-30%. Transnet rail failures are chronic — coal exports running at 50-60% capacity. PGM sector under pressure from EV transition (less catalytic converter demand).",
    },

    "DRC": {
        "name": "Corresponsal RD Congo",
        "country": "Democratic Republic of Congo",
        "capital": "Kinshasa",
        "lat": -4.4419, "lon": 15.2663,
        "language": "fr",
        "commodities": ["cobalt", "copper", "tin", "tantalum", "gold"],
        "critical_entities": {
            "mines": ["Kamoa-Kakula (Ivanhoe/Zijin, copper)", "Mutanda (Glencore, cobalt)", "Tenke Fungurume (CMOC, copper/cobalt)", "Kolwezi region (artisanal cobalt)"],
            "companies": ["Gécamines (state)", "CMOC Group", "Glencore DRC", "Ivanhoe Mines", "Zijin Mining"],
            "ports": ["Dar es Salaam (Tanzania, transit)", "Durban (SA, transit)", "Lobito corridor (Angola, new)"],
        },
        "news_sources": [
            {"name": "Mining Review Africa", "url": "https://www.miningreview.com", "type": "industry", "focus": "mining"},
            {"name": "RFI Afrique", "url": "https://www.rfi.fr/afrique", "type": "news", "focus": "francophone_africa"},
            {"name": "7sur7.cd", "url": "https://7sur7.cd", "type": "local_news", "focus": "DRC_general"},
        ],
        "alert_triggers": [
            "M23 conflict / eastern DRC instability",
            "Mining code enforcement (royalty increases)",
            "Artisanal mining accidents/child labor reports (ESG risk)",
            "Cobalt export tax changes",
            "Kamoa-Kakula production ramp-up reports",
            "Lobito corridor railway construction progress",
            "Chinese-DRC mining deal renegotiations",
        ],
        "key_intelligence": "DRC produces 70% of global cobalt and is becoming a top-5 copper producer. Artisanal mining (ASM) supplies 15-20% of cobalt — ESG risk for EV supply chain. Kamoa-Kakula is fastest-growing copper mine globally. Lobito corridor (Angola-DRC railway) backed by US to counter Chinese logistics dominance. Gécamines (state miner) regularly renegotiates JV terms with foreign miners.",
    },

    "RUSSIA": {
        "name": "Corresponsal Rusia",
        "country": "Russia",
        "capital": "Moscow",
        "lat": 55.7558, "lon": 37.6173,
        "language": "ru",
        "commodities": ["oil", "natural_gas", "aluminum", "nickel", "palladium", "wheat", "coal", "fertilizer", "uranium"],
        "critical_entities": {
            "companies": ["Gazprom", "Rosneft", "Rusal", "Norilsk Nickel", "ALROSA (diamonds)", "Uralkali (potash)"],
            "ports": ["Novorossiysk (oil)", "Primorsk (oil)", "Murmansk", "Vladivostok (Pacific coal)"],
            "institutions": ["Central Bank of Russia", "Energy Ministry", "Rosnedra (mineral resources)"],
        },
        "news_sources": [
            {"name": "TASS", "url": "https://tass.com", "type": "state_media", "focus": "official"},
            {"name": "Interfax Russia", "url": "https://www.interfax.ru", "type": "wire", "focus": "energy/commodities"},
            {"name": "The Moscow Times", "url": "https://www.themoscowtimes.com", "type": "newspaper", "focus": "business"},
        ],
        "alert_triggers": [
            "Sanctions enforcement changes (G7, EU)",
            "OPEC+ production quotas (Russia compliance)",
            "Pipeline gas flows to Europe (Nord Stream status)",
            "Rusal/Norilsk Nickel production reports",
            "Wheat export quotas/bans",
            "Arctic shipping route openings (seasonal)",
            "Ruble exchange rate movements",
            "LNG project timelines (Arctic LNG 2, Sakhalin)",
        ],
        "key_intelligence": "Russia is #2 oil, #2 natural gas, #2 aluminum, #1 palladium, #1 wheat exporter. Post-2022 sanctions redirected Russian oil to China/India at discounts. Rusal under sanctions risk = aluminum supply uncertainty. Norilsk Nickel = 10% global nickel + 40% palladium. Russian wheat export bans/quotas directly spike global food prices.",
    },

    "SAUDI_ARABIA": {
        "name": "Corresponsal Arabia Saudita",
        "country": "Saudi Arabia",
        "capital": "Riyadh",
        "lat": 24.7136, "lon": 46.6753,
        "language": "ar",
        "commodities": ["oil", "natural_gas", "petrochemicals", "phosphate", "gold"],
        "critical_entities": {
            "companies": ["Saudi Aramco", "SABIC (petrochemicals)", "Ma'aden (mining)", "ACWA Power"],
            "ports": ["Ras Tanura (oil)", "Yanbu", "Jeddah"],
            "institutions": ["MoEP (Energy Ministry)", "OPEC Secretariat influence"],
        },
        "news_sources": [
            {"name": "Argaam", "url": "https://www.argaam.com", "type": "business", "focus": "markets"},
            {"name": "Arab News", "url": "https://www.arabnews.com", "type": "newspaper", "focus": "general"},
            {"name": "Saudi Gazette", "url": "https://saudigazette.com.sa", "type": "newspaper", "focus": "general"},
        ],
        "alert_triggers": [
            "OPEC+ meetings / Saudi production decisions",
            "Aramco OSP (Official Selling Price) changes",
            "Yemen/Houthi attacks on infrastructure",
            "Vision 2030 mining expansion projects",
            "Saudi-Iran diplomatic status",
            "Spare capacity utilization rate",
        ],
        "key_intelligence": "Saudi Arabia is OPEC's swing producer — production decisions set global oil prices. Aramco OSP (Official Selling Price) monthly changes are THE oil market signal. Saudi spare capacity <2 Mbpd = geopolitical risk premium. Vision 2030 diversification into mining (Ma'aden phosphate, gold) is expanding commodity footprint.",
    },
}


# ═══════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def get_correspondent_for_country(country: str) -> Optional[Dict[str, Any]]:
    """Get correspondent config for a specific country."""
    country_upper = country.upper().replace(" ", "_")
    return CORRESPONDENTS.get(country_upper)


def get_correspondents_for_commodity(commodity: str) -> List[Dict[str, Any]]:
    """Get all correspondents relevant to a commodity."""
    commodity_lower = commodity.lower().replace("_", " ")
    relevant = []
    for code, corr in CORRESPONDENTS.items():
        for comm in corr["commodities"]:
            if commodity_lower in comm or comm in commodity_lower:
                relevant.append({"country_code": code, **corr})
                break
    return relevant


def build_correspondent_prompt(country_code: str) -> str:
    """
    Build an intelligence prompt for a specific country's correspondent.
    This is injected into agent prompts to give them local context.
    """
    corr = CORRESPONDENTS.get(country_code.upper())
    if not corr:
        return ""

    parts = [
        f"LOCAL INTELLIGENCE — {corr['country']} ({corr['language'].upper()})",
        f"COMMODITIES: {', '.join(corr['commodities'])}",
    ]

    # Critical entities
    entities = corr.get("critical_entities", {})
    if "mines" in entities:
        parts.append(f"KEY MINES: {', '.join(entities['mines'][:4])}")
    if "companies" in entities:
        parts.append(f"KEY COMPANIES: {', '.join(entities['companies'][:4])}")
    if "ports" in entities:
        parts.append(f"PORTS: {', '.join(entities['ports'][:3])}")

    # Alert triggers
    triggers = corr.get("alert_triggers", [])
    if triggers:
        parts.append(f"WATCH FOR: {'; '.join(triggers[:4])}")

    # Key intelligence
    if "key_intelligence" in corr:
        parts.append(f"CONTEXT: {corr['key_intelligence'][:200]}")

    # News sources for reference
    sources = corr.get("news_sources", [])
    if sources:
        source_names = ", ".join(s["name"] for s in sources[:3])
        parts.append(f"LOCAL SOURCES: {source_names}")

    return "\n".join(parts)


def build_commodity_correspondent_prompt(commodity: str) -> str:
    """
    Build a multi-country correspondent prompt for a commodity.
    Used to inject producer-country intelligence into analysis.
    """
    correspondents = get_correspondents_for_commodity(commodity)
    if not correspondents:
        return ""

    sections = [f"CORRESPONDENT NETWORK — {commodity.upper()} intelligence from {len(correspondents)} countries:"]
    for corr in correspondents[:4]:  # Limit to top 4 to save tokens
        country = corr["country"]
        key_intel = corr.get("key_intelligence", "")[:150]
        triggers = corr.get("alert_triggers", [])[:3]
        trigger_str = "; ".join(triggers) if triggers else "N/A"
        sections.append(f"  [{country}] {key_intel}... WATCH: {trigger_str}")

    return "\n".join(sections)


def get_all_correspondents_summary() -> List[Dict[str, Any]]:
    """Return summary of all correspondents for the API."""
    return [
        {
            "country_code": code,
            "country": c["country"],
            "capital": c["capital"],
            "lat": c["lat"],
            "lon": c["lon"],
            "language": c["language"],
            "commodities": c["commodities"],
            "num_sources": len(c.get("news_sources", [])),
            "num_triggers": len(c.get("alert_triggers", [])),
        }
        for code, c in CORRESPONDENTS.items()
    ]
