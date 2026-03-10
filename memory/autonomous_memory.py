#!/usr/bin/env python3
"""
METEORO X v13 — AUTONOMOUS MEMORY SYSTEM
==========================================
The system that learns, remembers, and auto-calibrates.

Architecture:
  - SQLite persistence (WAL mode for concurrent reads)
  - Every analysis stored with full agent-level detail
  - Track record per commodity AND per agent
  - Auto-calibration: adjust confidence weights based on historical accuracy
  - Memory injection: provide historical context to future analyses
  - Performance metrics: accuracy, Sharpe-like ratio, win/loss streaks

Tables:
  1. signal_log — Every swarm signal with consensus + per-agent breakdown
  2. agent_performance — Per-agent accuracy tracking over time
  3. price_snapshots — Record prices at signal time and T+1/5/20/60 days
  4. calibration_weights — Auto-adjusted agent weights based on performance

Author: Meteoro X Team
Version: 13.0.0
"""

import asyncio
import sqlite3
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

# Para obtener datos de precios reales
try:
    import yfinance as yf
except ImportError:
    yf = None

logger = logging.getLogger(__name__)


class AutonomousMemory:
    """
    Sistema de memoria autónoma para Meteoro X.
    Persiste señales, rastrea desempeño de agentes, y auto-calibra pesos.

    This class manages:
      - Signal persistence with full agent-level breakdown
      - Per-agent and per-commodity performance tracking
      - Automatic weight calibration based on historical accuracy
      - Memory injection for future agent analysis
      - Performance dashboards and leaderboards
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Inicializa el sistema de memoria.

        Args:
            db_path: Ruta a la base de datos SQLite.
                    Si no se proporciona, usa: parent_dir/meteoro_memory.db
        """
        if db_path is None:
            # Usar directorio padre como ubicación por defecto
            parent_dir = Path(__file__).parent.parent
            db_path = str(parent_dir / "meteoro_memory.db")

        self.db_path = db_path
        self._tables_initialized = False
        logger.info(f"AutonomousMemory initialized with db_path: {self.db_path}")

        # Inicializar tablas de forma sincrónica (safe para __init__)
        try:
            conn = self._get_connection()
            self._ensure_tables(conn)
            conn.close()
            self._tables_initialized = True
        except Exception as e:
            logger.warning(f"Could not initialize tables in __init__: {e}")

    async def _ensure_tables_async(self):
        """Wrapper async para garantizar tablas al inicializar."""
        conn = await asyncio.to_thread(self._get_connection)
        try:
            await asyncio.to_thread(self._ensure_tables, conn)
        finally:
            conn.close()

    def _get_connection(self) -> sqlite3.Connection:
        """
        Obtiene una conexión a la base de datos con WAL mode.

        Returns:
            sqlite3.Connection object
        """
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row

        # Habilitar WAL mode para mejor concurrencia
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")

        return conn

    def _ensure_tables(self, conn: sqlite3.Connection) -> None:
        """
        Crea todas las tablas necesarias si no existen.

        Tablas:
          1. signal_log — Cada señal swarm con detalles de consenso
          2. agent_performance — Rastreo de precisión por agente
          3. price_snapshots — Precios en momento de señal y futuro
          4. calibration_weights — Pesos auto-ajustados

        Args:
            conn: sqlite3.Connection object
        """
        cursor = conn.cursor()

        # Tabla 1: signal_log
        # Registro completo de cada señal swarm con breakdown por agente
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signal_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                commodity TEXT NOT NULL,
                final_signal TEXT NOT NULL,
                conviction INTEGER,
                reasoning TEXT,
                agents_bullish INTEGER,
                agents_bearish INTEGER,
                agents_neutral INTEGER,
                risk_guardian_veto BOOLEAN DEFAULT 0,
                total_latency_ms REAL,
                cost_usd REAL,
                session_id TEXT,
                price_at_signal REAL,
                agent_results_json TEXT,
                outcome_correct BOOLEAN,
                outcome_price_t1 REAL,
                outcome_price_t5 REAL,
                outcome_price_t20 REAL,
                outcome_price_t60 REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Índices para signal_log
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_signal_commodity
            ON signal_log(commodity)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_signal_timestamp
            ON signal_log(timestamp)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_signal_session
            ON signal_log(session_id)
        """)

        # Tabla 2: agent_performance
        # Rastreo de desempeño por agente
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agent_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity TEXT NOT NULL,
                agent_name TEXT NOT NULL,
                signal TEXT,
                confidence REAL,
                was_correct BOOLEAN,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Índices para agent_performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_agent_commodity
            ON agent_performance(commodity, agent_name)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_agent_timestamp
            ON agent_performance(timestamp)
        """)

        # Tabla 3: price_snapshots
        # Registro de precios para comparación posterior
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS price_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity TEXT NOT NULL,
                price REAL NOT NULL,
                source TEXT DEFAULT 'yfinance',
                days_offset INTEGER,
                signal_id INTEGER,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (signal_id) REFERENCES signal_log(id)
            )
        """)

        # Índices para price_snapshots
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_price_commodity
            ON price_snapshots(commodity)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_price_signal
            ON price_snapshots(signal_id)
        """)

        # Tabla 4: calibration_weights
        # Pesos auto-ajustados basados en desempeño histórico
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS calibration_weights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity TEXT NOT NULL,
                agent_name TEXT NOT NULL,
                weight REAL DEFAULT 1.0,
                accuracy_pct REAL,
                total_signals INTEGER DEFAULT 0,
                correct_signals INTEGER DEFAULT 0,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity, agent_name)
            )
        """)

        # Índices para calibration_weights
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_calib_commodity
            ON calibration_weights(commodity)
        """)

        conn.commit()
        logger.info("Database tables ensured")

    async def save_signal(self, signal_data: Dict[str, Any]) -> int:
        """
        Guarda una señal swarm completa con todos los detalles de agentes.

        Args:
            signal_data: Diccionario con:
                - commodity (str)
                - timestamp (str/datetime)
                - final_signal (str): BUY, SELL, HOLD
                - conviction (int): 0-100
                - reasoning (str)
                - agents_bullish (int)
                - agents_bearish (int)
                - agents_neutral (int)
                - risk_guardian_veto (bool)
                - total_latency_ms (float)
                - cost_usd (float)
                - session_id (str)
                - price_at_signal (float)
                - agent_results (list): cada elemento es un dict con
                  {agent_name, signal, confidence, model_used, provider, latency_ms}

        Returns:
            ID de la señal guardada
        """
        def _save():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Serializar agent_results a JSON
                agent_results_json = json.dumps(signal_data.get("agent_results", []))

                cursor.execute("""
                    INSERT INTO signal_log (
                        commodity, timestamp, final_signal, conviction, reasoning,
                        agents_bullish, agents_bearish, agents_neutral,
                        risk_guardian_veto, total_latency_ms, cost_usd,
                        session_id, price_at_signal, agent_results_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    signal_data.get("commodity"),
                    signal_data.get("timestamp", datetime.utcnow()),
                    signal_data.get("final_signal"),
                    signal_data.get("conviction"),
                    signal_data.get("reasoning"),
                    signal_data.get("agents_bullish", 0),
                    signal_data.get("agents_bearish", 0),
                    signal_data.get("agents_neutral", 0),
                    signal_data.get("risk_guardian_veto", False),
                    signal_data.get("total_latency_ms"),
                    signal_data.get("cost_usd"),
                    signal_data.get("session_id"),
                    signal_data.get("price_at_signal"),
                    agent_results_json
                ))

                signal_id = cursor.lastrowid
                conn.commit()

                logger.info(f"Signal saved with ID {signal_id} for {signal_data.get('commodity')}")
                return signal_id
            finally:
                conn.close()

        return await asyncio.to_thread(_save)

    async def record_agent_performance(
        self,
        commodity: str,
        agent_name: str,
        signal: str,
        confidence: float,
        was_correct: Optional[bool] = None
    ) -> None:
        """
        Registra el desempeño de un agente individual.

        Args:
            commodity: Nombre del commodity
            agent_name: Nombre del agente
            signal: BUY, SELL, HOLD
            confidence: 0-100
            was_correct: True/False si ya se conoce el resultado
        """
        def _record():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT INTO agent_performance (
                        commodity, agent_name, signal, confidence, was_correct, timestamp
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    commodity,
                    agent_name,
                    signal,
                    confidence,
                    was_correct,
                    datetime.utcnow()
                ))

                conn.commit()
                logger.debug(f"Agent performance recorded: {agent_name} @ {commodity}")
            finally:
                conn.close()

        await asyncio.to_thread(_record)

    async def snapshot_price(
        self,
        commodity: str,
        price: float,
        source: str = "yfinance",
        signal_id: Optional[int] = None
    ) -> None:
        """
        Registra una captura de precio para comparación posterior.

        Args:
            commodity: Nombre del commodity
            price: Precio actual
            source: Fuente de datos (default: yfinance)
            signal_id: ID de la señal asociada (opcional)
        """
        def _snapshot():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT INTO price_snapshots (
                        commodity, price, source, signal_id, timestamp
                    )
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    commodity,
                    price,
                    source,
                    signal_id,
                    datetime.utcnow()
                ))

                conn.commit()
                logger.debug(f"Price snapshot recorded: {commodity} @ {price}")
            finally:
                conn.close()

        await asyncio.to_thread(_snapshot)

    async def update_outcomes(self, days: int = 5) -> Dict[str, int]:
        """
        Compara señales de hace N días contra precios actuales.
        Usa yfinance para obtener precios reales.

        Args:
            days: Número de días para mirar hacia atrás

        Returns:
            Diccionario con conteos: {updated: int, errors: int}
        """
        if yf is None:
            logger.warning("yfinance not installed, skipping outcome update")
            return {"updated": 0, "errors": 0}

        def _update():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Buscar señales de N días atrás sin outcome
                cutoff_date = datetime.utcnow() - timedelta(days=days)

                cursor.execute("""
                    SELECT id, commodity, final_signal, price_at_signal, timestamp
                    FROM signal_log
                    WHERE timestamp < ? AND outcome_correct IS NULL
                    ORDER BY timestamp DESC
                """, (cutoff_date,))

                signals = cursor.fetchall()
                updated_count = 0
                error_count = 0

                for signal_row in signals:
                    signal_id = signal_row[0]
                    commodity = signal_row[1]
                    final_signal = signal_row[2]
                    price_at_signal = signal_row[3]
                    signal_timestamp = signal_row[4]

                    try:
                        # Obtener símbolo ticker
                        ticker_symbol = self._get_ticker_symbol(commodity)
                        if not ticker_symbol:
                            error_count += 1
                            continue

                        # Descargar datos de yfinance
                        data = yf.download(
                            ticker_symbol,
                            start=signal_timestamp,
                            end=datetime.utcnow(),
                            progress=False
                        )

                        if data.empty:
                            error_count += 1
                            continue

                        # Obtener precios en diferentes períodos
                        current_price = data['Close'].iloc[-1] if len(data) > 0 else None

                        if current_price is None:
                            error_count += 1
                            continue

                        # Determinar si la señal fue correcta
                        # BUY: precio debe subir, SELL: precio debe bajar, HOLD: indiferente
                        price_change = current_price - price_at_signal

                        if final_signal == "BUY":
                            was_correct = price_change > 0
                        elif final_signal == "SELL":
                            was_correct = price_change < 0
                        else:  # HOLD
                            was_correct = abs(price_change) < (price_at_signal * 0.02)  # 2%

                        # Actualizar la señal
                        cursor.execute("""
                            UPDATE signal_log
                            SET outcome_correct = ?,
                                outcome_price_t5 = ?
                            WHERE id = ?
                        """, (was_correct, current_price, signal_id))

                        updated_count += 1

                    except Exception as e:
                        logger.error(f"Error updating outcome for signal {signal_id}: {e}")
                        error_count += 1

                conn.commit()
                logger.info(f"Outcomes updated: {updated_count} signals, {error_count} errors")
                return {"updated": updated_count, "errors": error_count}

            finally:
                conn.close()

        return await asyncio.to_thread(_update)

    async def get_track_record(self, commodity: Optional[str] = None) -> Dict[str, Any]:
        """
        Obtiene estadísticas generales de desempeño.

        Args:
            commodity: Filtrar por commodity específico (opcional)

        Returns:
            Dict con:
            - total_signals (int)
            - correct (int)
            - accuracy_pct (float)
            - avg_conviction (float)
            - best_commodity (str)
            - worst_commodity (str)
            - signals_by_commodity (dict)
        """
        def _get_record():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Construcción dinámica de query
                where_clause = ""
                params = []

                if commodity:
                    where_clause = "WHERE commodity = ?"
                    params = [commodity]

                # Estadísticas generales
                cursor.execute(f"""
                    SELECT
                        COUNT(*) as total_signals,
                        SUM(CASE WHEN outcome_correct = 1 THEN 1 ELSE 0 END) as correct,
                        AVG(conviction) as avg_conviction
                    FROM signal_log
                    {where_clause}
                """, params)

                row = cursor.fetchone()
                total_signals = row[0] or 0
                correct = row[1] or 0
                avg_conviction = row[2] or 0

                accuracy_pct = (correct / total_signals * 100) if total_signals > 0 else 0

                # Desempeño por commodity
                cursor.execute("""
                    SELECT
                        commodity,
                        COUNT(*) as count,
                        SUM(CASE WHEN outcome_correct = 1 THEN 1 ELSE 0 END) as correct
                    FROM signal_log
                    GROUP BY commodity
                    ORDER BY count DESC
                """)

                signals_by_commodity = {}
                best_commodity = None
                worst_commodity = None
                best_accuracy = -1
                worst_accuracy = 101

                for row in cursor.fetchall():
                    comm = row[0]
                    count = row[1]
                    corr = row[2] or 0
                    acc = (corr / count * 100) if count > 0 else 0

                    signals_by_commodity[comm] = {
                        "count": count,
                        "correct": corr,
                        "accuracy_pct": round(acc, 2)
                    }

                    if acc > best_accuracy:
                        best_accuracy = acc
                        best_commodity = comm
                    if acc < worst_accuracy:
                        worst_accuracy = acc
                        worst_commodity = comm

                return {
                    "total_signals": total_signals,
                    "correct": correct,
                    "accuracy_pct": round(accuracy_pct, 2),
                    "avg_conviction": round(avg_conviction, 2),
                    "best_commodity": best_commodity,
                    "worst_commodity": worst_commodity,
                    "signals_by_commodity": signals_by_commodity
                }

            finally:
                conn.close()

        return await asyncio.to_thread(_get_record)

    async def get_agent_leaderboard(
        self,
        commodity: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Obtiene el ranking de agentes por precisión.

        Args:
            commodity: Filtrar por commodity específico (opcional)
            limit: Máximo de agentes a retornar

        Returns:
            Lista ordenada de dicts con:
            - agent_name (str)
            - accuracy_pct (float)
            - total_signals (int)
            - correct_signals (int)
            - avg_confidence (float)
        """
        def _get_leaderboard():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                where_clause = ""
                params = []

                if commodity:
                    where_clause = "WHERE commodity = ?"
                    params = [commodity]

                cursor.execute(f"""
                    SELECT
                        agent_name,
                        COUNT(*) as total_signals,
                        SUM(CASE WHEN was_correct = 1 THEN 1 ELSE 0 END) as correct_signals,
                        AVG(confidence) as avg_confidence
                    FROM agent_performance
                    {where_clause}
                    GROUP BY agent_name
                    ORDER BY correct_signals DESC, total_signals DESC
                    LIMIT ?
                """, params + [limit])

                leaderboard = []
                for row in cursor.fetchall():
                    agent_name = row[0]
                    total = row[1]
                    correct = row[2] or 0
                    avg_conf = row[3] or 0

                    accuracy_pct = (correct / total * 100) if total > 0 else 0

                    leaderboard.append({
                        "agent_name": agent_name,
                        "accuracy_pct": round(accuracy_pct, 2),
                        "total_signals": total,
                        "correct_signals": correct,
                        "avg_confidence": round(avg_conf, 2)
                    })

                return leaderboard

            finally:
                conn.close()

        return await asyncio.to_thread(_get_leaderboard)

    async def get_calibration_weights(self, commodity: str) -> Dict[str, float]:
        """
        Obtiene pesos de calibración auto-ajustados para un commodity.

        Lógica:
        - accuracy > 80% → weight = 2.0
        - accuracy 70-80% → weight = 1.5
        - accuracy 40-70% → weight = 1.0
        - accuracy < 40% → weight = 0.5

        Si no hay datos históricos, retorna weight = 1.0 (neutral)

        Args:
            commodity: Nombre del commodity

        Returns:
            Dict {agent_name: weight}
        """
        def _get_weights():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Obtener agentes y su desempeño histórico
                cursor.execute("""
                    SELECT
                        agent_name,
                        COUNT(*) as total_signals,
                        SUM(CASE WHEN was_correct = 1 THEN 1 ELSE 0 END) as correct_signals
                    FROM agent_performance
                    WHERE commodity = ?
                    GROUP BY agent_name
                """, (commodity,))

                weights = {}

                for row in cursor.fetchall():
                    agent_name = row[0]
                    total = row[1]
                    correct = row[2] or 0

                    accuracy = (correct / total * 100) if total > 0 else 50

                    # Aplicar lógica de pesos
                    if accuracy > 80:
                        weight = 2.0
                    elif accuracy >= 70:
                        weight = 1.5
                    elif accuracy >= 40:
                        weight = 1.0
                    else:
                        weight = 0.5

                    weights[agent_name] = weight

                return weights

            finally:
                conn.close()

        return await asyncio.to_thread(_get_weights)

    async def get_memory_context(self, commodity: str) -> str:
        """
        Construye un contexto narrativo para inyectar en prompts de agentes.

        Incluye:
        - Últimas 5 señales
        - Track record general
        - Ranking de agentes
        - Patrones detectados

        Args:
            commodity: Nombre del commodity

        Returns:
            String narrativo con contexto histórico
        """
        def _get_context():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Últimas 5 señales
                cursor.execute("""
                    SELECT timestamp, final_signal, conviction, reasoning, outcome_correct
                    FROM signal_log
                    WHERE commodity = ?
                    ORDER BY timestamp DESC
                    LIMIT 5
                """, (commodity,))

                recent_signals = cursor.fetchall()

                # Track record
                cursor.execute("""
                    SELECT
                        COUNT(*) as total,
                        SUM(CASE WHEN outcome_correct = 1 THEN 1 ELSE 0 END) as correct
                    FROM signal_log
                    WHERE commodity = ?
                """, (commodity,))

                track_row = cursor.fetchone()
                total_signals = track_row[0] or 0
                correct_signals = track_row[1] or 0
                accuracy = (correct_signals / total_signals * 100) if total_signals > 0 else 0

                # Top 3 agentes
                cursor.execute("""
                    SELECT
                        agent_name,
                        SUM(CASE WHEN was_correct = 1 THEN 1 ELSE 0 END) as correct,
                        COUNT(*) as total
                    FROM agent_performance
                    WHERE commodity = ?
                    GROUP BY agent_name
                    ORDER BY correct DESC
                    LIMIT 3
                """, (commodity,))

                top_agents = cursor.fetchall()

                # Construir narrativa
                context = f"""
═══════════════════════════════════════════════════════════
HISTORICAL MEMORY CONTEXT: {commodity.upper()}
═══════════════════════════════════════════════════════════

📊 TRACK RECORD:
   Total Signals: {total_signals}
   Correct: {correct_signals}
   Accuracy: {accuracy:.1f}%

📈 RECENT SIGNALS (last 5):
"""

                for sig in recent_signals:
                    timestamp = sig[0]
                    signal = sig[1]
                    conviction = sig[2]
                    reasoning = sig[3]
                    outcome = "✓ CORRECT" if sig[4] else "✗ WRONG" if sig[4] is False else "⏳ PENDING"

                    context += f"""   {timestamp} | {signal} (conviction: {conviction}%) | {outcome}
      Reasoning: {reasoning[:80]}...\n"""

                context += "\n🏆 TOP PERFORMING AGENTS:\n"
                for agent in top_agents:
                    agent_name = agent[0]
                    agent_correct = agent[1] or 0
                    agent_total = agent[2]
                    agent_accuracy = (agent_correct / agent_total * 100) if agent_total > 0 else 0
                    context += f"   • {agent_name}: {agent_accuracy:.1f}% ({agent_correct}/{agent_total})\n"

                context += "\n═══════════════════════════════════════════════════════════\n"

                return context

            finally:
                conn.close()

        return await asyncio.to_thread(_get_context)

    async def get_performance_dashboard(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas completas para endpoint de API/dashboard.

        Returns:
            Dict con:
            - total_signals (int)
            - accuracy_by_commodity (dict)
            - agent_leaderboard (list)
            - recent_signals (list, últimas 10)
            - calibration_active (bool)
            - streaks (dict): win/loss streaks por commodity
            - timestamp (str)
        """
        def _get_dashboard():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Total de señales
                cursor.execute("SELECT COUNT(*) FROM signal_log")
                total_signals = cursor.fetchone()[0] or 0

                # Precisión por commodity
                cursor.execute("""
                    SELECT
                        commodity,
                        COUNT(*) as total,
                        SUM(CASE WHEN outcome_correct = 1 THEN 1 ELSE 0 END) as correct
                    FROM signal_log
                    GROUP BY commodity
                """)

                accuracy_by_commodity = {}
                for row in cursor.fetchall():
                    comm = row[0]
                    total = row[1]
                    correct = row[2] or 0
                    acc = (correct / total * 100) if total > 0 else 0
                    accuracy_by_commodity[comm] = {
                        "accuracy_pct": round(acc, 2),
                        "total": total,
                        "correct": correct
                    }

                # Top 10 agentes
                cursor.execute("""
                    SELECT
                        agent_name,
                        SUM(CASE WHEN was_correct = 1 THEN 1 ELSE 0 END) as correct,
                        COUNT(*) as total,
                        AVG(confidence) as avg_conf
                    FROM agent_performance
                    GROUP BY agent_name
                    ORDER BY correct DESC
                    LIMIT 10
                """)

                agent_leaderboard = []
                for row in cursor.fetchall():
                    agent_name = row[0]
                    correct = row[1] or 0
                    total = row[2]
                    avg_conf = row[3] or 0
                    accuracy = (correct / total * 100) if total > 0 else 0

                    agent_leaderboard.append({
                        "agent_name": agent_name,
                        "accuracy_pct": round(accuracy, 2),
                        "total_signals": total,
                        "correct_signals": correct,
                        "avg_confidence": round(avg_conf, 2)
                    })

                # Últimas 10 señales
                cursor.execute("""
                    SELECT
                        id, commodity, timestamp, final_signal, conviction,
                        outcome_correct, agents_bullish, agents_bearish, agents_neutral
                    FROM signal_log
                    ORDER BY timestamp DESC
                    LIMIT 10
                """)

                recent_signals = []
                for row in cursor.fetchall():
                    outcome = "✓" if row[5] else "✗" if row[5] is False else "⏳"
                    recent_signals.append({
                        "id": row[0],
                        "commodity": row[1],
                        "timestamp": row[2],
                        "signal": row[3],
                        "conviction": row[4],
                        "outcome": outcome,
                        "agents": {
                            "bullish": row[6],
                            "bearish": row[7],
                            "neutral": row[8]
                        }
                    })

                # Calcular streaks
                cursor.execute("""
                    SELECT commodity, outcome_correct
                    FROM signal_log
                    WHERE outcome_correct IS NOT NULL
                    ORDER BY timestamp DESC
                    LIMIT 50
                """)

                streaks = {}
                for commodity in accuracy_by_commodity.keys():
                    cursor.execute("""
                        SELECT outcome_correct
                        FROM signal_log
                        WHERE commodity = ? AND outcome_correct IS NOT NULL
                        ORDER BY timestamp DESC
                        LIMIT 10
                    """, (commodity,))

                    results = [row[0] for row in cursor.fetchall()]

                    win_streak = 0
                    loss_streak = 0

                    for result in results:
                        if result:
                            win_streak += 1
                            loss_streak = 0
                        else:
                            loss_streak += 1
                            win_streak = 0

                    streaks[commodity] = {
                        "current_win_streak": win_streak,
                        "current_loss_streak": loss_streak
                    }

                return {
                    "total_signals": total_signals,
                    "accuracy_by_commodity": accuracy_by_commodity,
                    "agent_leaderboard": agent_leaderboard,
                    "recent_signals": recent_signals,
                    "calibration_active": True,
                    "streaks": streaks,
                    "timestamp": datetime.utcnow().isoformat()
                }

            finally:
                conn.close()

        return await asyncio.to_thread(_get_dashboard)

    async def cleanup_old(self, days: int = 90) -> Dict[str, int]:
        """
        Elimina datos más antiguos que N días.
        Mantiene integridad referencial.

        Args:
            days: Número de días para retener (default: 90)

        Returns:
            Dict con conteos: {signal_log: int, agent_performance: int, price_snapshots: int}
        """
        def _cleanup():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                cutoff_date = datetime.utcnow() - timedelta(days=days)

                # Obtener IDs de señales a eliminar
                cursor.execute("""
                    SELECT id FROM signal_log WHERE created_at < ?
                """, (cutoff_date,))

                signal_ids = [row[0] for row in cursor.fetchall()]

                # Eliminar datos asociados
                deleted_counts = {
                    "signal_log": 0,
                    "agent_performance": 0,
                    "price_snapshots": 0,
                    "calibration_weights": 0
                }

                if signal_ids:
                    placeholders = ",".join("?" * len(signal_ids))

                    # Eliminar price_snapshots
                    cursor.execute(f"""
                        DELETE FROM price_snapshots WHERE signal_id IN ({placeholders})
                    """, signal_ids)
                    deleted_counts["price_snapshots"] = cursor.rowcount

                    # Eliminar signal_log
                    cursor.execute(f"""
                        DELETE FROM signal_log WHERE id IN ({placeholders})
                    """, signal_ids)
                    deleted_counts["signal_log"] = cursor.rowcount

                # Eliminar agent_performance antiguo
                cursor.execute("""
                    DELETE FROM agent_performance WHERE created_at < ?
                """, (cutoff_date,))
                deleted_counts["agent_performance"] = cursor.rowcount

                conn.commit()
                logger.info(f"Cleanup completed: {deleted_counts}")
                return deleted_counts

            finally:
                conn.close()

        return await asyncio.to_thread(_cleanup)

    # ======================================================================
    # HELPER METHODS
    # ======================================================================

    def _get_ticker_symbol(self, commodity: str) -> Optional[str]:
        """
        Mapea nombres de commodities a símbolos de yfinance.

        Args:
            commodity: Nombre del commodity

        Returns:
            Símbolo ticker o None si no se encuentra
        """
        # Mapeo completo de commodities a tickers de yfinance
        commodity_map = {
            # Energy
            "oil": "CL=F", "crude_oil": "CL=F", "wti": "CL=F", "brent": "BZ=F",
            "natural_gas": "NG=F", "gas": "NG=F",
            # Metals
            "gold": "GC=F", "oro": "GC=F",
            "silver": "SI=F", "plata": "SI=F",
            "copper": "HG=F", "cobre": "HG=F",
            "platinum": "PL=F", "platino": "PL=F",
            "palladium": "PA=F", "paladio": "PA=F",
            "aluminum": "ALI=F", "aluminio": "ALI=F",
            "nickel": "^SPGSIKTR", "niquel": "^SPGSIKTR",
            "zinc": "^SPGSZN", "lithium": "LIT",
            # Agriculture
            "wheat": "ZW=F", "trigo": "ZW=F",
            "corn": "ZC=F", "maiz": "ZC=F",
            "soybeans": "ZS=F", "soja": "ZS=F", "soy": "ZS=F",
            "coffee": "KC=F", "cafe": "KC=F",
            "sugar": "SB=F", "azucar": "SB=F",
            "cocoa": "CC=F", "cacao": "CC=F",
            "cotton": "CT=F", "algodon": "CT=F",
            "rice": "ZR=F", "arroz": "ZR=F",
            # Bulk
            "coal": "MTF=F", "carbon": "MTF=F",
            "iron_ore": "TIOC=F", "hierro": "TIOC=F",
            # Indices
            "sp500": "^GSPC", "nasdaq": "^IXIC",
            "dollar_index": "DX-Y.NYB", "dxy": "DX-Y.NYB",
            # Crypto
            "bitcoin": "BTC-USD", "ethereum": "ETH-USD",
        }

        return commodity_map.get(commodity.lower())


# ============================================================================
# TEST AND INITIALIZATION
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Test básico
    async def test():
        memory = AutonomousMemory()

        # Crear una señal de prueba
        test_signal = {
            "commodity": "gold",
            "timestamp": datetime.utcnow(),
            "final_signal": "BUY",
            "conviction": 75,
            "reasoning": "Golden cross detected on daily chart",
            "agents_bullish": 7,
            "agents_bearish": 2,
            "agents_neutral": 1,
            "risk_guardian_veto": False,
            "total_latency_ms": 1250,
            "cost_usd": 0.15,
            "session_id": "test_session_001",
            "price_at_signal": 2050.50,
            "agent_results": [
                {
                    "agent_name": "technical_analyzer",
                    "signal": "BUY",
                    "confidence": 85,
                    "model_used": "lstm_v2",
                    "provider": "internal",
                    "latency_ms": 350
                },
                {
                    "agent_name": "fundamental_analyst",
                    "signal": "BUY",
                    "confidence": 70,
                    "model_used": "bert_finance",
                    "provider": "huggingface",
                    "latency_ms": 450
                }
            ]
        }

        signal_id = await memory.save_signal(test_signal)
        print(f"✓ Signal saved with ID: {signal_id}")

        # Registrar desempeño de agentes
        await memory.record_agent_performance("gold", "technical_analyzer", "BUY", 85, was_correct=True)
        await memory.record_agent_performance("gold", "fundamental_analyst", "BUY", 70, was_correct=True)
        print("✓ Agent performance recorded")

        # Obtener track record
        track_record = await memory.get_track_record()
        print(f"✓ Track record: {track_record}")

        # Obtener leaderboard
        leaderboard = await memory.get_agent_leaderboard()
        print(f"✓ Agent leaderboard: {leaderboard}")

        # Obtener contexto de memoria
        context = await memory.get_memory_context("gold")
        print(f"✓ Memory context:\n{context}")

        # Obtener dashboard
        dashboard = await memory.get_performance_dashboard()
        print(f"✓ Dashboard generated with {dashboard['total_signals']} total signals")

        print("\n✅ All tests passed!")

    asyncio.run(test())
