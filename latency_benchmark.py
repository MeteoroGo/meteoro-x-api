"""
METEORO X — Latency Benchmark (Shadow Engine Module 1)
=======================================================
Measures the exact time delta between Meteoro's detection (T=0)
and when Bloomberg/Reuters publish the equivalent headline (T+X hours).

This is our most powerful metric for YC:
"We detect disruptions X hours before Bloomberg, consistently."

Usage:
  1. When Meteoro detects an event → record_detection(pack_id, command, T=0)
  2. When Bloomberg publishes headline → record_bloomberg_headline(pack_id, T+X)
  3. Get stats → get_stats() → avg latency advantage in hours
"""

import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List


class LatencyBenchmark:
    """
    Shadow Engine Module 1: Latency Benchmarking.
    Tracks the time advantage Meteoro has over traditional news services.
    """

    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "latency_benchmark.db"
            )
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize SQLite database for latency tracking."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS detections (
                pack_id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                detection_time TEXT NOT NULL,
                commodity TEXT,
                ticker TEXT,
                signal_direction TEXT,
                conviction REAL DEFAULT 0,
                bloomberg_time TEXT,
                bloomberg_headline TEXT,
                bloomberg_url TEXT,
                reuters_time TEXT,
                reuters_headline TEXT,
                latency_hours REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY,
                total_detections INTEGER DEFAULT 0,
                bloomberg_confirmed INTEGER DEFAULT 0,
                avg_latency_hours REAL,
                min_latency_hours REAL,
                max_latency_hours REAL,
                total_advantage_hours REAL DEFAULT 0
            )
        """)
        conn.commit()
        conn.close()

    def record_detection(self, pack_id: str, command: str, detection_time: str,
                         commodity: str = "", ticker: str = "",
                         signal_direction: str = "", conviction: float = 0):
        """Record a new Meteoro detection event (T=0)."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO detections
                (pack_id, command, detection_time, commodity, ticker, signal_direction, conviction)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (pack_id, command, detection_time, commodity, ticker,
                  signal_direction, conviction))
            conn.commit()
        finally:
            conn.close()

    def record_bloomberg_headline(self, pack_id: str, bloomberg_time: str,
                                  headline: str = "", headline_url: str = ""):
        """Record when Bloomberg publishes a headline for a detected event."""
        conn = sqlite3.connect(self.db_path)
        try:
            # Get detection time
            row = conn.execute(
                "SELECT detection_time FROM detections WHERE pack_id = ?",
                (pack_id,)
            ).fetchone()

            if not row:
                return {"error": f"Pack {pack_id} not found"}

            detection_time = datetime.fromisoformat(row[0])
            bloomberg_dt = datetime.fromisoformat(bloomberg_time)

            # Calculate latency in hours
            delta = bloomberg_dt - detection_time
            latency_hours = delta.total_seconds() / 3600

            conn.execute("""
                UPDATE detections
                SET bloomberg_time = ?, bloomberg_headline = ?, bloomberg_url = ?,
                    latency_hours = ?
                WHERE pack_id = ?
            """, (bloomberg_time, headline, headline_url, latency_hours, pack_id))

            # Update daily stats
            date_str = detection_time.strftime("%Y-%m-%d")
            self._update_daily_stats(conn, date_str)

            conn.commit()
            return {
                "pack_id": pack_id,
                "latency_hours": round(latency_hours, 2),
                "latency_human": self._format_latency(latency_hours),
            }
        finally:
            conn.close()

    def record_reuters_headline(self, pack_id: str, reuters_time: str,
                                headline: str = ""):
        """Record when Reuters publishes a headline for a detected event."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                UPDATE detections
                SET reuters_time = ?, reuters_headline = ?
                WHERE pack_id = ?
            """, (reuters_time, headline, pack_id))
            conn.commit()
        finally:
            conn.close()

    def get_stats(self) -> Dict:
        """Get comprehensive latency benchmarking statistics."""
        conn = sqlite3.connect(self.db_path)
        try:
            # Total detections
            total = conn.execute("SELECT COUNT(*) FROM detections").fetchone()[0]

            # Confirmed by Bloomberg
            confirmed = conn.execute(
                "SELECT COUNT(*) FROM detections WHERE bloomberg_time IS NOT NULL"
            ).fetchone()[0]

            # Average latency
            avg_row = conn.execute(
                "SELECT AVG(latency_hours), MIN(latency_hours), MAX(latency_hours) "
                "FROM detections WHERE latency_hours IS NOT NULL"
            ).fetchone()

            avg_latency = avg_row[0] if avg_row[0] else 0
            min_latency = avg_row[1] if avg_row[1] else 0
            max_latency = avg_row[2] if avg_row[2] else 0

            # Recent detections
            recent = conn.execute(
                "SELECT pack_id, command, detection_time, bloomberg_time, latency_hours "
                "FROM detections ORDER BY created_at DESC LIMIT 10"
            ).fetchall()

            recent_list = []
            for row in recent:
                recent_list.append({
                    "pack_id": row[0],
                    "command": row[1][:50],
                    "detection_time": row[2],
                    "bloomberg_time": row[3],
                    "latency_hours": round(row[4], 2) if row[4] else None,
                    "status": "confirmed" if row[3] else "pending",
                })

            # Daily breakdown
            daily = conn.execute(
                "SELECT date, total_detections, bloomberg_confirmed, avg_latency_hours "
                "FROM daily_stats ORDER BY date DESC LIMIT 30"
            ).fetchall()

            return {
                "status": "operational",
                "module": "Shadow Engine — Latency Benchmark",
                "summary": {
                    "total_detections": total,
                    "bloomberg_confirmed": confirmed,
                    "confirmation_rate": f"{(confirmed/max(total,1))*100:.0f}%",
                    "avg_advantage_hours": round(avg_latency, 2),
                    "avg_advantage_human": self._format_latency(avg_latency),
                    "min_advantage_hours": round(min_latency, 2),
                    "max_advantage_hours": round(max_latency, 2),
                    "yc_metric": f"Meteoro detects {self._format_latency(avg_latency)} before Bloomberg"
                                 if avg_latency > 0 else "Collecting data...",
                },
                "recent_detections": recent_list,
                "daily_stats": [
                    {"date": d[0], "detections": d[1], "confirmed": d[2], "avg_hours": d[3]}
                    for d in daily
                ],
            }
        finally:
            conn.close()

    def _update_daily_stats(self, conn, date_str: str):
        """Recalculate daily stats for a given date."""
        stats = conn.execute("""
            SELECT COUNT(*),
                   SUM(CASE WHEN bloomberg_time IS NOT NULL THEN 1 ELSE 0 END),
                   AVG(latency_hours),
                   MIN(latency_hours),
                   MAX(latency_hours),
                   SUM(COALESCE(latency_hours, 0))
            FROM detections
            WHERE date(detection_time) = ?
        """, (date_str,)).fetchone()

        conn.execute("""
            INSERT OR REPLACE INTO daily_stats
            (date, total_detections, bloomberg_confirmed, avg_latency_hours,
             min_latency_hours, max_latency_hours, total_advantage_hours)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (date_str, stats[0], stats[1], stats[2], stats[3], stats[4], stats[5]))

    @staticmethod
    def _format_latency(hours: float) -> str:
        """Format latency hours into human-readable string."""
        if hours <= 0:
            return "N/A"
        if hours < 1:
            return f"{int(hours * 60)} minutes"
        elif hours < 24:
            return f"{hours:.1f} hours"
        else:
            days = int(hours / 24)
            remaining_hours = hours % 24
            return f"{days}d {remaining_hours:.0f}h"


# ═══════════════════════════════════════════════════════════════
# CLI TEST
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Testing Latency Benchmark Module...")
    bench = LatencyBenchmark(db_path="/tmp/test_latency.db")

    # Simulate detection
    t0 = datetime.now(timezone.utc)
    bench.record_detection(
        pack_id="MX-TEST-001",
        command="Cerrejon coal strike detected",
        detection_time=t0.isoformat(),
        commodity="coal",
        ticker="MTF=F",
        signal_direction="LONG",
        conviction=85.0,
    )
    print(f"  Detection recorded at T=0: {t0.isoformat()}")

    # Simulate Bloomberg headline 18 hours later
    t_bloomberg = t0 + timedelta(hours=18)
    result = bench.record_bloomberg_headline(
        pack_id="MX-TEST-001",
        bloomberg_time=t_bloomberg.isoformat(),
        headline="Cerrejon Workers Walk Out in Colombia Coal Strike",
        headline_url="https://bloomberg.com/example",
    )
    print(f"  Bloomberg headline at T+18h: {result}")

    # Simulate another detection (6 hours ahead)
    t1 = datetime.now(timezone.utc) - timedelta(hours=1)
    bench.record_detection(
        pack_id="MX-TEST-002",
        command="Las Bambas blockade satellite anomaly",
        detection_time=t1.isoformat(),
        commodity="copper",
        ticker="HG=F",
    )

    t_bloomberg2 = t1 + timedelta(hours=6)
    bench.record_bloomberg_headline(
        pack_id="MX-TEST-002",
        bloomberg_time=t_bloomberg2.isoformat(),
        headline="Peru Copper Mine Faces Fresh Blockade",
    )

    # Get stats
    stats = bench.get_stats()
    print(f"\n  Stats: {json.dumps(stats['summary'], indent=2)}")
    print(f"\n  Recent: {json.dumps(stats['recent_detections'], indent=2)}")

    # Cleanup
    os.remove("/tmp/test_latency.db")
    print("\n  Test passed!")
