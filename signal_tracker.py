"""
Meteoro X Signal Tracker
========================

Phase 1: PROVE - Signal tracking and verification system

Tracks every trading signal produced by the Meteoro X system, stores it in SQLite,
and verifies outcomes against actual price movements. Builds a verifiable track record
of model performance.

Thread-safe: Uses connection-per-call pattern for SQLite access.
"""

import sqlite3
import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any
from statistics import mean, stdev

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False


# Database location (relative to module directory)
DB_PATH = Path(__file__).parent / "signals.db"

# Risk-free rate for Sharpe calculation (5% annual)
RISK_FREE_RATE = 0.05


# ============================================================================
# Database Initialization
# ============================================================================

def init_db() -> None:
    """
    Create signals table if it doesn't exist.
    
    Schema:
    - id: UUID primary key
    - timestamp: ISO format creation time
    - commodity: e.g., 'BTC', 'ETH', 'AUDUSD', 'XAUUSD'
    - direction: 'LONG', 'SHORT', or 'HOLD'
    - conviction: 0-100 confidence score
    - entry_price: Entry price at signal time
    - stop_loss: Stop loss price
    - take_profit: Take profit target
    - position_size_pct: Position size as % of portfolio
    - risk_reward: Risk/reward ratio
    - kelly_fraction: Kelly criterion bet size fraction
    - key_risk: Main risk factor for this signal
    - headline: Human-readable signal description
    - model_used: Which model generated this signal
    - pipeline_latency_ms: Processing latency
    - cost_usd: Estimated transaction cost
    - data_sources: JSON array of data sources used
    - verified_5d, verified_20d, verified_60d: Actual % change from entry
    - outcome_5d, outcome_20d, outcome_60d: WIN/LOSS (NULL until verified)
    - closed: Boolean flag, 1 if position closed
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS signals (
        id TEXT PRIMARY KEY,
        timestamp TEXT NOT NULL,
        commodity TEXT NOT NULL,
        direction TEXT NOT NULL,
        conviction REAL NOT NULL,
        entry_price REAL NOT NULL,
        stop_loss REAL NOT NULL,
        take_profit REAL NOT NULL,
        position_size_pct REAL NOT NULL,
        risk_reward REAL NOT NULL,
        kelly_fraction REAL NOT NULL,
        key_risk TEXT NOT NULL,
        headline TEXT NOT NULL,
        model_used TEXT NOT NULL,
        pipeline_latency_ms REAL NOT NULL,
        cost_usd REAL NOT NULL,
        data_sources TEXT NOT NULL,
        verified_5d REAL,
        verified_20d REAL,
        verified_60d REAL,
        outcome_5d TEXT,
        outcome_20d TEXT,
        outcome_60d TEXT,
        closed INTEGER DEFAULT 0,
        created_at TEXT NOT NULL
    )
    """)
    
    # Create indexes for common queries
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_timestamp ON signals(timestamp)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_commodity ON signals(commodity)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_direction ON signals(direction)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_closed ON signals(closed)
    """)
    
    conn.commit()
    conn.close()


# ============================================================================
# Signal Recording
# ============================================================================

def record_signal(signal_data: Dict[str, Any]) -> str:
    """
    Record a new trading signal to the database.
    
    Args:
        signal_data: Dictionary with keys:
            - commodity (str): e.g., 'BTC', 'ETH', 'AUDUSD'
            - direction (str): 'LONG', 'SHORT', or 'HOLD'
            - conviction (float): 0-100
            - entry_price (float)
            - stop_loss (float)
            - take_profit (float)
            - position_size_pct (float)
            - risk_reward (float)
            - kelly_fraction (float)
            - key_risk (str)
            - headline (str)
            - model_used (str)
            - pipeline_latency_ms (float)
            - cost_usd (float)
            - data_sources (list): List of data source names
    
    Returns:
        Signal ID (UUID string)
    
    Raises:
        KeyError: If required field is missing
        ValueError: If data validation fails
    """
    # Validate required fields
    required_fields = {
        'commodity', 'direction', 'conviction', 'entry_price', 'stop_loss',
        'take_profit', 'position_size_pct', 'risk_reward', 'kelly_fraction',
        'key_risk', 'headline', 'model_used', 'pipeline_latency_ms',
        'cost_usd', 'data_sources'
    }
    
    missing = required_fields - set(signal_data.keys())
    if missing:
        raise KeyError(f"Missing required fields: {missing}")
    
    # Validate direction
    if signal_data['direction'] not in ('LONG', 'SHORT', 'HOLD'):
        raise ValueError(f"Invalid direction: {signal_data['direction']}")
    
    # Validate conviction range
    if not (0 <= signal_data['conviction'] <= 100):
        raise ValueError(f"Conviction must be 0-100, got {signal_data['conviction']}")
    
    # Validate prices
    if signal_data['direction'] in ('LONG', 'SHORT'):
        entry = signal_data['entry_price']
        stop = signal_data['stop_loss']
        tp = signal_data['take_profit']
        
        if entry <= 0 or stop <= 0 or tp <= 0:
            raise ValueError("Prices must be positive")
        
        if signal_data['direction'] == 'LONG':
            if not (stop < entry < tp):
                raise ValueError(
                    f"For LONG: need stop_loss < entry_price < take_profit, "
                    f"got {stop} < {entry} < {tp}"
                )
        else:  # SHORT
            if not (tp < entry < stop):
                raise ValueError(
                    f"For SHORT: need take_profit < entry_price < stop_loss, "
                    f"got {tp} < {entry} < {stop}"
                )
    
    # Generate ID and timestamps
    signal_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat() + "Z"
    
    # Serialize data_sources to JSON
    data_sources_json = json.dumps(signal_data['data_sources'])
    
    # Insert into database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    INSERT INTO signals (
        id, timestamp, commodity, direction, conviction, entry_price,
        stop_loss, take_profit, position_size_pct, risk_reward, kelly_fraction,
        key_risk, headline, model_used, pipeline_latency_ms, cost_usd,
        data_sources, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        signal_id,
        signal_data.get('timestamp', now),
        signal_data['commodity'],
        signal_data['direction'],
        signal_data['conviction'],
        signal_data['entry_price'],
        signal_data['stop_loss'],
        signal_data['take_profit'],
        signal_data['position_size_pct'],
        signal_data['risk_reward'],
        signal_data['kelly_fraction'],
        signal_data['key_risk'],
        signal_data['headline'],
        signal_data['model_used'],
        signal_data['pipeline_latency_ms'],
        signal_data['cost_usd'],
        data_sources_json,
        now
    ))
    
    conn.commit()
    conn.close()
    
    return signal_id


# ============================================================================
# Signal Verification
# ============================================================================

def _get_current_price(commodity: str) -> Optional[float]:
    """
    Fetch current price for a commodity using yfinance.
    
    Args:
        commodity: e.g., 'BTC', 'ETH', 'AAPL', 'AUDUSD', 'XAUUSD'
    
    Returns:
        Current price, or None if unable to fetch
    """
    if not HAS_YFINANCE:
        return None
    
    # Map commodity symbols to yfinance tickers
    ticker_map = {
        'BTC': 'BTC-USD',
        'ETH': 'ETH-USD',
        'XAU': 'GC=F',
        'XAUUSD': 'GC=F',
        'AUDUSD': 'AUDUSD=X',
        'GBPUSD': 'GBPUSD=X',
        'EURUSD': 'EURUSD=X',
        'USDJPY': 'USDJPY=X',
    }
    
    ticker = ticker_map.get(commodity.upper(), f"{commodity.upper()}-USD")
    
    try:
        data = yf.Ticker(ticker)
        price = data.info.get('currentPrice') or data.info.get('regularMarketPrice')
        if price:
            return float(price)
    except Exception:
        pass
    
    return None


def verify_signals() -> Dict[str, Any]:
    """
    Verify all open signals against current prices.
    
    For each signal:
    - If older than 5 days, fetch current price
    - Calculate % change from entry_price
    - Determine WIN (hit TP before SL) or LOSS (hit SL before TP)
    - Update verified_5d, verified_20d, verified_60d fields
    - Update outcome fields
    
    Returns:
        Dictionary with verification stats:
        {
            'total_verified': int,
            'new_wins': int,
            'new_losses': int,
            'errors': list of error messages
        }
    """
    if not HAS_YFINANCE:
        return {
            'total_verified': 0,
            'new_wins': 0,
            'new_losses': 0,
            'errors': ['yfinance not available - cannot verify signals']
        }
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all open signals
    cursor.execute("""
    SELECT id, commodity, direction, entry_price, stop_loss, take_profit,
           timestamp, verified_5d, verified_20d, verified_60d
    FROM signals
    WHERE closed = 0
    ORDER BY timestamp ASC
    """)
    
    signals = cursor.fetchall()
    
    stats = {
        'total_verified': 0,
        'new_wins': 0,
        'new_losses': 0,
        'errors': []
    }
    
    now = datetime.utcnow()
    
    for signal_row in signals:
        (signal_id, commodity, direction, entry_price, stop_loss, take_profit,
         timestamp_str, verified_5d, verified_20d, verified_60d) = signal_row
        
        try:
            # Parse signal timestamp
            signal_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            age_days = (now - signal_time).days
            
            # Only verify signals older than 5 days
            if age_days < 5:
                continue
            
            # Skip if already verified at this age
            if age_days >= 5 and verified_5d is not None:
                continue
            
            # Fetch current price
            current_price = _get_current_price(commodity)
            if current_price is None:
                stats['errors'].append(f"Could not fetch price for {commodity}")
                continue
            
            # Calculate % change
            pct_change = ((current_price - entry_price) / entry_price) * 100
            
            # Determine outcome based on direction
            if direction == 'LONG':
                if current_price >= take_profit:
                    outcome = 'WIN'
                    stats['new_wins'] += 1
                elif current_price <= stop_loss:
                    outcome = 'LOSS'
                    stats['new_losses'] += 1
                else:
                    outcome = None  # Still open
            elif direction == 'SHORT':
                if current_price <= take_profit:
                    outcome = 'WIN'
                    stats['new_wins'] += 1
                elif current_price >= stop_loss:
                    outcome = 'LOSS'
                    stats['new_losses'] += 1
                else:
                    outcome = None  # Still open
            else:  # HOLD
                outcome = None
            
            # Update database based on age
            if age_days >= 5 and verified_5d is None:
                cursor.execute("""
                UPDATE signals
                SET verified_5d = ?, outcome_5d = ?
                WHERE id = ?
                """, (pct_change, outcome, signal_id))
                stats['total_verified'] += 1
            
            if age_days >= 20 and verified_20d is None:
                cursor.execute("""
                UPDATE signals
                SET verified_20d = ?, outcome_20d = ?
                WHERE id = ?
                """, (pct_change, outcome, signal_id))
            
            if age_days >= 60 and verified_60d is None:
                cursor.execute("""
                UPDATE signals
                SET verified_60d = ?, outcome_60d = ?
                WHERE id = ?
                """, (pct_change, outcome, signal_id))
            
            # Mark as closed if outcome reached
            if outcome is not None:
                cursor.execute("""
                UPDATE signals
                SET closed = 1
                WHERE id = ?
                """, (signal_id,))
        
        except Exception as e:
            stats['errors'].append(f"Error verifying {signal_id}: {str(e)}")
    
    conn.commit()
    conn.close()
    
    return stats


# ============================================================================
# Track Record & Analytics
# ============================================================================

def get_track_record(commodity: Optional[str] = None) -> Dict[str, Any]:
    """
    Calculate and return comprehensive track record statistics.
    
    Args:
        commodity: If provided, filter to this commodity only (e.g., 'BTC')
    
    Returns:
        Dictionary with:
        {
            'total_signals': int,
            'win_rate_5d': float (0-100) or None,
            'win_rate_20d': float (0-100) or None,
            'win_rate_60d': float (0-100) or None,
            'avg_return_5d': float (%) or None,
            'avg_return_20d': float (%) or None,
            'avg_return_60d': float (%) or None,
            'sharpe_ratio_5d': float or None,
            'sharpe_ratio_20d': float or None,
            'sharpe_ratio_60d': float or None,
            'best_signal': {signal dict} or None,
            'worst_signal': {signal dict} or None,
            'by_commodity': {commodity: stats} dict,
            'by_direction': {LONG/SHORT: stats} dict,
        }
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Base query
    where_clause = ""
    params = []
    if commodity:
        where_clause = "WHERE commodity = ?"
        params = [commodity]
    
    # Total signals
    cursor.execute(f"SELECT COUNT(*) FROM signals {where_clause}", params)
    total = cursor.fetchone()[0]
    
    # 5-day outcomes
    cursor.execute(f"""
    SELECT outcome_5d, verified_5d FROM signals
    WHERE outcome_5d IS NOT NULL {f'AND commodity = ?' if commodity else ''}
    """, params if commodity else [])
    
    outcomes_5d = cursor.fetchall()
    wins_5d = sum(1 for outcome, _ in outcomes_5d if outcome == 'WIN')
    returns_5d = [ret for _, ret in outcomes_5d]
    win_rate_5d = (wins_5d / len(outcomes_5d) * 100) if outcomes_5d else None
    avg_return_5d = mean(returns_5d) if returns_5d else None
    sharpe_5d = _calculate_sharpe(returns_5d, 5) if returns_5d else None
    
    # 20-day outcomes
    cursor.execute(f"""
    SELECT outcome_20d, verified_20d FROM signals
    WHERE outcome_20d IS NOT NULL {f'AND commodity = ?' if commodity else ''}
    """, params if commodity else [])
    
    outcomes_20d = cursor.fetchall()
    wins_20d = sum(1 for outcome, _ in outcomes_20d if outcome == 'WIN')
    returns_20d = [ret for _, ret in outcomes_20d]
    win_rate_20d = (wins_20d / len(outcomes_20d) * 100) if outcomes_20d else None
    avg_return_20d = mean(returns_20d) if returns_20d else None
    sharpe_20d = _calculate_sharpe(returns_20d, 20) if returns_20d else None
    
    # 60-day outcomes
    cursor.execute(f"""
    SELECT outcome_60d, verified_60d FROM signals
    WHERE outcome_60d IS NOT NULL {f'AND commodity = ?' if commodity else ''}
    """, params if commodity else [])
    
    outcomes_60d = cursor.fetchall()
    wins_60d = sum(1 for outcome, _ in outcomes_60d if outcome == 'WIN')
    returns_60d = [ret for _, ret in outcomes_60d]
    win_rate_60d = (wins_60d / len(outcomes_60d) * 100) if outcomes_60d else None
    avg_return_60d = mean(returns_60d) if returns_60d else None
    sharpe_60d = _calculate_sharpe(returns_60d, 60) if returns_60d else None
    
    # Best and worst signals (by 5d return)
    cursor.execute(f"""
    SELECT * FROM signals
    WHERE verified_5d IS NOT NULL {f'AND commodity = ?' if commodity else ''}
    ORDER BY verified_5d DESC
    LIMIT 1
    """, params if commodity else [])
    best = cursor.fetchone()
    
    cursor.execute(f"""
    SELECT * FROM signals
    WHERE verified_5d IS NOT NULL {f'AND commodity = ?' if commodity else ''}
    ORDER BY verified_5d ASC
    LIMIT 1
    """, params if commodity else [])
    worst = cursor.fetchone()
    
    # By commodity
    cursor.execute("""
    SELECT commodity, COUNT(*), 
           SUM(CASE WHEN outcome_5d = 'WIN' THEN 1 ELSE 0 END),
           AVG(verified_5d)
    FROM signals
    WHERE outcome_5d IS NOT NULL
    GROUP BY commodity
    """)
    by_commodity = {}
    for row in cursor.fetchall():
        commodity_name, count, wins, avg_ret = row
        by_commodity[commodity_name] = {
            'signals': count,
            'wins': wins,
            'win_rate': (wins / count * 100) if count else None,
            'avg_return_5d': avg_ret
        }
    
    # By direction
    cursor.execute("""
    SELECT direction, COUNT(*),
           SUM(CASE WHEN outcome_5d = 'WIN' THEN 1 ELSE 0 END),
           AVG(verified_5d)
    FROM signals
    WHERE outcome_5d IS NOT NULL
    GROUP BY direction
    """)
    by_direction = {}
    for row in cursor.fetchall():
        direction, count, wins, avg_ret = row
        by_direction[direction] = {
            'signals': count,
            'wins': wins,
            'win_rate': (wins / count * 100) if count else None,
            'avg_return_5d': avg_ret
        }
    
    conn.close()
    
    return {
        'total_signals': total,
        'win_rate_5d': win_rate_5d,
        'win_rate_20d': win_rate_20d,
        'win_rate_60d': win_rate_60d,
        'avg_return_5d': avg_return_5d,
        'avg_return_20d': avg_return_20d,
        'avg_return_60d': avg_return_60d,
        'sharpe_ratio_5d': sharpe_5d,
        'sharpe_ratio_20d': sharpe_20d,
        'sharpe_ratio_60d': sharpe_60d,
        'best_signal': _row_to_dict(best) if best else None,
        'worst_signal': _row_to_dict(worst) if worst else None,
        'by_commodity': by_commodity,
        'by_direction': by_direction,
    }


def _calculate_sharpe(returns: List[float], holding_period_days: int) -> Optional[float]:
    """
    Calculate annualized Sharpe ratio.
    
    Formula: (mean_return - risk_free_rate) / std_return * sqrt(252/holding_period)
    
    Args:
        returns: List of returns in %
        holding_period_days: Number of days held
    
    Returns:
        Annualized Sharpe ratio, or None if insufficient data
    """
    if len(returns) < 2:
        return None
    
    mean_ret = mean(returns) / 100  # Convert to decimal
    
    try:
        std_ret = stdev(returns) / 100
    except ValueError:
        return None
    
    if std_ret == 0:
        return None
    
    # Annualize: (252 / holding_period) adjusts for the holding period
    annual_periods = 252 / holding_period_days
    sharpe = (mean_ret - RISK_FREE_RATE) / std_ret * (annual_periods ** 0.5)
    
    return sharpe


def _row_to_dict(row: tuple) -> Dict[str, Any]:
    """Convert database row tuple to signal dictionary."""
    if not row:
        return None
    
    # Column order from SELECT *
    keys = [
        'id', 'timestamp', 'commodity', 'direction', 'conviction', 'entry_price',
        'stop_loss', 'take_profit', 'position_size_pct', 'risk_reward',
        'kelly_fraction', 'key_risk', 'headline', 'model_used',
        'pipeline_latency_ms', 'cost_usd', 'data_sources', 'verified_5d',
        'verified_20d', 'verified_60d', 'outcome_5d', 'outcome_20d',
        'outcome_60d', 'closed', 'created_at'
    ]
    
    signal_dict = dict(zip(keys, row))
    
    # Parse data_sources JSON
    if signal_dict.get('data_sources'):
        try:
            signal_dict['data_sources'] = json.loads(signal_dict['data_sources'])
        except json.JSONDecodeError:
            signal_dict['data_sources'] = []
    
    return signal_dict


# ============================================================================
# Query Functions
# ============================================================================

def get_recent_signals(
    limit: int = 20,
    commodity: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Get recent signals with their outcomes.
    
    Args:
        limit: Maximum number of signals to return
        commodity: Filter to specific commodity (e.g., 'BTC')
    
    Returns:
        List of signal dictionaries, sorted by timestamp DESC
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    where_clause = ""
    params = []
    if commodity:
        where_clause = "WHERE commodity = ?"
        params = [commodity]
    
    cursor.execute(f"""
    SELECT * FROM signals
    {where_clause}
    ORDER BY timestamp DESC
    LIMIT ?
    """, params + [limit])
    
    rows = cursor.fetchall()
    conn.close()
    
    return [_row_to_dict(row) for row in rows]


def get_signal_by_id(signal_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve a specific signal by ID.
    
    Args:
        signal_id: UUID of the signal
    
    Returns:
        Signal dictionary, or None if not found
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM signals WHERE id = ?", (signal_id,))
    row = cursor.fetchone()
    conn.close()
    
    return _row_to_dict(row) if row else None


def get_signals_by_commodity(commodity: str) -> List[Dict[str, Any]]:
    """
    Get all signals for a specific commodity.
    
    Args:
        commodity: e.g., 'BTC', 'ETH', 'AUDUSD'
    
    Returns:
        List of signal dictionaries
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT * FROM signals
    WHERE commodity = ?
    ORDER BY timestamp DESC
    """, (commodity,))
    
    rows = cursor.fetchall()
    conn.close()
    
    return [_row_to_dict(row) for row in rows]


def get_open_signals() -> List[Dict[str, Any]]:
    """
    Get all currently open (unverified) signals.
    
    Returns:
        List of signal dictionaries with closed = 0
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT * FROM signals
    WHERE closed = 0
    ORDER BY timestamp DESC
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    return [_row_to_dict(row) for row in rows]


def get_closed_signals() -> List[Dict[str, Any]]:
    """
    Get all closed signals with verified outcomes.
    
    Returns:
        List of signal dictionaries with closed = 1
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT * FROM signals
    WHERE closed = 1
    ORDER BY timestamp DESC
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    return [_row_to_dict(row) for row in rows]


# ============================================================================
# Utility Functions
# ============================================================================

def delete_all_signals() -> None:
    """
    Delete all signals from the database.
    WARNING: This is only for testing/reset purposes.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM signals")
    conn.commit()
    conn.close()


def get_db_path() -> Path:
    """Return the path to the signals database file."""
    return DB_PATH


if __name__ == '__main__':
    # Quick test
    init_db()
    print("Database initialized at:", DB_PATH)
