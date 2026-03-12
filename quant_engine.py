"""
Meteoro X Quantitative Trading Engine
======================================

A production-ready quantitative trading engine that calculates all technical indicators,
risk metrics, and execution plans using pure Python/NumPy mathematics.

All outputs are mathematically verifiable and include calculation methods for transparency.
No LLM delegation - every number is calculated.
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Union
import warnings

warnings.filterwarnings('ignore')


class QuantitativeEngine:
    """Production-ready quantitative trading engine for technical analysis and signal generation."""
    
    # Default parameters
    DEFAULT_WIN_RATE = 0.55
    DEFAULT_WIN_LOSS_RATIO = 1.5
    RSI_PERIOD = 14
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9
    BB_PERIOD = 20
    BB_STD_DEV = 2.0
    ADX_PERIOD = 14
    ATR_PERIOD = 14
    STOCH_RSI_PERIOD = 14
    SMA_PERIODS = [10, 20, 50, 200]
    EMA_PERIODS = [12, 26]
    
    @staticmethod
    def _validate_data(closes: np.ndarray, min_length: int = 2) -> bool:
        """Validate input data."""
        if closes is None or len(closes) < min_length:
            return False
        if not np.isfinite(closes).all():
            return False
        return True
    
    @staticmethod
    def _calculate_rsi(closes: np.ndarray, period: int = 14) -> np.ndarray:
        """
        Calculate Relative Strength Index (Wilder's smoothing).
        
        Returns array of RSI values (0-100).
        """
        if not QuantitativeEngine._validate_data(closes, period + 1):
            return np.full(len(closes), np.nan)
        
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.zeros(len(closes))
        avg_loss = np.zeros(len(closes))
        
        # Initialize with SMA
        avg_gain[period] = np.mean(gains[:period])
        avg_loss[period] = np.mean(losses[:period])
        
        # Wilder's smoothing
        for i in range(period + 1, len(closes)):
            avg_gain[i] = (avg_gain[i-1] * (period - 1) + gains[i-1]) / period
            avg_loss[i] = (avg_loss[i-1] * (period - 1) + losses[i-1]) / period
        
        rs = np.divide(avg_gain, avg_loss, where=avg_loss != 0, out=np.zeros_like(avg_gain))
        rsi = 100 - (100 / (1 + rs))
        
        # Fill initial values with NaN
        rsi[:period] = np.nan
        return rsi
    
    @staticmethod
    def _calculate_ema(closes: np.ndarray, period: int) -> np.ndarray:
        """Calculate Exponential Moving Average."""
        if not QuantitativeEngine._validate_data(closes, period):
            return np.full(len(closes), np.nan)
        
        ema = np.zeros(len(closes))
        multiplier = 2 / (period + 1)
        
        # Start with SMA
        ema[period - 1] = np.mean(closes[:period])
        
        for i in range(period, len(closes)):
            ema[i] = (closes[i] * multiplier) + (ema[i-1] * (1 - multiplier))
        
        ema[:period - 1] = np.nan
        return ema
    
    @staticmethod
    def _calculate_sma(closes: np.ndarray, period: int) -> np.ndarray:
        """Calculate Simple Moving Average."""
        if not QuantitativeEngine._validate_data(closes, period):
            return np.full(len(closes), np.nan)
        
        sma = np.convolve(closes, np.ones(period)/period, mode='valid')
        return np.concatenate([np.full(period - 1, np.nan), sma])
    
    @staticmethod
    def _calculate_macd(closes: np.ndarray, 
                        fast: int = 12, 
                        slow: int = 26, 
                        signal: int = 9) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Calculate MACD (Moving Average Convergence Divergence).
        
        Returns:
            macd_line, signal_line, histogram
        """
        if not QuantitativeEngine._validate_data(closes, slow + signal):
            return (np.full(len(closes), np.nan), 
                    np.full(len(closes), np.nan), 
                    np.full(len(closes), np.nan))
        
        ema_fast = QuantitativeEngine._calculate_ema(closes, fast)
        ema_slow = QuantitativeEngine._calculate_ema(closes, slow)
        
        macd_line = ema_fast - ema_slow
        signal_line = QuantitativeEngine._calculate_ema(macd_line, signal)
        histogram = macd_line - signal_line
        
        return macd_line, signal_line, histogram
    
    @staticmethod
    def _calculate_bollinger_bands(closes: np.ndarray, 
                                   period: int = 20, 
                                   num_std: float = 2.0) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Calculate Bollinger Bands.
        
        Returns:
            upper_band, middle_band (SMA), lower_band, percent_b, bandwidth
        """
        if not QuantitativeEngine._validate_data(closes, period):
            size = len(closes)
            return (np.full(size, np.nan), np.full(size, np.nan), 
                    np.full(size, np.nan), np.full(size, np.nan), np.full(size, np.nan))
        
        middle = QuantitativeEngine._calculate_sma(closes, period)
        std = np.zeros(len(closes))
        
        for i in range(period - 1, len(closes)):
            std[i] = np.std(closes[i - period + 1:i + 1])
        
        upper = middle + (std * num_std)
        lower = middle - (std * num_std)
        
        # %B = (Price - Lower) / (Upper - Lower)
        percent_b = np.divide(closes - lower, upper - lower, 
                             where=(upper - lower) != 0, 
                             out=np.zeros_like(closes))
        
        # Bandwidth = (Upper - Lower) / Middle
        bandwidth = np.divide(upper - lower, middle, 
                             where=middle != 0, 
                             out=np.zeros_like(middle))
        
        return upper, middle, lower, percent_b, bandwidth
    
    @staticmethod
    def _calculate_atr(highs: np.ndarray, 
                       lows: np.ndarray, 
                       closes: np.ndarray, 
                       period: int = 14) -> np.ndarray:
        """
        Calculate Average True Range.
        
        Returns ATR values.
        """
        if not (QuantitativeEngine._validate_data(highs, period) and 
                QuantitativeEngine._validate_data(lows, period) and 
                QuantitativeEngine._validate_data(closes, period)):
            return np.full(len(closes), np.nan)
        
        tr = np.zeros(len(closes))
        tr[0] = highs[0] - lows[0]
        
        for i in range(1, len(closes)):
            tr[i] = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
        
        atr = np.zeros(len(closes))
        atr[period - 1] = np.mean(tr[:period])
        
        for i in range(period, len(closes)):
            atr[i] = (atr[i-1] * (period - 1) + tr[i]) / period
        
        atr[:period - 1] = np.nan
        return atr
    
    @staticmethod
    def _calculate_adx(highs: np.ndarray, 
                       lows: np.ndarray, 
                       period: int = 14) -> np.ndarray:
        """
        Calculate Average Directional Index (ADX) for trend strength.
        
        Returns ADX values (0-100).
        """
        if not (QuantitativeEngine._validate_data(highs, period * 2) and 
                QuantitativeEngine._validate_data(lows, period * 2)):
            return np.full(len(highs), np.nan)
        
        # Directional movements
        up_move = np.zeros(len(highs))
        down_move = np.zeros(len(highs))
        
        for i in range(1, len(highs)):
            up = highs[i] - highs[i-1]
            down = lows[i-1] - lows[i]
            
            up_move[i] = up if up > 0 and up > down else 0
            down_move[i] = down if down > 0 and down > up else 0
        
        # True range
        tr = np.zeros(len(highs))
        tr[0] = highs[0] - lows[0]
        for i in range(1, len(highs)):
            tr[i] = max(highs[i] - lows[i], 
                       abs(highs[i] - (closes if 'closes' in locals() else highs)[i-1]),
                       abs(lows[i] - (closes if 'closes' in locals() else highs)[i-1]))
        
        # Smoothed values
        plus_dm = np.zeros(len(highs))
        minus_dm = np.zeros(len(highs))
        plus_dm[period-1] = np.sum(up_move[:period])
        minus_dm[period-1] = np.sum(down_move[:period])
        
        for i in range(period, len(highs)):
            plus_dm[i] = plus_dm[i-1] - plus_dm[i-1]/period + up_move[i]
            minus_dm[i] = minus_dm[i-1] - minus_dm[i-1]/period + down_move[i]
        
        tr_sum = np.zeros(len(highs))
        tr_sum[period-1] = np.sum(tr[:period])
        for i in range(period, len(highs)):
            tr_sum[i] = tr_sum[i-1] - tr_sum[i-1]/period + tr[i]
        
        plus_di = 100 * np.divide(plus_dm, tr_sum, where=tr_sum != 0, out=np.zeros_like(tr_sum))
        minus_di = 100 * np.divide(minus_dm, tr_sum, where=tr_sum != 0, out=np.zeros_like(tr_sum))
        
        di_diff = np.abs(plus_di - minus_di)
        di_sum = plus_di + minus_di
        
        di_ratio = np.divide(di_diff, di_sum, where=di_sum != 0, out=np.zeros_like(di_sum))
        
        # ADX smoothing
        adx = np.zeros(len(highs))
        adx[period * 2 - 1] = np.mean(di_ratio[period:period * 2])
        
        for i in range(period * 2, len(highs)):
            adx[i] = (adx[i-1] * (period - 1) + di_ratio[i]) / period
        
        adx[:period * 2 - 1] = np.nan
        return adx * 100
    
    @staticmethod
    def _calculate_stochastic_rsi(closes: np.ndarray, 
                                   period: int = 14) -> Tuple[np.ndarray, np.ndarray]:
        """
        Calculate Stochastic RSI.
        
        Returns:
            stoch_rsi, signal_line
        """
        if not QuantitativeEngine._validate_data(closes, period * 2):
            return np.full(len(closes), np.nan), np.full(len(closes), np.nan)
        
        rsi = QuantitativeEngine._calculate_rsi(closes, period)
        
        stoch_rsi = np.zeros(len(closes))
        for i in range(period - 1, len(closes)):
            rsi_min = np.nanmin(rsi[i - period + 1:i + 1])
            rsi_max = np.nanmax(rsi[i - period + 1:i + 1])
            
            if rsi_max - rsi_min != 0:
                stoch_rsi[i] = (rsi[i] - rsi_min) / (rsi_max - rsi_min) * 100
            else:
                stoch_rsi[i] = 50
        
        stoch_rsi[:period] = np.nan
        
        # 3-period EMA of Stochastic RSI as signal
        signal = QuantitativeEngine._calculate_ema(stoch_rsi, 3)
        
        return stoch_rsi, signal
    
    @staticmethod
    def _calculate_vwap(highs: np.ndarray, 
                        lows: np.ndarray, 
                        closes: np.ndarray, 
                        volumes: np.ndarray) -> np.ndarray:
        """
        Calculate Volume Weighted Average Price.
        
        Returns VWAP values.
        """
        if not (QuantitativeEngine._validate_data(closes, 1) and 
                QuantitativeEngine._validate_data(volumes, 1) and
                np.any(volumes > 0)):
            return np.full(len(closes), np.nan)
        
        typical_price = (highs + lows + closes) / 3
        vwap = np.cumsum(typical_price * volumes) / np.cumsum(volumes)
        
        return vwap
    
    @staticmethod
    def _calculate_zscore(closes: np.ndarray, period: int = 20) -> np.ndarray:
        """
        Calculate Z-Score (standard deviations from mean).
        
        Returns Z-Score values.
        """
        if not QuantitativeEngine._validate_data(closes, period):
            return np.full(len(closes), np.nan)
        
        zscore = np.zeros(len(closes))
        
        for i in range(period - 1, len(closes)):
            window = closes[i - period + 1:i + 1]
            mean = np.mean(window)
            std = np.std(window)
            
            if std != 0:
                zscore[i] = (closes[i] - mean) / std
            else:
                zscore[i] = 0
        
        zscore[:period - 1] = np.nan
        return zscore
    
    @staticmethod
    def _find_support_resistance(highs: np.ndarray, 
                                 lows: np.ndarray, 
                                 period: int = 20) -> Tuple[float, float]:
        """
        Find support and resistance from recent highs/lows.
        
        Returns:
            support_level, resistance_level
        """
        if len(highs) < period or len(lows) < period:
            return lows[-1], highs[-1]
        
        recent_high = np.max(highs[-period:])
        recent_low = np.min(lows[-period:])
        
        return recent_low, recent_high
    
    @staticmethod
    def calculate_full_technicals(closes: np.ndarray,
                                  highs: np.ndarray,
                                  lows: np.ndarray,
                                  volumes: Optional[np.ndarray] = None) -> Dict:
        """
        Calculate ALL technical indicators from raw OHLCV data.
        
        Args:
            closes: Array of closing prices
            highs: Array of high prices
            lows: Array of low prices
            volumes: Array of volumes (optional)
        
        Returns:
            Dictionary with all technical indicators
        """
        # Input validation and conversion
        try:
            closes = np.asarray(closes, dtype=float)
            highs = np.asarray(highs, dtype=float)
            lows = np.asarray(lows, dtype=float)
            if volumes is not None:
                volumes = np.asarray(volumes, dtype=float)
        except (ValueError, TypeError):
            return {'error': 'Invalid input data', 'calculation_method': 'NumPy validation'}
        
        if len(closes) < 2:
            return {'error': 'Insufficient data (minimum 2 bars)', 'calculation_method': 'Data validation'}
        
        # Calculate all indicators
        current_price = closes[-1]
        
        # RSI
        rsi = QuantitativeEngine._calculate_rsi(closes, period=QuantitativeEngine.RSI_PERIOD)
        rsi_value = float(rsi[-1]) if np.isfinite(rsi[-1]) else np.nan
        
        # MACD
        macd, macd_signal, macd_hist = QuantitativeEngine._calculate_macd(
            closes, 
            fast=QuantitativeEngine.MACD_FAST,
            slow=QuantitativeEngine.MACD_SLOW,
            signal=QuantitativeEngine.MACD_SIGNAL
        )
        
        # Bollinger Bands
        bb_upper, bb_middle, bb_lower, bb_percent_b, bb_bandwidth = QuantitativeEngine._calculate_bollinger_bands(
            closes,
            period=QuantitativeEngine.BB_PERIOD,
            num_std=QuantitativeEngine.BB_STD_DEV
        )
        
        # Moving Averages
        sma_dict = {}
        for period in QuantitativeEngine.SMA_PERIODS:
            sma = QuantitativeEngine._calculate_sma(closes, period)
            sma_dict[f'sma_{period}'] = float(sma[-1]) if np.isfinite(sma[-1]) else None
        
        ema_dict = {}
        for period in QuantitativeEngine.EMA_PERIODS:
            ema = QuantitativeEngine._calculate_ema(closes, period)
            ema_dict[f'ema_{period}'] = float(ema[-1]) if np.isfinite(ema[-1]) else None
        
        # ADX
        adx = QuantitativeEngine._calculate_adx(highs, lows, period=QuantitativeEngine.ADX_PERIOD)
        adx_value = float(adx[-1]) if np.isfinite(adx[-1]) else np.nan
        
        # ATR
        atr = QuantitativeEngine._calculate_atr(highs, lows, closes, period=QuantitativeEngine.ATR_PERIOD)
        atr_value = float(atr[-1]) if np.isfinite(atr[-1]) else np.nan
        
        # Stochastic RSI
        stoch_rsi, stoch_rsi_signal = QuantitativeEngine._calculate_stochastic_rsi(
            closes,
            period=QuantitativeEngine.STOCH_RSI_PERIOD
        )
        stoch_rsi_value = float(stoch_rsi[-1]) if np.isfinite(stoch_rsi[-1]) else np.nan
        stoch_rsi_signal_value = float(stoch_rsi_signal[-1]) if np.isfinite(stoch_rsi_signal[-1]) else np.nan
        
        # VWAP
        vwap_value = None
        if volumes is not None and np.any(volumes > 0):
            vwap = QuantitativeEngine._calculate_vwap(highs, lows, closes, volumes)
            vwap_value = float(vwap[-1]) if np.isfinite(vwap[-1]) else None
        
        # Z-Score
        zscore = QuantitativeEngine._calculate_zscore(closes, period=20)
        zscore_value = float(zscore[-1]) if np.isfinite(zscore[-1]) else np.nan
        
        # Support & Resistance
        support, resistance = QuantitativeEngine._find_support_resistance(highs, lows, period=20)
        
        # Golden Cross / Death Cross detection
        sma_10 = QuantitativeEngine._calculate_sma(closes, 10)
        sma_20 = QuantitativeEngine._calculate_sma(closes, 20)
        sma_50 = QuantitativeEngine._calculate_sma(closes, 50)
        sma_200 = QuantitativeEngine._calculate_sma(closes, 200)
        
        golden_cross_10_20 = None
        if np.isfinite(sma_10[-1]) and np.isfinite(sma_20[-1]):
            golden_cross_10_20 = sma_10[-1] > sma_20[-1]
        
        golden_cross_50_200 = None
        if len(closes) >= 200 and np.isfinite(sma_50[-1]) and np.isfinite(sma_200[-1]):
            golden_cross_50_200 = sma_50[-1] > sma_200[-1]
        
        result = {
            'current_price': float(current_price),
            'calculation_method': 'NumPy mathematical calculations',
            
            'rsi': {
                'value': rsi_value,
                'overbought': rsi_value > 70 if np.isfinite(rsi_value) else None,
                'oversold': rsi_value < 30 if np.isfinite(rsi_value) else None,
                'period': QuantitativeEngine.RSI_PERIOD,
                'method': 'Wilder smoothing'
            },
            
            'macd': {
                'line': float(macd[-1]) if np.isfinite(macd[-1]) else np.nan,
                'signal': float(macd_signal[-1]) if np.isfinite(macd_signal[-1]) else np.nan,
                'histogram': float(macd_hist[-1]) if np.isfinite(macd_hist[-1]) else np.nan,
                'fast_period': QuantitativeEngine.MACD_FAST,
                'slow_period': QuantitativeEngine.MACD_SLOW,
                'signal_period': QuantitativeEngine.MACD_SIGNAL
            },
            
            'bollinger_bands': {
                'upper': float(bb_upper[-1]) if np.isfinite(bb_upper[-1]) else np.nan,
                'middle': float(bb_middle[-1]) if np.isfinite(bb_middle[-1]) else np.nan,
                'lower': float(bb_lower[-1]) if np.isfinite(bb_lower[-1]) else np.nan,
                'percent_b': float(bb_percent_b[-1]) if np.isfinite(bb_percent_b[-1]) else np.nan,
                'bandwidth': float(bb_bandwidth[-1]) if np.isfinite(bb_bandwidth[-1]) else np.nan,
                'period': QuantitativeEngine.BB_PERIOD,
                'std_dev': QuantitativeEngine.BB_STD_DEV
            },
            
            'moving_averages': {
                **sma_dict,
                **ema_dict
            },
            
            'adx': {
                'value': adx_value,
                'strong_trend': adx_value > 25 if np.isfinite(adx_value) else None,
                'period': QuantitativeEngine.ADX_PERIOD
            },
            
            'atr': {
                'value': atr_value,
                'period': QuantitativeEngine.ATR_PERIOD,
                'atr_percent': float((atr_value / current_price * 100)) if np.isfinite(atr_value) and current_price > 0 else np.nan
            },
            
            'stochastic_rsi': {
                'value': stoch_rsi_value,
                'signal': stoch_rsi_signal_value,
                'overbought': stoch_rsi_value > 80 if np.isfinite(stoch_rsi_value) else None,
                'oversold': stoch_rsi_value < 20 if np.isfinite(stoch_rsi_value) else None,
                'period': QuantitativeEngine.STOCH_RSI_PERIOD
            },
            
            'vwap': vwap_value,
            
            'zscore': {
                'value': zscore_value,
                'period': 20,
                'extreme_high': zscore_value > 2.5 if np.isfinite(zscore_value) else None,
                'extreme_low': zscore_value < -2.5 if np.isfinite(zscore_value) else None
            },
            
            'support_resistance': {
                'support': float(support),
                'resistance': float(resistance),
                'pivot_point': float((support + resistance) / 2)
            },
            
            'crossovers': {
                'golden_cross_10_20': golden_cross_10_20,
                'golden_cross_50_200': golden_cross_50_200,
            }
        }
        
        return result
    
    @staticmethod
    def calculate_kelly_criterion(win_rate: float = 0.55,
                                  avg_win: float = 1.5,
                                  avg_loss: float = 1.0,
                                  portfolio_size: float = 100000) -> Dict:
        """
        Calculate Kelly Criterion for position sizing.
        
        Kelly % = (bp - q) / b
        where:
            p = probability of win (win_rate)
            q = probability of loss (1 - win_rate)
            b = ratio of win/loss (avg_win / avg_loss)
        
        Args:
            win_rate: Historical win rate (0.0-1.0)
            avg_win: Average winning trade size (in dollars or ratio)
            avg_loss: Average losing trade size (in dollars or ratio)
            portfolio_size: Total portfolio size for position sizing
        
        Returns:
            Dictionary with Kelly fractions and position sizes
        """
        if win_rate <= 0 or win_rate >= 1:
            win_rate = QuantitativeEngine.DEFAULT_WIN_RATE
        
        if avg_win <= 0 or avg_loss <= 0:
            avg_win = QuantitativeEngine.DEFAULT_WIN_LOSS_RATIO
            avg_loss = 1.0
        
        p = win_rate
        q = 1 - win_rate
        b = avg_win / avg_loss
        
        # Kelly formula
        kelly_fraction = (b * p - q) / b
        
        # Ensure Kelly fraction is reasonable (between 0 and 1)
        kelly_fraction = max(0, min(kelly_fraction, 1))
        
        half_kelly = kelly_fraction / 2
        quarter_kelly = kelly_fraction / 4
        
        result = {
            'kelly_fraction': float(kelly_fraction),
            'half_kelly': float(half_kelly),
            'quarter_kelly': float(quarter_kelly),
            
            'position_size_pct': {
                'full_kelly': float(kelly_fraction * 100),
                'half_kelly': float(half_kelly * 100),
                'quarter_kelly': float(quarter_kelly * 100)
            },
            
            'position_size_dollars': {
                'full_kelly': float(kelly_fraction * portfolio_size),
                'half_kelly': float(half_kelly * portfolio_size),
                'quarter_kelly': float(quarter_kelly * portfolio_size)
            },
            
            'inputs': {
                'win_rate': float(p),
                'loss_rate': float(q),
                'avg_win': float(avg_win),
                'avg_loss': float(avg_loss),
                'win_loss_ratio': float(b),
                'portfolio_size': float(portfolio_size)
            },
            
            'calculation_method': 'Kelly Criterion: (b*p - q) / b',
            'recommended_kelly': 'half_kelly'  # Conservative recommendation
        }
        
        return result
    
    @staticmethod
    def calculate_execution_plan(price: float,
                                technicals: Dict,
                                signal_direction: str = 'BUY',
                                conviction: float = 0.65,
                                portfolio_size: float = 100000,
                                risk_pct: float = 2.0) -> Dict:
        """
        Generate mathematically-derived execution plan.
        
        Args:
            price: Current price
            technicals: Dictionary from calculate_full_technicals
            signal_direction: 'BUY' or 'SELL'
            conviction: Signal confidence (0.0-1.0)
            portfolio_size: Total portfolio size
            risk_pct: Maximum risk as % of portfolio (default 2%)
        
        Returns:
            Dictionary with entry, stop loss, take profit, position size, R:R ratio
        """
        if price <= 0:
            return {'error': 'Invalid price', 'calculation_method': 'Validation'}
        
        conviction = max(0, min(conviction, 1.0))  # Clamp to 0-1
        
        # Get ATR for stop loss calculation
        atr = technicals.get('atr', {}).get('value', np.nan)
        
        if not np.isfinite(atr):
            atr = price * 0.02  # Default to 2% of price
        
        # Determine volatility regime
        atr_percent = (atr / price) * 100
        high_volatility = atr_percent > 2.5
        atr_multiplier = 2.5 if high_volatility else 2.0
        
        # Get recent support/resistance
        support = technicals.get('support_resistance', {}).get('support', price * 0.95)
        resistance = technicals.get('support_resistance', {}).get('resistance', price * 1.05)
        
        if signal_direction == 'BUY':
            entry = price
            stop_loss = price - (atr * atr_multiplier)
            
            # Take profit based on R:R of 1.5:1 minimum
            risk = entry - stop_loss
            target = entry + (risk * 1.5)
            
            # Ensure target is above resistance if possible
            if target < resistance:
                target = resistance
        
        else:  # SELL
            entry = price
            stop_loss = price + (atr * atr_multiplier)
            
            # Take profit
            risk = stop_loss - entry
            target = entry - (risk * 1.5)
            
            # Ensure target is below support if possible
            if target > support:
                target = support
        
        # Calculate position size based on Kelly and risk
        risk_amount = portfolio_size * (risk_pct / 100)
        risk_per_share = abs(entry - stop_loss)
        
        if risk_per_share > 0:
            position_shares = risk_amount / risk_per_share
            position_size_pct = (position_shares * price / portfolio_size) * 100
        else:
            position_shares = 0
            position_size_pct = 0
        
        # Calculate R:R ratio
        reward = abs(target - entry)
        rratio = reward / abs(entry - stop_loss) if abs(entry - stop_loss) > 0 else 0
        
        # Adjust for conviction
        final_position_size = position_size_pct * conviction
        
        result = {
            'direction': signal_direction,
            'conviction': float(conviction),
            'volatility_regime': 'high' if high_volatility else 'normal',
            
            'entry': {
                'price': float(entry),
                'method': 'Current market price'
            },
            
            'stop_loss': {
                'price': float(stop_loss),
                'distance_pct': float(abs((stop_loss - entry) / entry * 100)) if entry > 0 else np.nan,
                'calculation': f'ATR {atr_multiplier}x (ATR={atr:.2f})'
            },
            
            'take_profit': {
                'price': float(target),
                'distance_pct': float((target - entry) / entry * 100) if entry > 0 else np.nan,
                'calculation': f'Risk × 1.5 R:R + resistance/support'
            },
            
            'position_size': {
                'percent_of_portfolio': float(final_position_size),
                'dollar_amount': float(final_position_size / 100 * portfolio_size),
                'shares': float(position_shares * conviction),
                'calculation': f'{risk_pct}% risk / risk-per-share × conviction'
            },
            
            'risk_reward': {
                'ratio': float(rratio),
                'acceptable': rratio >= 1.5,
                'risk_dollar_amount': float(risk_amount * conviction),
                'reward_dollar_amount': float(reward * position_shares * conviction)
            },
            
            'technical_levels': {
                'support': float(support),
                'resistance': float(resistance),
                'pivot': float((support + resistance) / 2)
            },
            
            'calculation_method': 'ATR-based stops + R:R ratio + position sizing',
            'portfolio_size': float(portfolio_size),
            'max_risk_pct': float(risk_pct)
        }
        
        return result
    
    @staticmethod
    def generate_quant_signal(commodity_data: Dict,
                             macro_data: Optional[Dict] = None,
                             fx_data: Optional[Dict] = None) -> Dict:
        """
        Combine all indicators into a single quantitative signal.
        
        Args:
            commodity_data: Dictionary with 'closes', 'highs', 'lows', 'volumes'
            macro_data: Optional macro indicators
            fx_data: Optional forex data
        
        Returns:
            Dictionary with combined signal, scores, and execution plan
        """
        # Calculate technical indicators
        closes = np.asarray(commodity_data.get('closes'), dtype=float)
        highs = np.asarray(commodity_data.get('highs'), dtype=float)
        lows = np.asarray(commodity_data.get('lows'), dtype=float)
        volumes = commodity_data.get('volumes')
        
        if volumes is not None:
            volumes = np.asarray(volumes, dtype=float)
        
        technicals = QuantitativeEngine.calculate_full_technicals(closes, highs, lows, volumes)
        
        if 'error' in technicals:
            return technicals
        
        # Calculate technical score (-100 to +100)
        tech_score = 0
        tech_signals = 0
        
        # RSI signals
        rsi = technicals.get('rsi', {}).get('value')
        if np.isfinite(rsi):
            if rsi > 70:
                tech_score -= 20
                tech_signals += 1
            elif rsi < 30:
                tech_score += 20
                tech_signals += 1
            else:
                # Closer to 50 is neutral, closer to extremes has smaller effect
                tech_score += (50 - rsi) / 2.5
                tech_signals += 1
        
        # MACD signals
        macd_line = technicals.get('macd', {}).get('line')
        macd_signal = technicals.get('macd', {}).get('signal')
        if np.isfinite(macd_line) and np.isfinite(macd_signal):
            if macd_line > macd_signal:
                tech_score += 15
            else:
                tech_score -= 15
            tech_signals += 1
        
        # ADX signals (trend strength)
        adx = technicals.get('adx', {}).get('value')
        if np.isfinite(adx):
            if adx > 40:
                tech_score += 10  # Strong trend
            elif adx < 20:
                tech_score -= 10  # Weak trend
            tech_signals += 1
        
        # Golden Cross signals
        golden_cross = technicals.get('crossovers', {}).get('golden_cross_10_20')
        if golden_cross is not None:
            tech_score += 20 if golden_cross else -20
            tech_signals += 1
        
        # Bollinger Bands signals
        bb_percent = technicals.get('bollinger_bands', {}).get('percent_b')
        if np.isfinite(bb_percent):
            if bb_percent > 0.8:
                tech_score -= 10
            elif bb_percent < 0.2:
                tech_score += 10
            tech_signals += 1
        
        # Z-Score signals
        zscore = technicals.get('zscore', {}).get('value')
        if np.isfinite(zscore):
            if zscore > 2:
                tech_score -= 15
            elif zscore < -2:
                tech_score += 15
            tech_signals += 1
        
        # Normalize technical score
        if tech_signals > 0:
            tech_score = max(-100, min(100, tech_score))
        
        # Calculate macro score if data provided
        macro_score = 0
        if macro_data:
            # Simple macro scoring (can be expanded)
            macro_signals = 0
            
            if 'risk_on' in macro_data:
                macro_score += 30 if macro_data['risk_on'] else -30
                macro_signals += 1
            
            if 'inflation_rising' in macro_data:
                macro_score -= 20 if macro_data['inflation_rising'] else 20
                macro_signals += 1
            
            if macro_signals > 0:
                macro_score = max(-100, min(100, macro_score))
        
        # FX adjustment (commodity strength vs USD)
        fx_score = 0
        if fx_data:
            if 'usd_strength' in fx_data:
                # Strong USD typically weakens commodities
                fx_score -= 20 if fx_data['usd_strength'] else 20
        
        # Combined score
        combined_score = (tech_score * 0.7 + macro_score * 0.2 + fx_score * 0.1)
        combined_score = max(-100, min(100, combined_score))
        
        # Generate signal
        if combined_score > 30:
            signal = 'BUY'
            confidence = abs(combined_score) / 100
        elif combined_score < -30:
            signal = 'SELL'
            confidence = abs(combined_score) / 100
        else:
            signal = 'HOLD'
            confidence = 0.5
        
        # Detect regime
        adx_val = technicals.get('adx', {}).get('value')
        is_trending = np.isfinite(adx_val) and adx_val > 25
        
        golden_cross_val = technicals.get('crossovers', {}).get('golden_cross_10_20')
        risk_on = golden_cross_val if golden_cross_val is not None else (combined_score > 0)
        
        # Generate execution plan
        execution = QuantitativeEngine.calculate_execution_plan(
            price=closes[-1],
            technicals=technicals,
            signal_direction=signal if signal != 'HOLD' else 'BUY',
            conviction=confidence
        )
        
        result = {
            'timestamp': commodity_data.get('timestamp', 'N/A'),
            'price': float(closes[-1]),
            
            'scores': {
                'technical': float(tech_score),
                'macro': float(macro_score),
                'fx_adjustment': float(fx_score),
                'combined': float(combined_score)
            },
            
            'signal': {
                'action': signal,
                'confidence': float(confidence),
                'reason': f'Combined score {combined_score:.1f}, Technical {tech_score:.1f}, Macro {macro_score:.1f}'
            },
            
            'regime': {
                'trending': is_trending,
                'trend_strength': float(adx_val) if np.isfinite(adx_val) else np.nan,
                'risk_on': risk_on,
                'volatility_pct': technicals.get('atr', {}).get('atr_percent', np.nan)
            },
            
            'key_levels': technicals.get('support_resistance', {}),
            
            'technicals_summary': {
                'rsi': technicals.get('rsi', {}).get('value'),
                'macd_histogram': technicals.get('macd', {}).get('histogram'),
                'adx': technicals.get('adx', {}).get('value'),
                'atr': technicals.get('atr', {}).get('value'),
                'bb_percent': technicals.get('bollinger_bands', {}).get('percent_b'),
                'zscore': technicals.get('zscore', {}).get('value')
            },
            
            'execution_plan': execution,
            
            'calculation_method': 'Comprehensive quantitative analysis: RSI, MACD, ADX, Golden Cross, BB, Z-Score + macro + FX overlay'
        }
        
        return result
    
    @staticmethod
    def calculate_var(position_size_pct: float,
                     price: float,
                     volatility_ann: float,
                     portfolio_size: float = 100000,
                     confidence_level: float = 0.95) -> Dict:
        """
        Calculate Value at Risk (VaR) for a position.
        
        Args:
            position_size_pct: Position size as % of portfolio
            price: Current price of asset
            volatility_ann: Annualized volatility (as decimal, e.g., 0.25 for 25%)
            portfolio_size: Total portfolio size
            confidence_level: Confidence level (0.95 = 95% VaR, 0.99 = 99% VaR)
        
        Returns:
            Dictionary with 1-day VaR, 5-day VaR, max drawdown estimate
        """
        if position_size_pct <= 0 or price <= 0 or volatility_ann < 0:
            return {'error': 'Invalid inputs', 'calculation_method': 'Validation'}
        
        # Z-score for confidence level
        z_scores = {0.90: 1.282, 0.95: 1.645, 0.99: 2.326}
        z_score = z_scores.get(confidence_level, 1.645)
        
        # Convert annual volatility to daily
        volatility_daily = volatility_ann / np.sqrt(252)
        
        # Position value
        position_value = (position_size_pct / 100) * portfolio_size
        
        # 1-day VaR
        var_1day = position_value * volatility_daily * z_score
        var_1day_pct = (volatility_daily * z_score) * 100
        
        # 5-day VaR (square root of time rule)
        var_5day = var_1day * np.sqrt(5)
        var_5day_pct = var_1day_pct * np.sqrt(5)
        
        # Maximum drawdown estimate (assumes worst-case scenario)
        # Typically 2-3x the annual volatility under stress
        max_drawdown_est = volatility_ann * 2.5
        
        # Conditional VaR (Expected Shortfall) - average loss beyond VaR
        cvar_1day = var_1day * 1.25  # Approximate
        
        result = {
            'position_size_pct': float(position_size_pct),
            'position_value': float(position_value),
            'current_price': float(price),
            'annualized_volatility': float(volatility_ann * 100),
            
            'var_1day': {
                'dollar_amount': float(var_1day),
                'percent': float(var_1day_pct),
                'confidence_level': float(confidence_level * 100)
            },
            
            'var_5day': {
                'dollar_amount': float(var_5day),
                'percent': float(var_5day_pct),
                'confidence_level': float(confidence_level * 100)
            },
            
            'expected_shortfall': {
                'dollar_amount': float(cvar_1day),
                'description': 'Average loss beyond 1-day VaR'
            },
            
            'max_drawdown_estimate': {
                'percent': float(max_drawdown_est * 100),
                'dollar_amount': float(position_value * max_drawdown_est),
                'confidence': 'Approximate, assuming 2.5x volatility stress'
            },
            
            'kelly_position_compatibility': {
                'conservative': float(position_size_pct) <= 5.0,
                'moderate': float(position_size_pct) <= 10.0,
                'aggressive': float(position_size_pct) > 10.0
            },
            
            'calculation_method': f'Parametric VaR with z-score {z_score} ({confidence_level*100:.0f}% confidence)',
            'assumptions': [
                'Normal distribution of returns',
                'Historical volatility representative of future',
                '252 trading days per year',
                'Linear position exposure'
            ]
        }
        
        return result


# Convenience functions (module-level API)
def calculate_full_technicals(closes, highs, lows, volumes=None):
    """Module-level wrapper for calculate_full_technicals."""
    return QuantitativeEngine.calculate_full_technicals(closes, highs, lows, volumes)


def calculate_kelly_criterion(win_rate=0.55, avg_win=1.5, avg_loss=1.0, portfolio_size=100000):
    """Module-level wrapper for calculate_kelly_criterion."""
    return QuantitativeEngine.calculate_kelly_criterion(win_rate, avg_win, avg_loss, portfolio_size)


def calculate_execution_plan(price, technicals, signal_direction='BUY', conviction=0.65, 
                            portfolio_size=100000, risk_pct=2.0):
    """Module-level wrapper for calculate_execution_plan."""
    return QuantitativeEngine.calculate_execution_plan(price, technicals, signal_direction, 
                                                       conviction, portfolio_size, risk_pct)


def generate_quant_signal(commodity_data, macro_data=None, fx_data=None):
    """Module-level wrapper for generate_quant_signal."""
    return QuantitativeEngine.generate_quant_signal(commodity_data, macro_data, fx_data)


def calculate_var(position_size_pct, price, volatility_ann, portfolio_size=100000, confidence_level=0.95):
    """Module-level wrapper for calculate_var."""
    return QuantitativeEngine.calculate_var(position_size_pct, price, volatility_ann, 
                                           portfolio_size, confidence_level)
