"""
Real Market Data Service for Forex Trading
Integrates multiple free APIs for historical and real-time data
"""

import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
import json
import logging
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class RealMarketData:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    bid: float
    ask: float
    spread: float
    change_24h: float
    change_percent_24h: float
    source: str

@dataclass
class TechnicalIndicators:
    symbol: str
    timeframe: str
    timestamp: datetime
    sma_20: float
    sma_50: float
    sma_200: float
    ema_20: float
    ema_50: float
    ema_200: float
    rsi_14: float
    macd_line: float
    macd_signal: float
    macd_histogram: float
    bollinger_upper: float
    bollinger_middle: float
    bollinger_lower: float
    atr_14: float
    stoch_k: float
    stoch_d: float
    williams_r: float
    cci_20: float
    support_level: float
    resistance_level: float
    trend_direction: str
    trend_strength: float
    volatility: float

class RealMarketDataManager:
    def __init__(self):
        self.session = None
        self.cache = {}
        self.cache_ttl = 60  # 1 minute cache
        
        # Free API endpoints
        self.apis = {
            "yahoo": "https://query1.finance.yahoo.com/v8/finance/chart/",
            "exchangerate": "https://api.exchangerate-api.com/v4/latest/",
            "fixer": "http://data.fixer.io/api/latest",
            "alpha_vantage_free": "https://www.alphavantage.co/query",
            "twelve_data_free": "https://api.twelvedata.com/",
        }
        
        # Currency mappings for different providers
        self.symbol_mappings = {
            "yahoo": {
                "EURUSD": "EURUSD=X",
                "GBPUSD": "GBPUSD=X",
                "USDJPY": "USDJPY=X",
                "AUDUSD": "AUDUSD=X",
                "USDCAD": "USDCAD=X",
                "EURJPY": "EURJPY=X",
                "GBPJPY": "GBPJPY=X",
                "EURGBP": "EURGBP=X",
                "USDCHF": "USDCHF=X",
                "AUDCAD": "AUDCAD=X"
            }
        }
    
    async def initialize(self):
        """Initialize HTTP session"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        )
    
    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
    
    async def get_yahoo_finance_data(self, symbol: str, period: str = "1d", interval: str = "1m") -> Optional[RealMarketData]:
        """Get real-time data from Yahoo Finance"""
        try:
            yahoo_symbol = self.symbol_mappings["yahoo"].get(symbol, f"{symbol}=X")
            url = f"{self.apis['yahoo']}{yahoo_symbol}"
            
            params = {
                "period1": int((datetime.now() - timedelta(days=1)).timestamp()),
                "period2": int(datetime.now().timestamp()),
                "interval": interval,
                "includePrePost": "true",
                "events": "div,splits"
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    chart_result = data.get("chart", {}).get("result", [])
                    if not chart_result:
                        return None
                    
                    result = chart_result[0]
                    meta = result.get("meta", {})
                    indicators = result.get("indicators", {}).get("quote", [{}])[0]
                    timestamps = result.get("timestamp", [])
                    
                    if not timestamps:
                        return None
                    
                    # Get the latest data point
                    latest_idx = -1
                    latest_timestamp = datetime.fromtimestamp(timestamps[latest_idx], tz=timezone.utc)
                    
                    # Extract OHLCV data
                    opens = indicators.get("open", [])
                    highs = indicators.get("high", [])
                    lows = indicators.get("low", [])
                    closes = indicators.get("close", [])
                    volumes = indicators.get("volume", [])
                    
                    if not closes or len(closes) <= abs(latest_idx):
                        return None
                    
                    current_price = closes[latest_idx]
                    open_price = opens[latest_idx] if opens and len(opens) > abs(latest_idx) else current_price
                    high_price = highs[latest_idx] if highs and len(highs) > abs(latest_idx) else current_price
                    low_price = lows[latest_idx] if lows and len(lows) > abs(latest_idx) else current_price
                    volume = volumes[latest_idx] if volumes and len(volumes) > abs(latest_idx) else 0
                    
                    # Calculate 24h change
                    if len(closes) > 1:
                        prev_close = closes[0]  # First price of the day
                        change_24h = current_price - prev_close
                        change_percent_24h = (change_24h / prev_close) * 100 if prev_close else 0
                    else:
                        change_24h = 0
                        change_percent_24h = 0
                    
                    # Calculate bid/ask spread (typical forex spreads)
                    if symbol.endswith("JPY"):
                        spread = 0.001  # 0.1 pips for JPY pairs
                    else:
                        spread = 0.00002  # 0.2 pips for major pairs
                    
                    bid = current_price - spread / 2
                    ask = current_price + spread / 2
                    
                    return RealMarketData(
                        symbol=symbol,
                        timestamp=latest_timestamp,
                        open=round(open_price, 5),
                        high=round(high_price, 5),
                        low=round(low_price, 5),
                        close=round(current_price, 5),
                        volume=int(volume) if volume else 0,
                        bid=round(bid, 5),
                        ask=round(ask, 5),
                        spread=spread,
                        change_24h=round(change_24h, 5),
                        change_percent_24h=round(change_percent_24h, 2),
                        source="yahoo_finance"
                    )
                    
        except Exception as e:
            logger.error(f"Yahoo Finance error for {symbol}: {e}")
            return None
    
    def get_historical_data_sync(self, symbol: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
        """Get historical data using yfinance (synchronous)"""
        try:
            yahoo_symbol = self.symbol_mappings["yahoo"].get(symbol, f"{symbol}=X")
            ticker = yf.Ticker(yahoo_symbol)
            
            # Get historical data
            hist = ticker.history(period=period, interval=interval)
            
            if hist.empty:
                logger.warning(f"No historical data found for {symbol}")
                return pd.DataFrame()
            
            # Clean the data
            hist = hist.dropna()
            
            # Add additional columns
            hist['Symbol'] = symbol
            hist['Change'] = hist['Close'].pct_change()
            hist['Change_Percent'] = hist['Change'] * 100
            
            # Calculate simple moving averages
            hist['SMA_20'] = hist['Close'].rolling(window=20).mean()
            hist['SMA_50'] = hist['Close'].rolling(window=50).mean()
            hist['SMA_200'] = hist['Close'].rolling(window=200).mean()
            
            logger.info(f"Retrieved {len(hist)} historical records for {symbol}")
            return hist
            
        except Exception as e:
            logger.error(f"Historical data error for {symbol}: {e}")
            return pd.DataFrame()
    
    def calculate_technical_indicators(self, df: pd.DataFrame) -> TechnicalIndicators:
        """Calculate comprehensive technical indicators from historical data"""
        try:
            if df.empty or len(df) < 50:
                logger.warning("Insufficient data for technical indicators")
                return self._get_default_indicators()
            
            close = df['Close'].values
            high = df['High'].values
            low = df['Low'].values
            volume = df['Volume'].values if 'Volume' in df.columns else np.zeros(len(close))
            
            # Simple Moving Averages
            sma_20 = self._sma(close, 20)
            sma_50 = self._sma(close, 50)
            sma_200 = self._sma(close, 200) if len(close) >= 200 else sma_50
            
            # Exponential Moving Averages
            ema_20 = self._ema(close, 20)
            ema_50 = self._ema(close, 50)
            ema_200 = self._ema(close, 200) if len(close) >= 200 else ema_50
            
            # RSI
            rsi_14 = self._rsi(close, 14)
            
            # MACD
            macd_line, macd_signal, macd_histogram = self._macd(close)
            
            # Bollinger Bands
            bb_upper, bb_middle, bb_lower = self._bollinger_bands(close, 20, 2)
            
            # ATR
            atr_14 = self._atr(high, low, close, 14)
            
            # Stochastic Oscillator
            stoch_k, stoch_d = self._stochastic(high, low, close, 14, 3)
            
            # Williams %R
            williams_r = self._williams_r(high, low, close, 14)
            
            # Commodity Channel Index
            cci_20 = self._cci(high, low, close, 20)
            
            # Support and Resistance
            support, resistance = self._support_resistance(high, low, close)
            
            # Trend Analysis
            trend_direction, trend_strength = self._trend_analysis(ema_20, ema_50, ema_200, close)
            
            # Volatility
            volatility = self._volatility(close)
            
            symbol = df['Symbol'].iloc[-1] if 'Symbol' in df.columns else "UNKNOWN"
            
            return TechnicalIndicators(
                symbol=symbol,
                timeframe="1d",
                timestamp=datetime.now(timezone.utc),
                sma_20=sma_20,
                sma_50=sma_50,
                sma_200=sma_200,
                ema_20=ema_20,
                ema_50=ema_50,
                ema_200=ema_200,
                rsi_14=rsi_14,
                macd_line=macd_line,
                macd_signal=macd_signal,
                macd_histogram=macd_histogram,
                bollinger_upper=bb_upper,
                bollinger_middle=bb_middle,
                bollinger_lower=bb_lower,
                atr_14=atr_14,
                stoch_k=stoch_k,
                stoch_d=stoch_d,
                williams_r=williams_r,
                cci_20=cci_20,
                support_level=support,
                resistance_level=resistance,
                trend_direction=trend_direction,
                trend_strength=trend_strength,
                volatility=volatility
            )
            
        except Exception as e:
            logger.error(f"Technical indicators calculation error: {e}")
            return self._get_default_indicators()
    
    def _sma(self, data: np.ndarray, period: int) -> float:
        """Simple Moving Average"""
        if len(data) < period:
            return data[-1] if len(data) > 0 else 0
        return np.mean(data[-period:])
    
    def _ema(self, data: np.ndarray, period: int) -> float:
        """Exponential Moving Average"""
        if len(data) < period:
            return data[-1] if len(data) > 0 else 0
        
        alpha = 2 / (period + 1)
        ema = data[0]
        for price in data[1:]:
            ema = alpha * price + (1 - alpha) * ema
        return ema
    
    def _rsi(self, data: np.ndarray, period: int = 14) -> float:
        """Relative Strength Index"""
        if len(data) < period + 1:
            return 50.0
        
        deltas = np.diff(data)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _macd(self, data: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple:
        """MACD Indicator"""
        if len(data) < slow:
            return 0.0, 0.0, 0.0
        
        ema_fast = self._ema(data, fast)
        ema_slow = self._ema(data, slow)
        macd_line = ema_fast - ema_slow
        
        # For signal line, we need more data points
        if len(data) < slow + signal:
            return macd_line, macd_line, 0.0
        
        # Calculate MACD signal line (simplified)
        macd_signal = macd_line * 0.9  # Approximation
        macd_histogram = macd_line - macd_signal
        
        return macd_line, macd_signal, macd_histogram
    
    def _bollinger_bands(self, data: np.ndarray, period: int = 20, std_dev: int = 2) -> tuple:
        """Bollinger Bands"""
        if len(data) < period:
            current_price = data[-1] if len(data) > 0 else 1.0
            return current_price * 1.01, current_price, current_price * 0.99
        
        sma = np.mean(data[-period:])
        std = np.std(data[-period:])
        
        upper = sma + (std * std_dev)
        lower = sma - (std * std_dev)
        
        return upper, sma, lower
    
    def _atr(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> float:
        """Average True Range"""
        if len(high) < period + 1:
            return 0.001
        
        tr1 = high[-period:] - low[-period:]
        tr2 = np.abs(high[-period:] - close[-period-1:-1])
        tr3 = np.abs(low[-period:] - close[-period-1:-1])
        
        true_range = np.maximum(tr1, np.maximum(tr2, tr3))
        atr = np.mean(true_range)
        
        return atr
    
    def _stochastic(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, 
                   k_period: int = 14, d_period: int = 3) -> tuple:
        """Stochastic Oscillator"""
        if len(high) < k_period:
            return 50.0, 50.0
        
        lowest_low = np.min(low[-k_period:])
        highest_high = np.max(high[-k_period:])
        
        if highest_high == lowest_low:
            k_percent = 50.0
        else:
            k_percent = ((close[-1] - lowest_low) / (highest_high - lowest_low)) * 100
        
        # Simplified %D calculation
        d_percent = k_percent * 0.9  # Approximation
        
        return k_percent, d_percent
    
    def _williams_r(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> float:
        """Williams %R"""
        if len(high) < period:
            return -50.0
        
        highest_high = np.max(high[-period:])
        lowest_low = np.min(low[-period:])
        
        if highest_high == lowest_low:
            return -50.0
        
        williams_r = ((highest_high - close[-1]) / (highest_high - lowest_low)) * -100
        return williams_r
    
    def _cci(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 20) -> float:
        """Commodity Channel Index"""
        if len(high) < period:
            return 0.0
        
        typical_price = (high[-period:] + low[-period:] + close[-period:]) / 3
        sma_tp = np.mean(typical_price)
        mean_deviation = np.mean(np.abs(typical_price - sma_tp))
        
        if mean_deviation == 0:
            return 0.0
        
        cci = (typical_price[-1] - sma_tp) / (0.015 * mean_deviation)
        return cci
    
    def _support_resistance(self, high: np.ndarray, low: np.ndarray, close: np.ndarray) -> tuple:
        """Calculate Support and Resistance levels"""
        period = min(20, len(high))
        
        if period < 5:
            current = close[-1] if len(close) > 0 else 1.0
            return current * 0.99, current * 1.01
        
        recent_high = high[-period:]
        recent_low = low[-period:]
        
        resistance = np.max(recent_high)
        support = np.min(recent_low)
        
        return support, resistance
    
    def _trend_analysis(self, ema_20: float, ema_50: float, ema_200: float, close: np.ndarray) -> tuple:
        """Analyze trend direction and strength"""
        if len(close) < 2:
            return "SIDEWAYS", 0.5
        
        # Trend direction based on EMA alignment
        if ema_20 > ema_50 > ema_200:
            trend = "STRONG_UP"
            strength = 0.8
        elif ema_20 > ema_50:
            trend = "UP"
            strength = 0.6
        elif ema_20 < ema_50 < ema_200:
            trend = "STRONG_DOWN"
            strength = 0.8
        elif ema_20 < ema_50:
            trend = "DOWN"
            strength = 0.6
        else:
            trend = "SIDEWAYS"
            strength = 0.3
        
        return trend, strength
    
    def _volatility(self, close: np.ndarray, period: int = 20) -> float:
        """Calculate volatility as standard deviation of returns"""
        if len(close) < period:
            return 1.0
        
        returns = np.diff(close[-period:]) / close[-period-1:-1]
        volatility = np.std(returns) * 100  # Convert to percentage
        
        return volatility
    
    def _get_default_indicators(self) -> TechnicalIndicators:
        """Return default indicators when calculation fails"""
        return TechnicalIndicators(
            symbol="UNKNOWN",
            timeframe="1d",
            timestamp=datetime.now(timezone.utc),
            sma_20=1.0, sma_50=1.0, sma_200=1.0,
            ema_20=1.0, ema_50=1.0, ema_200=1.0,
            rsi_14=50.0, macd_line=0.0, macd_signal=0.0, macd_histogram=0.0,
            bollinger_upper=1.01, bollinger_middle=1.0, bollinger_lower=0.99,
            atr_14=0.001, stoch_k=50.0, stoch_d=50.0, williams_r=-50.0, cci_20=0.0,
            support_level=0.99, resistance_level=1.01, trend_direction="SIDEWAYS",
            trend_strength=0.5, volatility=1.0
        )
    
    async def get_multiple_pairs_data(self, symbols: List[str]) -> Dict[str, RealMarketData]:
        """Get real-time data for multiple currency pairs"""
        results = {}
        
        tasks = []
        for symbol in symbols:
            task = self.get_yahoo_finance_data(symbol)
            tasks.append((symbol, task))
        
        for symbol, task in tasks:
            try:
                data = await task
                if data:
                    results[symbol] = data
                else:
                    logger.warning(f"No data received for {symbol}")
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}")
        
        return results
    
    def backtest_historical_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Get historical data for backtesting"""
        try:
            yahoo_symbol = self.symbol_mappings["yahoo"].get(symbol, f"{symbol}=X")
            ticker = yf.Ticker(yahoo_symbol)
            
            # Get historical data with specific date range
            hist = ticker.history(start=start_date, end=end_date, interval="1d")
            
            if hist.empty:
                logger.warning(f"No historical data found for {symbol} between {start_date} and {end_date}")
                return pd.DataFrame()
            
            # Clean and enhance the data
            hist = hist.dropna()
            hist['Symbol'] = symbol
            hist['Date'] = hist.index
            hist['Change'] = hist['Close'].pct_change()
            hist['Change_Percent'] = hist['Change'] * 100
            
            # Add technical indicators
            hist['SMA_20'] = hist['Close'].rolling(window=20).mean()
            hist['SMA_50'] = hist['Close'].rolling(window=50).mean()
            hist['SMA_200'] = hist['Close'].rolling(window=200).mean()
            
            # Calculate RSI
            delta = hist['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            hist['RSI'] = 100 - (100 / (1 + rs))
            
            logger.info(f"Retrieved {len(hist)} historical records for {symbol} from {start_date} to {end_date}")
            return hist
            
        except Exception as e:
            logger.error(f"Backtesting data error for {symbol}: {e}")
            return pd.DataFrame()

# Global instance
real_data_manager = RealMarketDataManager()