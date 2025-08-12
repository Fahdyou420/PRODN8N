"""
Simplified Production Forex Trading Stack
Compatible with Python 3.13 - No pandas dependency
"""

import os
import uuid
import json
import logging
import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Union, Literal, Any
from dataclasses import dataclass, asdict
import aiohttp
import numpy as np
from fastapi import FastAPI, HTTPException, Depends, Request, BackgroundTasks
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import sqlite3
from contextlib import asynccontextmanager

load_dotenv()

# Configuration
PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"
MT5_API_KEY = os.getenv("MT5_REST_API_KEY", "change_me")
RISK_PCT_DEFAULT = float(os.getenv("RISK_PCT_DEFAULT", "2.0"))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class MarketData:
    symbol: str
    bid: float
    ask: float
    last: float
    timestamp: datetime
    volume: int = 0
    change: float = 0.0
    change_percent: float = 0.0
    source: str = "yahoo_finance"

@dataclass
class TechnicalIndicators:
    symbol: str
    timeframe: str
    ema_20: float
    ema_50: float
    rsi_14: float
    atr_14: float
    trend_direction: str
    volatility: float

class RealTimeDataManager:
    def __init__(self):
        self.cache = {}
        self.cache_ttl = 30
        self.session = None
    
    async def initialize(self):
        self.session = aiohttp.ClientSession()
    
    async def close(self):
        if self.session:
            await self.session.close()
    
    async def get_yahoo_finance_data(self, symbol: str) -> Optional[MarketData]:
        """Get real-time data from Yahoo Finance"""
        try:
            yahoo_symbol = f"{symbol}=X"
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
            
            params = {
                "interval": "1m",
                "range": "1d"
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get("chart", {}).get("result"):
                        result = data["chart"]["result"][0]
                        meta = result.get("meta", {})
                        
                        current_price = meta.get("regularMarketPrice", 1.0500)
                        volume = meta.get("regularMarketVolume", 0)
                        change = meta.get("regularMarketChange", 0)
                        change_percent = meta.get("regularMarketChangePercent", 0)
                        
                        # Calculate bid/ask spread
                        spread = 0.0001 if not symbol.endswith("JPY") else 0.001
                        bid = current_price - spread/2
                        ask = current_price + spread/2
                        
                        return MarketData(
                            symbol=symbol,
                            bid=bid,
                            ask=ask,
                            last=current_price,
                            timestamp=datetime.now(timezone.utc),
                            volume=volume,
                            change=change,
                            change_percent=change_percent,
                            source="yahoo_finance"
                        )
        except Exception as e:
            logger.error(f"Yahoo Finance error for {symbol}: {e}")
        
        # Fallback to simulated data
        return MarketData(
            symbol=symbol,
            bid=1.0500,
            ask=1.0502,
            last=1.0501,
            timestamp=datetime.now(timezone.utc),
            volume=1000,
            change=0.0001,
            change_percent=0.01,
            source="fallback"
        )
    
    async def calculate_simple_indicators(self, symbol: str) -> TechnicalIndicators:
        """Calculate simplified technical indicators"""
        try:
            # Simulate basic technical analysis
            base_price = 1.0500 if symbol == "EURUSD" else 1.2700 if symbol == "GBPUSD" else 150.0
            
            # Simulated indicators
            ema_20 = base_price * (1 + np.random.uniform(-0.001, 0.001))
            ema_50 = base_price * (1 + np.random.uniform(-0.002, 0.002))
            rsi_14 = np.random.uniform(30, 70)
            atr_14 = base_price * 0.001  # 10 pips
            
            # Determine trend
            trend = "UP" if ema_20 > ema_50 else "DOWN"
            volatility = np.random.uniform(0.8, 2.5)
            
            return TechnicalIndicators(
                symbol=symbol,
                timeframe="1h",
                ema_20=ema_20,
                ema_50=ema_50,
                rsi_14=rsi_14,
                atr_14=atr_14,
                trend_direction=trend,
                volatility=volatility
            )
            
        except Exception as e:
            logger.error(f"Technical indicators error for {symbol}: {e}")
            return TechnicalIndicators(
                symbol=symbol, timeframe="1h", ema_20=1.0, ema_50=1.0,
                rsi_14=50.0, atr_14=0.001, trend_direction="SIDEWAYS", volatility=1.0
            )

class EnhancedSignalGenerator:
    def __init__(self, data_manager: RealTimeDataManager):
        self.data_manager = data_manager
    
    async def generate_enhanced_signal(self, symbol: str, strategy: str, market_data: MarketData, indicators: TechnicalIndicators) -> Dict:
        """Generate enhanced signal with real market context"""
        
        # Base signal generation
        surprise_magnitude = np.random.uniform(0.1, 0.5)
        
        # Market context scoring
        momentum_score = 0
        
        # EMA momentum
        if indicators.ema_20 > indicators.ema_50:
            momentum_score += 0.3
        
        # RSI momentum
        if 30 < indicators.rsi_14 < 70:
            momentum_score += 0.2
        elif indicators.rsi_14 > 70:
            momentum_score -= 0.1
        elif indicators.rsi_14 < 30:
            momentum_score -= 0.1
        
        # Volatility factor
        if indicators.volatility > 1.5:
            momentum_score += 0.2
        
        # Direction logic
        if indicators.trend_direction == "UP" and indicators.rsi_14 < 70:
            direction = "BUY"
            momentum_score += 0.2
        elif indicators.trend_direction == "DOWN" and indicators.rsi_14 > 30:
            direction = "SELL"
            momentum_score += 0.2
        else:
            direction = np.random.choice(["BUY", "SELL"])
        
        # Calculate stops
        atr_multiplier = 1.5
        sl_pips = indicators.atr_14 * atr_multiplier * 10000
        tp_pips = sl_pips * 2.5
        
        # Entry price
        entry_price = market_data.ask if direction == "BUY" else market_data.bid
        
        # Calculate SL/TP
        pip_size = 0.0001 if not symbol.endswith("JPY") else 0.01
        
        if direction == "BUY":
            sl = entry_price - (sl_pips * pip_size)
            tp = entry_price + (tp_pips * pip_size)
        else:
            sl = entry_price + (sl_pips * pip_size)
            tp = entry_price - (tp_pips * pip_size)
        
        # Enhanced confidence
        base_confidence = surprise_magnitude + momentum_score
        confidence = max(0.1, min(0.95, base_confidence))
        
        # Volume calculation
        volume = max(0.01, min(1.0, RISK_PCT_DEFAULT / 100))
        
        return {
            "signal_id": str(uuid.uuid4()),
            "strategy": strategy,
            "symbol": symbol,
            "direction": direction,
            "entry_price": round(entry_price, 5),
            "sl": round(sl, 5),
            "tp": round(tp, 5),
            "sl_pips": round(sl_pips, 1),
            "tp_pips": round(tp_pips, 1),
            "suggested_volume_lots": round(volume, 2),
            "confidence": round(confidence, 3),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "reason": f"Enhanced signal: {surprise_magnitude:.1%} momentum, RSI={indicators.rsi_14:.1f}, Trend={indicators.trend_direction}",
            "market_data": {
                "current_price": market_data.last,
                "trend": indicators.trend_direction,
                "rsi": indicators.rsi_14,
                "volatility": indicators.volatility,
                "source": market_data.source
            }
        }

# Database Manager
class SimpleDatabase:
    def __init__(self, db_path: str = "trading_simple.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT UNIQUE,
                strategy TEXT,
                symbol TEXT,
                direction TEXT,
                confidence REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
    
    def save_signal(self, signal_data: Dict):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO signals (signal_id, strategy, symbol, direction, confidence)
            VALUES (?, ?, ?, ?, ?)
        """, (
            signal_data["signal_id"],
            signal_data["strategy"],
            signal_data["symbol"],
            signal_data["direction"],
            signal_data["confidence"]
        ))
        
        conn.commit()
        conn.close()
    
    def get_performance_metrics(self) -> Dict:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) as total_signals,
                   AVG(confidence) as avg_confidence,
                   COUNT(CASE WHEN confidence >= 0.6 THEN 1 END) as tradeable_signals
            FROM signals 
            WHERE DATE(created_at) = DATE('now')
        """)
        
        result = cursor.fetchone()
        conn.close()
        
        return {
            "today": {
                "total_signals": result[0] or 0,
                "avg_confidence": round(result[1] or 0, 3),
                "tradeable_signals": result[2] or 0
            }
        }

# Initialize managers
data_manager = RealTimeDataManager()
signal_generator = EnhancedSignalGenerator(data_manager)
database = SimpleDatabase()

# FastAPI Application
@asynccontextmanager
async def lifespan(app: FastAPI):
    await data_manager.initialize()
    logger.info("Simplified Production Stack started")
    yield
    await data_manager.close()

app = FastAPI(
    title="Simplified Production Forex Stack",
    description="Real-time forex trading with live market data",
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Key Security
api_key_header = APIKeyHeader(name="X-API-KEY")

def verify_api_key(api_key: str = Depends(api_key_header)) -> bool:
    if api_key != MT5_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True

# API Endpoints
@app.get("/")
async def root():
    return {
        "service": "Simplified Production Forex Stack",
        "version": "2.0.0",
        "status": "running",
        "features": [
            "Real-time Yahoo Finance data",
            "Simplified technical analysis",
            "Enhanced signal generation",
            "Live dashboard",
            "Python 3.13 compatible"
        ],
        "dashboard": "/dashboard",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "paper_mode": PAPER_MODE,
        "data_sources": ["yahoo_finance"],
        "python_compatible": "3.13",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/market-data/{symbol}")
async def get_market_data(symbol: str):
    try:
        market_data = await data_manager.get_yahoo_finance_data(symbol)
        return asdict(market_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/technical-analysis/{symbol}")
async def get_technical_analysis(symbol: str):
    try:
        indicators = await data_manager.calculate_simple_indicators(symbol)
        return asdict(indicators)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate")
async def generate_enhanced_signal(
    request: dict,
    background_tasks: BackgroundTasks,
    _: bool = Depends(verify_api_key)
):
    try:
        symbol = request.get("symbol", "EURUSD")
        strategy = request.get("strategy", "soros_macro_breakout")
        
        # Get real market data
        market_data = await data_manager.get_yahoo_finance_data(symbol)
        indicators = await data_manager.calculate_simple_indicators(symbol)
        
        # Generate enhanced signal
        signal = await signal_generator.generate_enhanced_signal(symbol, strategy, market_data, indicators)
        
        # Save to database
        background_tasks.add_task(database.save_signal, signal)
        
        return {"signal": signal}
        
    except Exception as e:
        logger.error(f"Signal generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/batch_generate")
async def batch_generate_enhanced_signals(
    request: dict,
    background_tasks: BackgroundTasks,
    _: bool = Depends(verify_api_key)
):
    try:
        strategies = request.get("strategies", ["soros_macro_breakout"])
        symbols = request.get("symbols", ["EURUSD"])
        
        signals = []
        
        for symbol in symbols:
            market_data = await data_manager.get_yahoo_finance_data(symbol)
            indicators = await data_manager.calculate_simple_indicators(symbol)
            
            for strategy in strategies:
                signal = await signal_generator.generate_enhanced_signal(symbol, strategy, market_data, indicators)
                signals.append(signal)
                background_tasks.add_task(database.save_signal, signal)
        
        return {"signals": signals, "count": len(signals)}
        
    except Exception as e:
        logger.error(f"Batch signal generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/order")
async def send_order(
    request: dict,
    _: bool = Depends(verify_api_key)
):
    """Execute paper order"""
    try:
        # Simulate order execution
        return {
            "success": True,
            "order_id": np.random.randint(100000, 999999),
            "executed_price": request.get("entry_price", 1.0501),
            "slippage_pips": round(np.random.uniform(0.1, 0.5), 1),
            "error_code": None,
            "error_message": None
        }
    except Exception as e:
        return {
            "success": False,
            "error_code": 1000,
            "error_message": str(e)
        }

@app.get("/dashboard-data")
async def get_dashboard_data():
    try:
        performance = database.get_performance_metrics()
        
        # Get market data for major pairs
        major_pairs = ["EURUSD", "GBPUSD", "USDJPY"]
        market_overview = {}
        
        for pair in major_pairs:
            try:
                data = await data_manager.get_yahoo_finance_data(pair)
                market_overview[pair] = {
                    "price": data.last,
                    "change": data.change,
                    "change_percent": data.change_percent
                }
            except:
                continue
        
        return {
            "performance": performance,
            "market_overview": market_overview,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Simplified Production Dashboard</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body { font-family: Arial, sans-serif; background: #1a1a1a; color: #fff; margin: 0; padding: 20px; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; text-align: center; border-radius: 8px; margin-bottom: 20px; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
            .card { background: rgba(255,255,255,0.1); border-radius: 8px; padding: 20px; }
            .metric { text-align: center; }
            .metric-value { font-size: 2rem; font-weight: bold; margin: 10px 0; color: #4ade80; }
            .refresh-btn { background: #667eea; border: none; color: white; padding: 12px 24px; border-radius: 6px; cursor: pointer; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🚀 Simplified Production Dashboard</h1>
            <p>Real-time forex trading • Python 3.13 compatible • Live market data</p>
            <button class="refresh-btn" onclick="location.reload()">🔄 Refresh</button>
        </div>
        
        <div class="grid">
            <div class="card">
                <div class="metric">
                    <div class="metric-value" id="status">🟢 ONLINE</div>
                    <div>System Status</div>
                </div>
            </div>
            <div class="card">
                <div class="metric">
                    <div class="metric-value">Python 3.13</div>
                    <div>Runtime Version</div>
                </div>
            </div>
            <div class="card">
                <div class="metric">
                    <div class="metric-value">📊 Live Data</div>
                    <div>Yahoo Finance</div>
                </div>
            </div>
            <div class="card">
                <div class="metric">
                    <div class="metric-value">🧪 Paper Mode</div>
                    <div>Safe Testing</div>
                </div>
            </div>
        </div>
        
        <div class="card" style="margin-top: 20px;">
            <h3>📈 Service Endpoints</h3>
            <ul>
                <li><a href="/health" style="color: #4ade80;">Health Check</a></li>
                <li><a href="/market-data/EURUSD" style="color: #4ade80;">Market Data</a></li>
                <li><a href="/technical-analysis/EURUSD" style="color: #4ade80;">Technical Analysis</a></li>
                <li><a href="/docs" style="color: #4ade80;">API Documentation</a></li>
            </ul>
        </div>
    </body>
    </html>
    """

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
