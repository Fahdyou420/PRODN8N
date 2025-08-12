# Forex Trading Automation Stack

A production-ready automated forex trading system that generates multi-strategy signals and executes them via MetaTrader 5, orchestrated through N8N workflows on Render cloud.

## 🚀 Features

- **5 Professional Trading Strategies**: Soros macro breakout, Paul Tudor Jones trend-following, Renaissance statistical arbitrage, Druckenmiller macro, and Michael Burry carry trades
- **Risk Management**: Position sizing, margin checks, daily loss limits, and exposure controls
- **Human-in-the-Loop**: Optional ChatGPT validation with manual approval workflow
- **Paper Trading**: Safe testing environment before live execution
- **N8N Orchestration**: Visual workflow management with Slack notifications
- **Production Ready**: Docker deployment, PostgreSQL logging, comprehensive testing

## 📋 Quick Start for N8N on Render

### 1. Deploy to Render

1. **Fork this repository** to your GitHub account

2. **Create a new Web Service** on Render:
   - Connect your GitHub repository
   - Runtime: Docker
   - Build command: `docker build -t trading-stack .`
   - Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`

3. **Add Environment Variables** in Render dashboard:
   ```bash
   PAPER_MODE=true
   MANUAL_APPROVAL=true
   MT5_REST_API_KEY=your_secret_key_here
   RISK_PCT_DEFAULT=2.0
   DATABASE_URL=your_postgres_url_from_render
   SLACK_WEBHOOK_URL=your_slack_webhook
   ```

4. **Create PostgreSQL Database** on Render and add the URL to `DATABASE_URL`

### 2. Set Up N8N Workflow

1. **Import the Workflow**:
   - Copy the contents of `n8n_trading_workflow.json`
   - In your N8N instance, go to Workflows → Import from JSON
   - Paste the JSON content

2. **Configure Environment Variables in N8N**:
   ```bash
   TRADING_SERVICE_URL=https://your-render-app.onrender.com
   MT5_REST_API_KEY=your_secret_key_here
   OPENAI_API_KEY=your_openai_api_key
   MANUAL_APPROVAL=true
   ```

3. **Set Up Slack Integration**:
   - Create a Slack App and get webhook URL
   - Add Slack credentials in N8N
   - Update webhook URLs in environment

### 3. MetaTrader 5 Setup

#### Option A: Local MT5 (Recommended for Testing)
1. Install MetaTrader 5 on your local machine
2. Open a demo account with any broker
3. Enable "Allow automated trading" in Tools → Options → Expert Advisors
4. Install the MT5 Python package: `pip install MetaTrader5`

#### Option B: MetaApi Cloud (Production)
1. Sign up for MetaApi.cloud account
2. Connect your MT5 broker account
3. Get API token and update configuration
4. Modify the MT5Manager class to use MetaApi instead of local MT5

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `PAPER_MODE` | Enable paper trading | `true` | Yes |
| `MANUAL_APPROVAL` | Require human approval | `true` | Yes |
| `MT5_REST_API_KEY` | API key for authentication | - | Yes |
| `RISK_PCT_DEFAULT` | Default risk per trade (%) | `2.0` | No |
| `MAX_DAILY_RISK_PCT` | Maximum daily loss (%) | `10.0` | No |
| `MAX_OPEN_POSITIONS` | Maximum concurrent trades | `5` | No |
| `DATABASE_URL` | PostgreSQL connection string | - | Yes |
| `SLACK_WEBHOOK_URL` | Slack notifications | - | No |

### Strategy Configuration

Enable/disable individual strategies:
```bash
ENABLE_SOROS=true
ENABLE_JONES=true
ENABLE_SIMONS=true
ENABLE_DRUCKENMILLER=true
ENABLE_BURRY=true
```

## 📊 Trading Strategies

### 1. Soros Macro Breakout (`soros_macro_breakout`)
- **Logic**: Economic surprise detection + momentum breakout
- **Timeframe**: M1/M5 for entry, economic calendar for triggers
- **Risk**: ATR-based stops, 2.5R target
- **Best For**: Major news events, central bank decisions

### 2. Paul Tudor Jones Trend (`jones_trend`)
- **Logic**: EMA50/200 crossover on H4 with D1 confirmation
- **Filter**: RSI exclusion (40-60 zone)
- **Risk**: ATR*1.5 stops, 2R target
- **Best For**: Strong trending markets

### 3. Renaissance Stat Arb (`simons_stat_arb`)
- **Logic**: Z-score mean reversion on currency pairs
- **Entry**: |Z-score| ≥ 2.0
- **Risk**: Tight stops (10 pips), quick scalping
- **Best For**: Range-bound, high-liquidity periods

### 4. Druckenmiller Macro (`druckenmiller_macro`)
- **Logic**: DXY trend + equity sentiment + yield analysis
- **Risk**: Wide stops (50 pips), large targets (150 pips)
- **Best For**: Major macro regime changes

### 5. Michael Burry Carry (`burry_carry`)
- **Logic**: Interest rate differential + valuation analysis
- **Risk**: Long-term holds, 80 pip stops
- **Best For**: Stable economic periods with clear rate differentials

## 🔄 N8N Workflow Description

The workflow includes these key nodes:

1. **Schedule Trigger**: Runs every 5 minutes
2. **Generate Signals**: Calls the FastAPI batch endpoint
3. **Filter High Confidence**: Filters signals with confidence ≥ 60%
4. **Manual Approval Check**: Routes based on MANUAL_APPROVAL setting
5. **ChatGPT Validation**: AI analysis of trading signals
6. **Slack Notification**: Human approval request with links
7. **Wait for Approval**: Webhook listener for human input
8. **Execute Order**: Sends orders to MT5 via FastAPI
9. **Trade Notifications**: Success/failure alerts

### Manual Approval Process

When `MANUAL_APPROVAL=true`:
1. Signal generated and validated by ChatGPT
2. Slack message sent with AI analysis
3. Human clicks APPROVE or REJECT link
4. Webhook processes the decision
5. Order executed only if approved

## 🧪 Testing

Run the comprehensive test suite:

```bash
# Install test dependencies
pip install pytest httpx

# Run all tests
pytest test_trading_stack.py -v

# Run specific test categories
pytest test_trading_stack.py::TestRiskManagement -v
pytest test_trading_stack.py::TestAPIEndpoints -v
pytest test_trading_stack.py::TestSignalGeneration -v
```

### Test Coverage

- ✅ Risk management calculations
- ✅ Signal generation for all strategies
- ✅ API endpoint functionality
- ✅ Error handling and validation
- ✅ Paper mode simulation
- ✅ Integration workflows

## 📈 Monitoring & Observability

### Health Checks
- **Endpoint**: `GET /health`
- **Monitors**: MT5 connection, database status, configuration
- **Render**: Automatic health check configuration included

### Logging
- **Application**: Structured JSON logs to stdout
- **Database**: All trades, signals, and approvals logged
- **Slack**: Real-time trade notifications

### Performance Metrics
- Win rate by strategy
- Average trade duration
- Daily/weekly P&L
- Risk-adjusted returns

## 🔒 Security

### API Security
- API key authentication on all endpoints
- Rate limiting (configure in Render)
- HTTPS enforcement

### Risk Controls
- Position size limits
- Daily loss limits
- Maximum concurrent positions
- Margin requirement checks

### Data Protection
- Sensitive config in environment variables
- No hardcoded credentials
- PostgreSQL connection encryption

## 🚨 Production Deployment

### Before Going Live

1. **Test in Paper Mode** for at least 2 weeks
2. **Validate all strategies** with historical data
3. **Test manual approval** workflow end-to-end
4. **Configure position sizing** for your account size
5. **Set up monitoring** and alerting
6. **Have a kill switch** plan ready

### Go-Live Checklist

- [ ] Paper mode tested extensively
- [ ] All environment variables configured
- [ ] MT5 connection working
- [ ] Database initialized
- [ ] Slack notifications working
- [ ] Manual approval tested
- [ ] Risk limits appropriate
- [ ] Monitoring dashboard ready
- [ ] Emergency stop procedure documented

### Risk Warning

**This software is for educational and testing purposes. Live trading involves substantial risk of loss. Never risk more than you can afford to lose. Always test thoroughly in paper mode before live deployment.**

## 📞 Support & Troubleshooting

### Common Issues

1. **MT5 Connection Failed**
   - Check MT5 is running and logged in
   - Verify "Allow automated trading" is enabled
   - Ensure Python has proper permissions

2. **N8N Webhook Not Working**
   - Check webhook URLs in environment variables
   - Verify N8N instance is accessible
   - Test webhook endpoints manually

3. **Database Connection Error**
   - Verify PostgreSQL URL is correct
   - Check database is accessible from Render
   - Run `init.sql` to create tables

4. **Signals Not Generating**
   - Check API key configuration
   - Verify strategy toggle environment variables
   - Check application logs for errors

### Getting Help

- **Documentation**: Check this README and code comments
- **Logs**: Monitor application logs in Render dashboard
- **Testing**: Run test suite to validate functionality
- **Health Check**: Use `/health` endpoint to verify status

## 📜 License

This project is provided as-is for educational purposes. See LICENSE file for details.

---

**⚠️ Disclaimer**: This software is for educational and research purposes only. Trading foreign exchange carries a high level of risk and may not be suitable for all investors. Past performance is not indicative of future results. Always consult with a qualified financial advisor before making trading decisions.


# 🚀 Production Forex Trading Stack Deployment Guide

Complete setup for production-grade automated forex trading with real market data, live charts, and comprehensive dashboard.

## 🏗️ **Production Architecture**

### **Free Data Sources:**
- ✅ **Yahoo Finance API** - Real-time forex prices
- ✅ **ExchangeRate-API** - Live exchange rates  
- ✅ **TradingView Charts** - Professional charting
- ✅ **Investing.com Calendar** - Economic events
- ✅ **Technical Indicators** - Real-time calculations

### **Components:**
1. **Enhanced Trading Service** - Real market data + technical analysis
2. **Live Dashboard** - Real-time monitoring with charts
3. **N8N Production Workflow** - Advanced signal processing
4. **SQLite Database** - Performance tracking and logging

---

## 📋 **Step 1: Deploy Enhanced Trading Service**

### **Create New Render Service:**
1. **Go to Render Dashboard**
2. **Create "Web Service" from Git**
3. **Upload the enhanced `production_trading_stack.py`**
4. **Name**: `forex-production-stack`

### **Environment Variables:**
```bash
# Core Configuration
PAPER_MODE=true
MT5_REST_API_KEY=production_key_2025
RISK_PCT_DEFAULT=2.0

# Data Sources (all free)
YAHOO_FINANCE_ENABLED=true
EXCHANGERATE_API_ENABLED=true
TECHNICAL_ANALYSIS_ENABLED=true

# Performance
MAX_DAILY_RISK_PCT=10.0
MAX_OPEN_POSITIONS=5
SIGNAL_GENERATION_INTERVAL=180

# Dashboard
DASHBOARD_ENABLED=true
LIVE_CHARTS_ENABLED=true
```

### **Requirements:**
Use `requirements_production.txt` with additional packages:
- `yfinance` for Yahoo Finance data
- `aiohttp` for async HTTP requests
- `sqlite3` for database (built-in)

---

## 📊 **Step 2: Access Production Dashboard**

### **Dashboard URL:**
```
https://forex-production-stack.onrender.com/dashboard
```

### **Features:**
- 📈 **Live TradingView Charts** (EURUSD, GBPUSD, etc.)
- 📊 **Real-time Market Data** (Yahoo Finance)
- 🎯 **Signal Performance Metrics**
- 📅 **Economic Calendar Integration**
- 💹 **Strategy Performance Analytics**
- 🔄 **Auto-refresh every 30 seconds**

---

## 🔧 **Step 3: Update N8N Production Workflow**

### **Import Enhanced Workflow:**
1. **Use `n8n_production_workflow.json`**
2. **Enhanced features:**
   - Real market data integration
   - Technical analysis filtering
   - Market context validation
   - Advanced confidence scoring
   - Live trade notifications

### **New N8N Environment Variables:**
```bash
# Production Trading Service
TRADING_SERVICE_URL=https://forex-production-stack.onrender.com
MT5_REST_API_KEY=production_key_2025

# Enhanced Features
LIVE_DATA_ENABLED=true
TECHNICAL_ANALYSIS_ENABLED=true
MARKET_CONTEXT_FILTERING=true

# Notifications (optional)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/your/webhook
TELEGRAM_BOT_TOKEN=your_bot_token
```

### **Enhanced Workflow Features:**
```bash
✅ Real-time market data fetching
✅ Technical indicator analysis
✅ Market context filtering (RSI, trend, volatility)
✅ Enhanced confidence scoring
✅ Live trade notifications
✅ Dashboard metrics updates
```

---

## 📈 **Step 4: Live Market Data Integration**

### **Yahoo Finance Integration:**
- **Real-time prices** for all major forex pairs
- **24-hour change** and volume data
- **Free tier** - no API key required
- **Rate limit**: ~2000 requests/hour

### **Technical Analysis:**
- **EMA 20/50/200** for trend analysis
- **RSI 14** for momentum
- **ATR 14** for volatility
- **MACD** for trend confirmation
- **Bollinger Bands** for volatility bands
- **Support/Resistance** levels

### **Enhanced Signal Processing:**
```python
# Market context filtering
if technicalData.volatility > 3.0:
    signal.confidence *= 0.8  # Reduce confidence in high volatility

if signal.direction === 'BUY' && technicalData.rsi_14 > 70:
    signal.confidence *= 0.7  # Reduce confidence when overbought
```

---

## 🎯 **Step 5: Production Workflow Execution**

### **Every 3 Minutes:**
1. **Health Check** ✅
2. **Get Live Market Data** (Yahoo Finance) ✅
3. **Calculate Technical Indicators** ✅
4. **Generate Enhanced Signals** ✅
5. **Market Context Analysis** ✅
6. **Execute High-Confidence Trades** (>80%) ✅
7. **Update Dashboard Metrics** ✅
8. **Send Trade Notifications** ✅

### **Enhanced Signal Analysis:**
```bash
📊 Market Data: EURUSD @ 1.0521
📈 Bid: 1.0520 | Ask: 1.0522
📉 24h Change: +0.15%
🌊 Volatility: 1.2%
📊 RSI: 65.4 | Trend: UP

🎯 Total signals: 20
🔥 High confidence (>80%): 3 signals
📈 Medium confidence (60-80%): 5 signals
📉 Low confidence (<60%): 12 signals
```

---

## 📊 **Step 6: Dashboard Features**

### **Live Market Overview:**
- **Real-time prices** for EURUSD, GBPUSD, USDJPY, AUDUSD
- **24-hour change** percentages
- **Bid/Ask spreads**
- **Volume information**

### **Performance Metrics:**
- **Total signals today**
- **Average confidence**
- **Tradeable signals** (>60% confidence)
- **Active paper trades**
- **Strategy performance** comparison

### **Integrated Charts:**
- **TradingView widget** with live EURUSD chart
- **5-minute timeframe** for scalping
- **Technical indicators** overlay
- **Dark theme** for professional look

### **Economic Calendar:**
- **Investing.com integration**
- **Live economic events**
- **Impact levels** (High/Medium/Low)
- **Real-time updates**

---

## 🔄 **Step 7: Monitoring & Alerts**

### **Slack Integration:**
```bash
🚀 Live Trade Executed

Strategy: soros_macro_breakout
Pair: EURUSD BUY
Confidence: 87.3%
Entry: 1.0521
Market Trend: UP
RSI: 65.4
Status: SUCCESS ✅

*Paper Mode - Live data, simulated execution*
```

### **Performance Tracking:**
- **SQLite database** for persistent storage
- **Signal history** and performance metrics
- **Strategy comparison** analytics
- **Win/loss ratios** tracking

---

## 🎯 **Step 8: Testing Production Environment**

### **Validation Checklist:**
- [ ] **Dashboard loads** at `/dashboard`
- [ ] **Live market data** updating
- [ ] **Technical indicators** calculating
- [ ] **Signals generating** every 3 minutes
- [ ] **Enhanced filtering** working
- [ ] **Trade execution** in paper mode
- [ ] **Notifications** sending (if configured)

### **Expected Results:**
```bash
✅ 20 signals generated every 3 minutes
✅ Real Yahoo Finance market data
✅ Technical analysis filtering
✅ 3-5 high-confidence signals per hour
✅ 2-3 paper trades executed per hour
✅ Live dashboard updates
✅ Professional-grade monitoring
```

---

## 🚀 **Step 9: Going Live (When Ready)**

### **Live Trading Preparation:**
1. **Test in production paper mode** for 2-4 weeks
2. **Analyze performance metrics** and strategy effectiveness
3. **Set up Windows VPS** with real MetaTrader 5
4. **Deploy production code** to VPS
5. **Set `PAPER_MODE=false`**
6. **Start with minimum position sizes**

### **Production Monitoring:**
- **24/7 dashboard monitoring**
- **Real-time trade alerts**
- **Performance analytics**
- **Risk management alerts**
- **System health monitoring**

---

## 💰 **Cost Breakdown (All FREE!)**

### **Render Free Tier:**
- ✅ **Trading Service**: Free 750 hours/month
- ✅ **N8N Service**: Free 750 hours/month
- ✅ **PostgreSQL**: Free 1GB database

### **Data Sources:**
- ✅ **Yahoo Finance**: Free (rate limited)
- ✅ **ExchangeRate API**: Free tier
- ✅ **TradingView Charts**: Free embedding
- ✅ **Investing.com Calendar**: Free integration

### **Total Monthly Cost: $0** 💸

---

## 🎯 **Production vs Development Comparison**

| Feature | Development | Production |
|---------|-------------|------------|
| Market Data | Simulated | Real-time Yahoo Finance |
| Technical Analysis | Basic | Full indicator suite |
| Dashboard | Basic metrics | Live charts + calendar |
| Signal Quality | Random | Market-driven |
| Filtering | Confidence only | Multi-factor analysis |
| Monitoring | Simple logs | Professional dashboard |
| Notifications | None | Slack/Telegram alerts |
| Database | In-memory | Persistent SQLite |
| Performance | Basic | Comprehensive analytics |

---

## 🚨 **Important Notes**

### **Rate Limits:**
- **Yahoo Finance**: ~2000 requests/hour (adjust N8N frequency if needed)
- **ExchangeRate API**: 1000 requests/month free
- **TradingView**: No limits for embedded charts

### **Data Quality:**
- **Yahoo Finance**: Excellent for major pairs
- **Technical indicators**: Calculated from real OHLC data
- **Economic calendar**: Manual integration (Investing.com iframe)

### **Scalability:**
- **Current setup**: Handles 5-10 pairs efficiently
- **Render free tier**: Sufficient for development and testing
- **Production scaling**: Consider paid tiers for heavy usage

**Your production environment is now ready for professional forex trading automation with real market data!** 🚀
