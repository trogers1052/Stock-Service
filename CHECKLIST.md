# Trading Platform – Master Checklist

**Last Updated:** January 18, 2026
**Project Owner:** Thomas Rogers

This checklist is the single source of truth to keep the project organized, grounded, and moving forward without losing architectural discipline.

Status markers:
- ⬜ Not started
- 🟡 In progress
- ✅ Complete

---

## 🧱 Current Baseline (Locked In)

These are assumptions going forward. If one of these changes, the checklist must be updated.

### Infrastructure
- ✅ Raspberry Pi 5 (16GB RAM) provisioned and stable
- ✅ Docker installed and working
- ✅ Docker Compose in use
- ✅ PostgreSQL 17 container running (`trading-db`)
- ✅ Redis 8 container running (`trading-realtime-data-shared`)
- ✅ Redpanda (Kafka-compatible) container running (`trading-redpanda`)
- ✅ Redpanda Console accessible (port 8080)

### Stock-Service (Go)
- ✅ Service exists and connects to PostgreSQL
- ✅ Database migrations written and applied (8 migrations)
- ✅ Models and repositories implemented
- ✅ Unit tests added to repositories
- ✅ REST API endpoints working (health, stocks CRUD)
- ✅ Kafka consumer for `trading.orders` topic
- ✅ Raw trades stored to `raw_trades` table
- ✅ Position aggregation logic (BUY creates/updates, SELL closes)
- ✅ Trade history archival on position close
- ✅ Docker image built for ARM64 and published to GHCR

### Robinhood-Sync (Python)
- ✅ Service created and running on Raspberry Pi
- ✅ Connects to Robinhood API (robin_stocks library)
- ✅ Detects filled orders
- ✅ Publishes `TRADE_DETECTED` events to `trading.orders` topic
- ✅ Redis deduplication (tracks synced order IDs)
- ✅ Market hours scheduling (4am-8pm ET, Mon-Fri)
- ✅ Continuous and single-run modes supported
- ✅ Historical sync capability (`--days N`)

---

## 🧠 Architectural Rules (Non-Negotiable)

Use this as a guardrail when making decisions.

- ✅ Services communicate via **Kafka (Redpanda)**, not direct DB calls
- ✅ PostgreSQL is the **source of truth**, not Redis
- ✅ Redis is for **hot / ephemeral data only** (deduplication, caching)
- ✅ Each service has **one primary responsibility**
- ✅ No service reaches into another service's database tables
- ✅ Events are immutable (no edits, only new facts)

---

## 🧪 Environment & Observability

### Local Infrastructure
- ✅ Docker Compose file documents all running services
- ✅ Redpanda Console / UI accessible locally (port 8080)
- 🟡 Logs for each service are easy to find and readable
- ✅ `.env` files exist and are gitignored

### Sanity Checks
- ✅ Can connect to PostgreSQL from host (port 5432)
- ✅ Can connect to Redis from host (port 6379)
- ✅ Can produce and consume events in Redpanda (verified with robinhood-sync → stock-service)

---

## 📦 Event Foundation (Critical Path)

### Kafka / Redpanda Setup
- ✅ Redpanda running in KRaft mode (no Zookeeper)
- ✅ Topics created:
  - ✅ `trading.orders` (trade events from Robinhood)
  - ✅ `stock-events` (stock CRUD events)
  - ⬜ `stock.quotes.realtime` (price updates)
  - ⬜ `stock.indicators` (calculated indicators)
  - ⬜ `trading.alerts` (fired alerts)
- 🟡 Topic naming conventions documented
- 🟡 Event schemas defined (versioned)

### Event Design
- ✅ `TradeEvent` schema defined and working:
  ```json
  {
    "event_type": "TRADE_DETECTED",
    "source": "robinhood",
    "timestamp": "ISO8601",
    "data": {
      "order_id", "symbol", "side", "quantity",
      "average_price", "total_notional", "fees",
      "state", "executed_at", "created_at"
    }
  }
  ```
- ⬜ `QuoteEvent` schema defined
- ⬜ `IndicatorEvent` schema defined
- ⬜ All events include: symbol, timestamp, source, schema_version

---

## 📈 Market Data Ingestion (Go – Producer)

### market-data-ingestion Service
- ⬜ Service scaffolded
- ⬜ Fetch quotes from Finnhub API
- ⬜ Publish `QuoteEvent` to `stock.quotes.realtime`
- ⬜ Rate limiting enforced (Finnhub free tier: 60/min)
- ⬜ Retries and error handling implemented
- ⬜ Structured logging added
- ⬜ Unit tests for Kafka producer

### Validation
- ⬜ Events visible in Redpanda Console
- ⬜ Payload matches schema exactly

---

## 💾 Stock Persistence Service (Consumer)

### Price Data Responsibilities
- ⬜ Consume `stock.quotes.realtime`
- ⬜ Write current price snapshot to PostgreSQL (`stocks` table)
- ⬜ Append daily OHLCV to `price_data_daily`
- ⬜ Cache current price in Redis with TTL

### Engineering
- ⬜ Idempotent writes (safe reprocessing)
- ⬜ Consumer group configured correctly
- ⬜ Graceful shutdown handling
- ⬜ Unit tests for persistence logic

---

## 📊 Analytics Service (High ROI)

### Real-Time Indicators
- ⬜ Consume `stock.quotes.realtime`
- ⬜ Calculate RSI, SMA, MACD, Bollinger Bands
- ⬜ Publish `IndicatorEvent` to `stock.indicators`
- ⬜ Persist indicators to `technical_indicators` table

### Quality Controls
- ⬜ Minimum data window checks
- ⬜ Timeframe clearly defined per indicator
- ⬜ No lookahead bias

---

## 🚨 Alert Service

### Core Logic
- ⬜ Consume `stock.quotes.realtime`
- ⬜ Consume `stock.indicators`
- ⬜ Load `alert_rules` from PostgreSQL
- ⬜ Evaluate multi-condition rules
- ⬜ Enforce cooldowns

### Notifications
- ⬜ Telegram integration working
- ⬜ Message templates standardized
- ⬜ Alerts logged to `alert_history`

---

## 🤖 Trade Automation & Journaling

### Robinhood Sync
- ✅ Poll Robinhood API (every 10 minutes during market hours)
- ✅ Detect filled orders
- ✅ Publish `TradeEvent` to `trading.orders`
- ✅ Deduplicate via Redis

### Stock-Service Trade Processing
- ✅ Consume `trading.orders`
- ✅ Insert raw trades into `raw_trades` table
- ✅ Deduplicate by (order_id, source)
- ✅ Aggregate into positions (weighted avg price)
- ✅ Close positions on SELL and move to `trades_history`

### Trade Journal Service
- ⬜ Consume `trading.orders` (separate service)
- ⬜ Send Telegram journal prompts
- ⬜ Capture replies and update trade records
- ⬜ Voice note support (Whisper transcription)

---

## 📊 Portfolio & Review

### Portfolio Tracker
- ⬜ FastAPI service scaffolded
- ⬜ Read-only endpoints from PostgreSQL
- ⬜ Live prices from Redis

### Performance Review
- ⬜ Win rate calculation
- ⬜ Avg win / avg loss
- ⬜ Holding time analysis
- ⬜ Alert effectiveness metrics

---

## 🔁 Backtesting & Iteration

- ⬜ Event replay from Kafka topics
- ⬜ Strategy rules extracted into reusable logic
- ⬜ Backtest results stored and compared
- ⬜ Parameters tuned with data, not intuition

---

## 🧠 Discipline Layer (Trader Rules)

Before every trade, the system should capture:

- ⬜ Entry reason logged
- ⬜ Stop loss defined
- ⬜ Profit target defined
- ⬜ Risk % of account calculated
- ⬜ Strategy tag assigned

After every trade:

- ⬜ Exit reason logged
- ⬜ What went right
- ⬜ What went wrong
- ⬜ Trade graded (A–F)

---

## 🏁 Definition of Success

This project is succeeding if:
- 🟡 Trades are explainable (raw trades captured, positions tracked)
- ⬜ Rules are followed more often than overridden
- ⬜ Alerts arrive before decisions are emotional
- ⬜ Journals are filled automatically
- ⬜ Strategy changes are data-backed

If a feature does not improve decision quality, it is optional.

---

## 📋 Current Data Flow (Working)

```
┌──────────────┐
│  Robinhood   │
│   Account    │
└──────┬───────┘
       │ Poll every 10 min (market hours)
       ▼
┌──────────────────────────┐
│ robinhood-sync (Python)  │
│ - Fetch filled orders    │
│ - Dedupe via Redis       │
│ - Publish to Kafka       │
└──────────┬───────────────┘
           │ TRADE_DETECTED event
           ▼
┌──────────────────────────┐
│ Redpanda (Kafka)         │
│ Topic: trading.orders    │
└──────────┬───────────────┘
           │ Consumer
           ▼
┌──────────────────────────┐
│ stock-service (Go)       │
│ - Consume events         │
│ - Save to raw_trades     │
│ - Update positions       │
│ - Archive to history     │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│ PostgreSQL               │
│ - raw_trades             │
│ - positions              │
│ - trades_history         │
└──────────────────────────┘
```

---

## 🎯 Next Milestones

### Milestone 1: Market Data Pipeline (NEXT)
Build the price data ingestion system:
1. ⬜ Create market-data-ingestion service (Go)
2. ⬜ Integrate Finnhub API
3. ⬜ Publish to `stock.quotes.realtime`
4. ⬜ Update stock-service to consume price events
5. ⬜ Populate `price_data_daily` table

### Milestone 2: Analytics & Indicators
Calculate technical indicators:
1. ⬜ Create analytics-service (Python)
2. ⬜ Calculate RSI, MACD, SMA, Bollinger Bands
3. ⬜ Publish to `stock.indicators`
4. ⬜ Persist to `technical_indicators` table

### Milestone 3: Alerts & Notifications
Never miss a setup:
1. ⬜ Create alert-service (Go)
2. ⬜ Telegram bot integration
3. ⬜ Multi-condition rule evaluation
4. ⬜ Cooldown management

### Milestone 4: Trade Journaling
Capture reasoning:
1. ⬜ Create trade-journal service (Python)
2. ⬜ Telegram prompts on new trades
3. ⬜ Reply capture and storage
4. ⬜ Voice note transcription

---

## 📝 Session Log

### Session 1 (January 17, 2026)
- Built database schema and migrations
- Created Go market data service (database-centric)
- Set up Docker Compose
- Decided on event-driven architecture with Kafka

### Session 2 (January 17-18, 2026)
- Built robinhood-sync service (Python)
- Added Kafka consumer to stock-service
- Created raw_trades table and migration
- Verified end-to-end flow: Robinhood → Kafka → stock-service → PostgreSQL
- Position aggregation and trade history working

---

**End of Checklist**
