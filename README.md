# stock-service

A Go REST API and Kafka consumer for the AI-assisted trading platform. It owns
the persistent view of the universe the platform watches and trades — monitored
stocks, positions, watchlist, raw trade history — and it is the system of record
for **signal feedback**: how every alerted trade signal actually played out. The
decision-engine / alert pipeline writes feedback here (via Telegram → REST), and
`reporting-service` reads it back to score rule accuracy and signal-to-outcome
quality.

It sits on the Raspberry Pi (the "spine"), exposing an HTTP API for management and
analysis while continuously ingesting trade, position, and watchlist events from
Kafka into PostgreSQL.

## Role / Architecture

```
                 (REST /api/v1, X-API-Key auth)
 clients ───────────────────────────────────────▶ stock-service ──▶ PostgreSQL
 (alert/feedback, dashboards)                          │  ▲             (16 migrations)
                                                       │  │
   Kafka: trading.orders ────────(consume)────────────┤  └──(cache)──▶ Redis
   Kafka: trading.positions ─────(consume)────────────┤                 (rule-accuracy
   Kafka: trading.watchlist ─────(consume)────────────┘                  cache)
                                                       │
   Kafka: stock-events ◀──────────(produce)───────────┘
```

**Inputs**
- **REST `/api/v1/*`** — stock CRUD, signal-feedback create/update/query, and
  tier ranking management. Protected by an `X-API-Key` header when `API_KEY` is set.
- **Kafka consumers** (all in the `KAFKA_CONSUMER_GROUP` group):
  - `trading.orders` — `TRADE_DETECTED` events stored as raw trades.
  - `trading.positions` — `POSITIONS_SNAPSHOT` events that replace the current
    position set.
  - `trading.watchlist` — `WATCHLIST_UPDATED` / `WATCHLIST_SYMBOL_ADDED` /
    `WATCHLIST_SYMBOL_REMOVED` events that sync the monitored-stocks watchlist flags.

**Outputs**
- **PostgreSQL** — all persistent state (see Migrations below), schema-migrated
  automatically on startup via `golang-migrate`.
- **Redis** — optional cache; a background writer publishes per-rule accuracy so
  hot reads don't hit Postgres. The service runs without Redis if it's unavailable.
- **Kafka `stock-events`** (`KAFKA_TOPIC`) — produces stock added/removed/updated
  events when stocks are managed through the API.

## Features

- REST API for the monitored-stock universe, watchlist, positions, and tiers.
- Signal-feedback persistence: create feedback for an alerted signal, update it,
  record the eventual outcome, and query aggregate rule accuracy / outcome quality.
- Three independent Kafka consumers (trades, positions, watchlist) running
  concurrently with graceful shutdown.
- Kafka producer for stock-management events on `stock-events`.
- API-key authentication using a constant-time comparison
  (`crypto/subtle.ConstantTimeCompare`); auth disabled when `API_KEY` is empty
  (dev mode).
- Automatic database migrations on startup (`db/migrations`, run with `golang-migrate`).
- Background accuracy-cache writer that pushes per-rule accuracy to Redis.
- Prometheus `/metrics` (request count + latency per route, low-cardinality via
  route templates) and an unauthenticated `/health` endpoint.
- Distroless Docker image with a standalone healthcheck binary.

### API endpoints

All endpoints under `/api/v1` require the `X-API-Key` header when `API_KEY` is set.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness/readiness check (unauthenticated). |
| GET | `/api/v1/stocks` | List all monitored stocks. |
| POST | `/api/v1/stocks` | Add a monitored stock (emits `stock-events`). |
| GET | `/api/v1/stocks/sectors` | List sectors. |
| GET | `/api/v1/stocks/{symbol}` | Get one stock. |
| DELETE | `/api/v1/stocks/{symbol}` | Remove a stock (emits `stock-events`). |
| POST | `/api/v1/feedback` | Create signal feedback. |
| GET | `/api/v1/feedback` | Query signal feedback. |
| GET | `/api/v1/feedback/summary` | Feedback summary. |
| GET | `/api/v1/feedback/accuracy` | Per-rule accuracy. |
| GET | `/api/v1/feedback/unresolved` | Signals awaiting an outcome. |
| GET | `/api/v1/feedback/outcome-quality` | Per-rule outcome quality. |
| PUT | `/api/v1/feedback/{id}` | Update a feedback record. |
| PUT | `/api/v1/feedback/{id}/outcome` | Record the outcome for a signal. |
| GET | `/api/v1/tiers` | List all tier rankings. |
| PUT | `/api/v1/tiers` | Upsert a single tier. |
| PUT | `/api/v1/tiers/bulk` | Bulk upsert tiers. |
| GET | `/api/v1/tiers/{symbol}` | Get a symbol's tier. |

## Configuration

All configuration is via environment variables (see `.env.example`). Use
placeholders only; never commit real secrets.

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8081` | HTTP API port. |
| `SERVER_HOST` | `0.0.0.0` | HTTP bind host. |
| `API_KEY` | _(empty)_ | If set, required as `X-API-Key` on `/api/v1/*`. Empty = auth disabled (dev). |
| `DB_HOST` | `postgres` | PostgreSQL host. |
| `DB_PORT` | `5432` | PostgreSQL port. |
| `DB_USER` | `trader` | PostgreSQL user. |
| `DB_PASSWORD` | _(empty)_ | PostgreSQL password. |
| `DB_NAME` | `trading_platform` | PostgreSQL database name. |
| `DB_SSLMODE` | `disable` | PostgreSQL SSL mode. |
| `DB_LOCAL` | _(unset)_ | Optional full DSN convenience var for running migrations locally. |
| `KAFKA_BROKERS` | `localhost:19092` | Comma-separated broker list. |
| `KAFKA_TOPIC` | `stock-events` | Topic the producer publishes stock events to. |
| `KAFKA_TRADES_TOPIC` | `trading.orders` | Trade-events topic consumed. |
| `KAFKA_POSITIONS_TOPIC` | `trading.positions` | Position-snapshot topic consumed. |
| `KAFKA_WATCHLIST_TOPIC` | `trading.watchlist` | Watchlist-events topic consumed. |
| `KAFKA_CONSUMER_GROUP` | `stock-service` | Consumer group ID. |
| `REDIS_HOST` | `localhost` | Redis host (cache; optional). |
| `REDIS_PORT` | `6379` | Redis port. |
| `REDIS_PASSWORD` | _(empty)_ | Redis password. |
| `METRICS_PORT` | `9097` | Prometheus `/metrics` port. |

## Running

### Local

Prerequisites: Go 1.25+, PostgreSQL, a Kafka/Redpanda broker, and optionally
Redis. Migrations in `db/migrations` are applied automatically on startup.

```bash
cp .env.example .env          # fill in DB/Kafka/API_KEY
set -a; source .env; set +a
go run ./cmd/server
```

A `Makefile` provides common targets:

```bash
make build   # build the server binary
make test    # run the test suite
make run     # run locally
```

### Docker

A `docker-compose.yml` is provided for bringing up the service with its
dependencies. To build the image directly, note the CI build flow first compiles
the server binary into `dist/` and the Dockerfile copies it in — see Notes.

CI publishes images to `ghcr.io/trogers1052/stocks-service`. The Raspberry Pi
(arm64) pulls with:

```bash
docker pull ghcr.io/trogers1052/stocks-service:latest
```

## Testing

```bash
go test ./...                       # all tests
go test -race -short -cover ./...   # race detector + coverage (CI parity)
go test -run Integration ./...      # integration tests (testcontainers)
```

CI runs `go vet`, unit tests (`-race -short -cover`), and integration tests on
every push and pull request to `main`; the Docker image is built and pushed only
on pushes to `main`.

## Migrations

Schema lives in `db/migrations` as ordered `golang-migrate` up/down pairs and is
applied automatically at startup. Current set (16 migrations):

1. `stocks` table
2. `positions` table
3. `price_data` table
4. `technical_indicators` table
5. `watchlist` table
6. `alert_rules` table
7. `trade_history` table
8. `raw_trades` table
9. stop-loss-guardian tables
10. add market-regime symbols
11. fix ETF alert flags
12. `signal_feedback` table
13. `backtest_tiers` table
14. enrich `signal_feedback`
15. add trade plan to `signal_feedback`
16. add outcome to `signal_feedback`

## Project layout

```
cmd/
  server/         # main entry point: wiring, migrations, HTTP + metrics, consumers
  healthcheck/    # standalone binary for the Docker HEALTHCHECK
internal/
  api/            # routes, handlers, API-key + Prometheus middleware
  config/         # env-var configuration loader
  database/       # PostgreSQL repositories (stocks, positions, feedback, tiers, ...)
  kafka/          # trades/positions/watchlist consumers + stock-events producer
  models/         # domain types
  metrics/        # Prometheus metrics
  redis/          # cache client (rule-accuracy cache)
db/migrations/    # ordered golang-migrate up/down SQL files
```

## Notes

- **Go module name:** the module is `github.com/trogers1052/stock-alert-system`
  (legacy name) while the repository is `stock-service`. This is intentional and
  left as-is — import paths throughout the code use the legacy module path.
- **Docker build:** the CI workflow builds the server binary into `dist/` and the
  `Dockerfile` copies that prebuilt binary in (the in-image build is only for the
  small healthcheck). The published image is multi-arch
  (`linux/amd64,linux/arm64`); ensure the prebuilt binary architecture matches the
  target platform when building images manually.

---

## Built with Claude Code

A large portion of this project — implementation, tests, and documentation — was written in pair-programming sessions with [Claude Code](https://claude.com/claude-code), Anthropic's agentic command-line tool.
