# Architecture

## Data flow

WebSocket adapters normalize exchange payloads into `domain.MarketTick`.

`MarketTick -> ingress -> symbol-hashed workers -> ShardedAggregator -> lifecycle events -> Tracker -> PostgreSQL + NotificationRouter -> Telegram`

## Market-data model

A market state is maintained per:

`symbol -> exchange -> market type`

The engine uses executable prices:

- buy = best ask;
- sell = best bid.

Freshness is bounded by a 5-second TTL. Old ticks cannot overwrite a newer received tick.

## Arbitrage types

### Cross-exchange

Futures vs futures between distinct exchanges. All currently fresh exchange pairs are checked, not only the global min/max pair.

### Intra-exchange

Spot vs futures on the same exchange. Only the supported route is generated:
SPOT BUY -> FUTURES SHORT. The reverse SPOT SHORT -> FUTURES LONG route is intentionally forbidden.

Funding is route-aware and fail-closed. A missing, stale or unhealthy funding record prevents a funding-adjusted opportunity from passing the arbitrage engine.

## Signal lifecycle

The aggregator maintains active route state and emits only meaningful lifecycle changes:

- `SignalOpened`
- `SignalUpdated` when a new maximum spread is observed
- `SignalClosed`

This prevents PostgreSQL and Telegram from receiving every market-data update.

The Tracker serializes lifecycle events and persists immutable snapshots.

## Concurrency

Ticks are dispatched to a fixed number of worker queues using `hash(symbol)`. This preserves processing order for a symbol while allowing parallel processing across unrelated symbols.

Tracker has one worker intentionally: OPEN/CLOSE ordering is more important than parallelism at this stage.

ConnectorManager serializes connector lifecycle operations. Adapters own reconnect loops.

## Persistence

Signals are persisted asynchronously with retry/backoff. A failed database does not silently discard a signal during normal runtime; the persistence worker retries until shutdown.

The database schema is embedded and applied idempotently during startup, so existing Docker volumes do not depend solely on initdb execution.

## Important non-guarantees

This is a market-data screener, not an execution engine. A detected spread is not guaranteed net profit. Taker fees (per-exchange, configurable via `FEES` / `/setfees`) ARE subtracted from every spread before thresholds and funding evaluation — all signals are net-spread. Slippage, order-book depth, latency and position constraints are still not part of the model.

Ticker quote volume is used for the 24h filter. The 1m/5m/15m/30m/1h/4h filters are built locally from 1-minute candle turnover, with conservative cold-start projection only when the current minute is present.

## Interval-volume pipeline (v3)

```text
Exchange 1m Candle WS
        |
        v
 CandleConnector
        |
        v
 VolumeEngine
  - replace current minute
  - retain 4h+2m buckets (longest window is 4h; 24h volume comes from the ticker field)
  - aggregate 1m -> 5m/15m/30m/1h/4h
        |
        +----> route prefilter (any active user can qualify)
        |
        v
 Arbitrage route engine
        |
        v
 User-specific volume recheck -> Telegram
```

The engine deliberately avoids order-book depth and trade-by-trade volume. This keeps the volume feature bounded and predictable under high message rates.

## Fees (net spread)

Per-exchange taker fees live in `ScreenerConfig` (default: conservative 5 bps per side; `DEFAULT` key applies to unlisted exchanges). The aggregator subtracts buy-side and sell-side fees from the raw executable spread BEFORE funding evaluation and thresholds, so `HARD_MIN_SPREAD` and user `MinSpread` both operate on net values. Telegram messages label this explicitly as "Spread (net)". Values are set via `FEES` env or `/setfees` and persist in the `settings` table.

## Observability

`internal/observability` runs an HTTP server (`METRICS_ADDR`, default `:9090`):

- `/metrics` — Prometheus registry: ticks by exchange, signals opened/closed, telegram sent/dropped, DB errors, queue depths, active signals, connected exchanges, funding age per exchange;
- `/healthz` — liveness JSON probe;
- `/api/status` — runtime snapshot (uptime, goroutines, queues, active signals, exchange availability checked at startup);
- `/debug/pprof/*` — Go profiling.

Startup performs a REST reachability check of all 8 exchanges and logs geo-blocks (HTTP 403/451) loudly; results are surfaced in `/api/status`.

## Operator settings persistence

The `settings` table (key/value) survives restarts for `/sethardspread` and `/setfees`; values are loaded in `Application.Run` before workers start. Optional capabilities (`SettingsRepository`, `StatsRepository`) are discovered by interface assertion, so test doubles and in-memory repos degrade gracefully.

## Telegram delivery

A single bot (`TELEGRAM_TOKEN`) or a pool (`TELEGRAM_TOKENS`) via `BotPool`: each user is pinned to the bot through which they subscribed (`users.bot_id`), and both private messages and broadcasts are routed through that bot. Bot-level rate limits no longer bound total throughput.

## Smoke testing

`cmd/smoke` runs the full pipeline (8 connectors + PostgreSQL + metrics) with a no-op Telegram sender, prints `/healthz`, `/api/status` and `screener_*` metrics, then verifies graceful shutdown. It is the fastest way to see which exchanges actually deliver data from a given host: `DATABASE_URL=... go run ./cmd/smoke`.