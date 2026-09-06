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

Spot vs futures on the same exchange. Both directions are supported.

Funding is optional by default and direction-aware when a funding feed exists.

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

This is a market-data screener, not an execution engine. A detected spread is not guaranteed net profit. Fees, slippage, order-book depth, latency and position constraints are not yet part of the PnL model.

Ticker quote volume is rolling 24h volume. Timeframe-specific volume requires a separate trades/kline pipeline.

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
  - retain 24h buckets
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
