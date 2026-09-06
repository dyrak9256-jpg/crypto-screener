# Детальный аудит и рефакторинг — 2026-09-07

## 1. Итог

Проект переработан как backend арбитражного скринера:

`WebSocket adapters -> MarketTick -> ordered/sharded ingestion -> executable route engine -> signal lifecycle -> per-user filters -> Telegram -> PostgreSQL`

Поддерживаемые классы связок:

- FUTURES -> FUTURES между разными биржами;
- SPOT -> FUTURES внутри одной биржи;
- обратное направление SPOT short / FUTURES long тоже учитывается;
- spread считается по executable BBO: buy = ask, sell = bid;
- route volume = минимум 24h quote volume двух ног;
- funding хранится отдельно по `(exchange, symbol)` и учитывает направление;
- signal имеет OPEN / UPDATE(peak) / CLOSE lifecycle;
- OPEN сохраняется сразу, новые пики сохраняются, CLOSE сохраняет final spread и duration;
- при штатном shutdown активные маршруты принудительно закрываются и сохраняются.

## 2. Что было критично исправлено

### `internal/app/sharded_aggregator.go`

Исправлено:

- ранее выбиралась только глобальная min/max пара; теперь проверяются все независимые futures/futures пары;
- запрещено сравнение биржи самой с собой;
- используется ask для покупки и bid для продажи;
- stale quotes не участвуют в маршруте;
- одинаковый symbol всегда попадает в один shard;
- stale/запоздалые ticks не должны перетирать более новый state;
- lifecycle state хранится в агрегаторе, поэтому DB/Telegram не получают событие на каждый ticker update;
- сохраняется peak spread;
- исчезнувший маршрут закрывается, вместо того чтобы висеть ACTIVE бесконечно;
- `FlushActive()` закрывает активные маршруты при shutdown;
- lifecycle events больше не silently dropped.

Ограничение: адаптеры всё ещё могут отбросить market tick при переполнении общего ingress channel. Это сделано, чтобы не блокировать чтение WebSocket навсегда. Для строгой гарантии обнаружения каждого краткоживущего спреда нужен per-symbol latest-value ingress/lock-free ring или отдельный market-data engine.

### `internal/app/app.go`

Исправлено:

- несколько ingestion workers больше не обрабатывают один symbol в произвольном порядке: ticks маршрутизируются в фиксированную worker queue по hash(symbol);
- tracker оставлен однопоточным для строгого порядка OPEN/UPDATE/CLOSE;
- connectors запускаются после готовности ingestion/tracker pipeline;
- shutdown порядок: stop producers -> drain ticks -> flush active routes -> drain tracker -> drain persistence;
- команды сериализованы;
- admin authorization fail-closed;
- connector factory используется вместо switch внутри application;
- пользовательские настройки загружаются с enforcement hard limits.

### `internal/app/tracker.go`

Исправлено:

- signal state не передаётся между goroutines как mutable pointer;
- OPEN сохраняется сразу;
- UPDATE сохраняет только новые peak values;
- CLOSE сохраняет duration/final spread;
- out-of-order events игнорируются;
- close notification отправляется только тем пользователям, которые получили OPEN.

### `internal/app/notification_router.go`

Исправлено:

- фильтрация выполняется отдельно для каждого пользователя;
- spread и volume filters реально применяются к конкретному signal;
- уведомления поставлены в bounded ordered queue вместо goroutine-per-user;
- Telegram API не вызывается под Tracker mutex.

Важно: сейчас volume — только rolling 24h quote volume. Пользовательские `1m/5m/15m/30m/1h/4h` volume windows не могут корректно вычисляться из ticker 24h. Команда `/settimeframe` поэтому принимает только `24h`. Для настоящих timeframe filters нужен trade/kline WebSocket pipeline.

### `internal/app/funding_manager.go`

Исправлено:

- funding state keyed by exchange + symbol;
- направление учитывается в расчёте net spread;
- stale funding не используется;
- funding health разделён по exchange;
- funding теперь optional по умолчанию, поэтому отсутствие funding feed не отключает SPOT/FUTURES detection на биржах без реализованного funding adapter;
- `RequireFunding=true` может сделать funding обязательным.

Важно: funding adapter реализован только для Binance. Для остальных бирж funding не должен считаться доступным автоматически.

### `internal/app/connector_manager.go`

Исправлено:

- ownership reconnect loop находится у adapters;
- manager не создаёт второй reconnect loop;
- Add/Remove/StopAll сериализованы lifecycle mutex;
- StopAll идемпотентен;
- manager не закрывает tick channel — это ответственность Application после остановки всех producers.

### `internal/adapters/*`

Исправлено:

- WebSocket read limits;
- strict BBO validation;
- strict numeric parsing;
- MEXC single-writer protection;
- BingX single-writer protection;
- Bybit pagination/subscription batching;
- Gate explicit contract subscriptions;
- KuCoin volume fields restored;
- Binance/Bybit/Bitget/OKX source timestamps captured where payload provides them;
- HTTP status codes checked in REST symbol discovery.

### `internal/adapters/postgres/repo.go`

Исправлено:

- pgx pool limits/lifetime/healthcheck;
- DB ping at startup;
- embedded idempotent schema execution at startup, so schema changes do not depend only on Docker's first-init directory;
- route, market, volume, funding, next funding and duration are persisted;
- rows.Err() checked.

### `internal/adapters/telegram/bot.go`

Исправлено:

- bounded command workers;
- bounded send queue;
- idempotent Close;
- send channel close protected against concurrent senders;
- no unbounded command goroutines;
- retry-after for Telegram 429.

### `internal/config/config.go`

Исправлено:

- malformed numeric configuration fails startup;
- DATABASE_URL is mandatory;
- admin list is parsed strictly;
- obsolete TELEGRAM_CHAT_ID removed.

## 3. Database lifecycle model

For every market signal:

1. `SignalOpened` -> INSERT/UPSERT ACTIVE row;
2. higher peak -> UPDATE same row;
3. `SignalClosed` -> UPDATE CLOSED row with `closed_at`, `final_spread`, `duration_ms`;
4. shutdown -> active routes are converted to CLOSE events.

Therefore the database can answer:

- when opportunity appeared;
- when it disappeared;
- lifetime;
- initial spread;
- maximum observed spread;
- final spread;
- route direction;
- market types;
- route liquidity snapshot;
- funding data when available.

## 4. Important limitations before production

### A. Build/test gate is not verified in this environment

The project requires Go 1.26.4. The audit environment has Go 1.23.2 and cannot download the required toolchain/dependencies because network access is unavailable.

Therefore the following were NOT honestly claimed as passed:

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `go build ./...`
- live WebSocket integration tests
- PostgreSQL Testcontainers tests

All Go sources were run through `gofmt` successfully.

### B. Exchange protocol integration tests are still required

Unit parsing tests are not enough. Each adapter must be tested against recorded official payload fixtures and, ideally, a live sandbox/public stream.

Particularly important: KuCoin futures. Current KuCoin documentation recommends `tickerV2:{symbol}` for real-time BBO; the existing all-symbol futures topic should be migrated before production. See official docs: Ticker V2 and Ticker V1.

### C. Exact max spread is bounded by processed market updates

If an adapter or upstream ingress drops a market update, a sub-millisecond spike that existed only in the dropped message cannot be reconstructed. A strict guarantee requires a loss-aware market-data ingestion architecture.

### D. Raw spread is not guaranteed net PnL

The engine currently calculates executable price spread, optionally adjusted by funding. It does NOT yet model:

- taker/maker fees per exchange;
- withdrawal/deposit costs;
- slippage based on actual order-book depth;
- latency between both legs;
- minimum executable quantity;
- position limits;
- borrow cost for spot short;
- contract multiplier differences.

Therefore a `2%` signal means executable BBO spread, not guaranteed `2%` profit.

### E. 24h volume is not interval volume

A rolling 24h ticker volume cannot be converted into valid 15m/1h volume by subtracting two rolling values. A real timeframe filter needs trades or kline turnover.

### F. Funding coverage is incomplete

Only Binance funding is wired as a funding connector. Other exchanges can still produce raw spot/futures opportunities because funding is optional, but funding-adjusted profitability is not exchange-complete.

### G. High-load performance is not yet benchmark-proven

The architecture is designed for bounded concurrency, sharding and backpressure, but no honest claim of `100k+ messages/sec` is made until benchmarks and `-race` runs are performed on the target machine.

The hottest code still uses `shopspring/decimal`, maps and string keys. If profiling shows CPU/GC pressure, the next optimization should be a fixed-point/integer price representation in the hot path, with decimal conversion at persistence/notification boundaries.

## 5. Production verification commands

```bash
go version
go test ./...
go test -race ./...
go vet ./...
go build ./...
docker compose config
docker compose build
```

Then perform live adapter verification and collect p50/p95/p99 processing latency, queue depth, dropped ticks, reconnect count, stale-route count, DB latency and Telegram queue latency.

## v3 additions: native 1m candle volume

- Added `MarketCandle`/`CandleSink` and `CandleConnector`.
- All eight exchange adapters now expose `ConnectCandles` using their native public candle WebSocket feeds.
- Only 1m candles are subscribed; 5m/15m/30m/1h/4h are computed locally.
- Repeated updates for the same open minute replace the bucket instead of being summed.
- Route volume is `min(buy leg, sell leg)`.
- Interval volume is checked before route lifecycle evaluation and again per user before Telegram delivery.
- 24h continues to use rolling ticker quote volume; it is not mixed with interval candle volume.
- Cold-start behavior is conservative: no historical volume is invented. An interval filter becomes fully populated after its window has elapsed unless historical backfill is added.

### Validation limitation

The environment used for this revision cannot download the Go 1.26.4 toolchain or external modules because outbound DNS/network access is unavailable. `gofmt` was run over the full source tree, but `go test ./...`, `go test -race ./...`, `go vet ./...` and `go build ./...` could not be completed in this environment. They remain a mandatory CI/release gate.
