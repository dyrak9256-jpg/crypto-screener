# V6 Final Deep Audit — post-double-audit closure

Date: 2026-09-07
Scope: complete V6 working tree, after the previous V6 double-audit report.

## Executive verdict

This pass was performed from the beginning of the tree, not only against the previous finding list. Every Go source file was included in the static review, including all 8 exchange adapters, application lifecycle, ingress, aggregation, funding, volume, persistence, Telegram, PostgreSQL, domain code, mocks and tests.

Final static result:

- Production P0/P1 defects remaining: **0**
- Known production P2/P3 limitations: listed below
- Production `panic(`: **0**
- Unchecked production `UpdateFunding`: **0**
- Unchecked production `SetReadDeadline`: **0**
- `http.DefaultClient` in adapters: **0**
- Legacy `GetSymbolVolume`: **0**
- Old concurrent `WriteMessage(PingMessage)`: **0**
- Go parser: **52/52 files parsed successfully**
- gofmt: clean
- Test files: **21**

The code is now at the point where further changes should primarily be runtime verification and feature development, not another architectural refactor.

## Findings discovered in this pass and closed

### P0 — compile blocker: unused locals in reconnect loops

The fresh pass found `startedAt` locals in Gate.io and MEXC reconnect loops that were no longer used after previous refactors. These are compile-time blockers, not runtime defects.

Status: **FIXED**. Unused locals removed.

### P1 — heartbeat goroutine leak on candle reconnect

Several candle readers passed the long-lived adapter context directly into `wsutil.StartHeartbeat`. When a candle websocket failed and the shard reconnected, the heartbeat goroutine could survive until application shutdown instead of stopping with that individual connection.

This is a real goroutine leak under repeated reconnects.

Affected candle readers: Binance, BingX, Bitget, Bybit, Gate.io, KuCoin and OKX.

Fix:
- every candle websocket now owns a connection-scoped `connCtx`;
- heartbeat receives that context;
- `closeOnCtx` receives that context;
- the context is cancelled when the individual websocket reader returns.

Status: **FIXED**.

### P1 — hot-removing a connector could permit duplicate exchange connectors

`ConnectorManager.RemoveConnector` removed the entry from the registry before successfully stopping it. If shutdown timed out, the old connector could still be alive but no longer registered. A subsequent `/addex` could then start a second connector for the same exchange.

That could create duplicate market streams and conflicting funding state.

Fix:
- the entry remains registered until `entry.stop()` succeeds;
- failed stop leaves the connector visible and blocks replacement;
- successful stop removes it atomically.

Status: **FIXED**.

### P1 — MEXC futures 24h volume had wrong units

MEXC Futures `volume24` is contract/statistical volume, not quote notional. The screener's volume contract is quote notional. Treating the raw value as USDT volume could understate/overstate volume by large price/contract-size factors.

Fix:
- load active USDT contract sizes from MEXC contract details;
- parse futures `lastPrice`;
- convert `volume24 * contractSize * lastPrice` into quote notional;
- fail closed if contract size is unavailable.

A dedicated unit test was added.

This matches MEXC's documented contract-size model and futures ticker semantics.

Status: **FIXED**.

### P1 — shutdown could process an unbounded market-tick backlog

The previous shutdown drained the ingress and then allowed the dispatch/ingestion pipeline to process the remaining `tickChan` backlog. Under DB/tracker backpressure, a large market-data backlog could make shutdown take an effectively unbounded amount of time.

Fix:
- ingress is stopped first so no new producers can submit;
- ingestion workers receive a shutdown signal and stop processing transient market ticks;
- dispatcher is stopped before closing the tick channel;
- queued market ticks are intentionally discarded during shutdown;
- lifecycle state is flushed separately through Tracker.

This is the correct durability boundary: market ticks are transient, lifecycle OPEN/CLOSE events are persisted separately.

Status: **FIXED**.

### P1 — user configuration could report success while DB persistence failed

Commands such as `/start`, `/stop`, `/setcross`, `/setvol`, `/setfundingtime` and `/settimeframe` previously updated memory and then logged DB errors without returning failure to the user.

A DB outage could therefore produce a false "success" response and the configuration would disappear after restart.

Fix:
- persistence errors are now returned from `persistUser` / `deleteUser`;
- successful DB write happens before the in-memory state is changed;
- failed writes leave the previous in-memory state intact;
- user receives an explicit failure response.

Status: **FIXED**.

### P2 — stale active DB signals after process crash

Active signal state is in memory. A hard process crash can therefore leave a PostgreSQL row marked active even though no corresponding in-memory route exists anymore.

Fix:
- concrete PostgreSQL repository now exposes `ReconcileActiveSignals`;
- application startup invokes it when supported;
- previous active rows are closed with a NULL final spread and calculated duration.

A PostgreSQL integration test was added.

Status: **FIXED**.

### P2 — Telegram HTTP client had no request timeout

The Telegram library's default HTTP client has no timeout. This could make startup `GetMe` or an outbound send wait indefinitely under a broken network path.

Fix:
- Telegram bot now uses an HTTP client with a 15-second timeout;
- long polling timeout was reduced to 10 seconds so it remains below the HTTP timeout;
- notification worker shutdown is explicitly interruptible;
- queued transient Telegram messages are not drained forever during process termination.

Status: **FIXED**.

### P2 — Telegram shutdown could drain an arbitrarily large queue

NotificationRouter previously attempted to drain accepted jobs during shutdown. With a large queue and Telegram backpressure this could extend shutdown for a very long time.

Fix:
- shutdown now prioritizes termination;
- queued Telegram jobs are transient and may be abandoned during shutdown;
- lifecycle persistence remains independent.

Status: **FIXED**.

### P2 — HTTP response validation gap

Fresh pass found MEXC and BingX/OKX symbol endpoints that did not consistently validate HTTP/API status before consuming the response.

Fix:
- HTTP status checked;
- exchange API status/code checked where provided;
- malformed/error responses fail closed.

Status: **FIXED**.

### P2 — nil application config could bypass the intended hard floor

`NewApplication(nil, ...)` previously created a zero-spread configuration before the domain constructor clamped it. This was inconsistent with the constructor's 1% invariant and made the intent unclear.

Fix: nil application config now explicitly starts from the 1% administrative floor.

Status: **FIXED**.

### P3 — dead `exactMarket` volume helper

A private `exactMarket` helper was no longer used anywhere.

Status: **REMOVED**.

### P3 — stale architecture documentation

Architecture documentation still claimed:
- both directions for spot/futures;
- funding was optional;
- interval volume was not implemented.

Those statements no longer matched the code.

Status: **FIXED**.

## Business invariants rechecked

- SPOT/FUTURES route is only **SPOT BUY -> FUTURES SHORT**.
- SPOT SHORT -> FUTURES LONG is not generated.
- FUTURES/FUTURES supports both cross-exchange directions.
- Funding is route-aware.
- Missing/stale/unhealthy funding fails closed.
- SPOT/FUTURES uses only the futures short funding leg.
- FUT/FUT uses sell funding minus buy funding.
- Administrative hard floor is at least 1%.
- User spread cannot fall below the administrative floor.
- There is no administrative minimum volume.
- Volume is user-specific.
- Route volume is the minimum of the two legs.
- 1m candle data is the basis for interval volume.
- Repeated updates to the same minute replace the bucket.
- Zero-volume candles are known data.
- Cold-start projection requires the current minute and contiguous observations.
- Missing minutes are not silently converted to zero.
- `/setfundingtime` is per-user.
- Default funding-time filter is 30 minutes.
- Latest-value ingress coalesces market ticks.
- Same-symbol processing remains sharded/sequential.
- Lifecycle events have a separate persistence path.

## Remaining limitations — not production-blocking defects

### P2 — full Go 1.26.4 runtime gate is still external

The sandbox has Go 1.23.2 while `go.mod` requires Go 1.26.4. Dependency downloads are also unavailable here.

Therefore the following still must be run on the user's Go 1.26.4 environment:

```text
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

This is a verification limitation, not an identified source-code defect.

### P2 — MEXC funding coverage can be availability-limited at very large symbol counts

MEXC's documented per-symbol funding endpoint is rate-limited. The current implementation polls symbols in batches and fails closed when an individual funding record becomes stale.

Consequence: under a sufficiently large symbol universe, some MEXC symbols may temporarily have no funding-adjusted opportunity even though the exchange is otherwise healthy.

Importantly, this cannot create a false funding-adjusted signal: stale data is rejected. It is a coverage/availability limitation, not a correctness or safety blocker.

### P2 — Telegram delivery is not a durable notification ledger

The application logs Telegram send failures and retries HTTP 429 once, but notification delivery is not persisted as a durable outbox. A process crash or prolonged Telegram outage can lose a notification.

The arbitrage lifecycle itself is persisted independently, so this does not corrupt signal state.

### P2 — bounded DB backpressure intentionally permits loss after prolonged saturation

Tracker persistence uses bounded queues and finite enqueue timeouts. If PostgreSQL remains unavailable and the DB queue remains saturated beyond the configured timeout, a lifecycle event can be dropped and logged.

This is an explicit bounded-backpressure tradeoff. Eliminating it completely would require a durable local outbox/WAL rather than an in-memory channel.

### P2 — connector readiness is operational, not semantic

Connector startup means goroutines have been launched. It does not currently expose a formal `connected + subscribed + first valid data` readiness state.

Funding itself remains fail-closed, and stale market data is rejected, so this does not permit invalid arbitrage signals.

## Final classification

| Class | Remaining |
|---|---:|
| P0 — crash/data corruption/security/blocker | **0** |
| P1 — production correctness / concurrency / lifecycle blocker | **0** |
| P2 — bounded operational/availability limitation | **4** |
| P3 — cosmetic/dead-code/documentation | **0** |

## Final recommendation

The source is ready to leave the refactor/audit cycle.

The next stage should be execution testing on Go 1.26.4:

1. normal unit/integration tests;
2. race detector;
3. vet/build;
4. reconnect storm tests for all 8 exchanges;
5. DB outage/recovery;
6. Telegram outage/recovery;
7. funding staleness/failure;
8. candle-shard failure/reconnect;
9. hot add/remove connector tests;
10. graceful shutdown with a saturated market-data queue;
11. long-running soak test.

No further known source-level P0/P1 defect remains from this audit pass.
