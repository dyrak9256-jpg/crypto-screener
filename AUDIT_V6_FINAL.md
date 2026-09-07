# AUDIT V6 FINAL — crypto-screener2

Date: 2026-09-07

## Scope

This audit was performed after applying every actionable item from `DOUBLE_AUDIT_V5.md` to V5.
The review covered all 50 Go files in the V6 tree, including all 8 exchange adapters, application lifecycle, funding, volume, ingress, persistence, notifications, domain interfaces, mocks and tests.

The review was repeated after fixes. Any issue found during the second pass was fixed and the audit was repeated again.

## Final static result

- Go files inspected: **50**
- `gofmt`: **clean**
- `go/parser` with `parser.AllErrors`: **50/50 parsed successfully**
- production `panic(`: **0**
- production `http.DefaultClient`: **0**
- production ignored `UpdateFunding` errors: **0**
- production ignored `SetReadDeadline` errors: **0**
- direct `return err`/`return readErr`/`return parseErr`: **0**
- tests: **19 test files**

Full `go test`, `go test -race`, `go vet` and `go build` could not be executed in the sandbox because the project requires Go 1.26.4 while the sandbox has Go 1.23.2 and cannot download the required toolchain/dependencies.

## V5 critical findings and their V6 status

### C1/D1 — concurrent WebSocket writers — FIXED

V5 used `WriteMessage(PingMessage)` from `wsutil.StartHeartbeat` while exchange-specific keepalive goroutines could write to the same connection.

V6 changes:

- heartbeat now uses `WriteControl(PingMessage, ...)` instead of `WriteMessage`;
- no `SetWriteDeadline` is performed by the heartbeat goroutine;
- read deadline handling remains in the reader/pong handler;
- exchange-specific application-level keepalives remain serialized by their own connection ownership/write locks where needed;
- Bybit candle/ticker keepalive was explicitly implemented and errors are wrapped.

This removes the gorilla/websocket single-concurrent-writer violation from the generic heartbeat path.

### C2 — silently ignored FundingSink errors — FIXED

All production `UpdateFunding` calls now check the returned error and wrap it with exchange/symbol/operation context.

### C3 — candle reconnect hot-loop — FIXED

All candle feed loops now have connection-local exponential backoff with jitter.

Affected exchanges:

- Binance
- BingX
- Bitget
- Bybit
- Gate.io
- KuCoin
- MEXC
- OKX

### C4 — subscription acknowledgement/error validation — FIXED/IMPROVED

Candle readers now explicitly reject protocol-level subscription errors where the exchange exposes them:

- Bitget
- Bybit
- BingX
- Gate.io
- KuCoin
- MEXC
- OKX

Binance uses stream URLs rather than explicit subscription messages, so there is no equivalent ACK request to wait for.

### C5 — Telegram transport coupled to business target calculation — FIXED

`NotificationRouter.ProcessSignal()` calculates targets independently of Telegram availability.

A signal can therefore determine and store its intended recipients even when the Telegram sender is not initialized yet.

Accepted notification jobs are retained until a Telegram sender becomes available, rather than being discarded solely because the transport was temporarily nil.

### C6 — DB queue backpressure — MITIGATED

Persistence now uses a worker pool of four consumers.

Tracker persistence enqueue has a finite five-second grace period and reports a wrapped/logged error if the bounded queue remains full instead of blocking forever.

### C7 — Telegram queue backpressure — MITIGATED

NotificationRouter now uses four workers instead of one.

The queue remains bounded and business-critical OPEN/CLOSE notifications are not silently dropped merely because Telegram was nil at the time target calculation occurred.

### C8 — live candle volume semantics — CLARIFIED/TESTED

VolumeEngine continues to treat a candle update as a replacement for the same minute bucket, which is required for exchanges that repeatedly update the current cumulative candle.

Cold-start projection is based only on a contiguous current run. Zero-volume candles remain known observations.

Existing tests cover:

- one-minute cold start projection;
- repeated current-candle replacement;
- zero-volume known bucket;
- projection stopping at a gap.

### C9 — MEXC spot kline protocol — FIXED

The previous V5 implementation incorrectly rejected binary MEXC spot kline frames.

MEXC's current Spot WebSocket documentation specifies protobuf transport for the `.pb` channels and defines `PushDataV3ApiWrapper` field 308 as `PublicSpotKlineV3Api`.

V6 implements the required protobuf wire decoding for the wrapper and spot-kline payload without guessing at undocumented fields. It extracts:

- symbol;
- send/create timestamps;
- interval;
- window start/end;
- quote amount.

A fixture-style unit test constructs a valid protobuf frame and verifies decoding.

### C10/D3 — inconsistent error wrapping — FIXED

Production adapter boundaries now wrap underlying errors with operation/exchange/symbol context.

Ignored read-deadline errors were removed.

Background keepalive failures are also wrapped before logging.

### C11 — misleading `GetSymbolVolume` API — FIXED

The misleading legacy method was removed from the aggregator and generated mock surface.

Route volume remains available through the explicit route-volume APIs.

### D4 — shutdown tracker flush — FIXED

Shutdown now gives tracker flush enqueue a finite timeout instead of allowing the shutdown path to wait forever for a blocked tracker queue.

### D5 — connector readiness — IMPROVED

Connector startup remains asynchronous by design because connector interfaces are long-lived blocking streams. The manager's contract is explicitly "goroutines started" rather than pretending that AddConnector proves exchange subscription readiness.

Subscription errors now propagate out of the affected stream and are logged with component context. Exchange stream health is therefore visible through failure paths instead of being treated as successful merely because a goroutine was created.

### D6 — funding stream negative health transition — FIXED

When a funding connector exits, `ConnectorManager` explicitly marks that exchange's funding stream unhealthy. Funding evaluation remains fail-closed and TTL staleness remains a second safety layer.

## Additional V6 correction discovered during post-fix audit

The second/third pass found a missing `bybitKeepAlive` implementation while reviewing every adapter call chain. V6 adds the implementation and wraps/logs its write failure.

This is important because static syntax parsing alone cannot catch unresolved identifiers.

## Business invariants rechecked

The final pass explicitly rechecked the core screener behavior:

1. Intra-exchange spot/futures route is only:
   `SPOT BUY -> FUTURES SHORT`.
2. Reverse `SPOT SHORT -> FUTURES LONG` is not generated.
3. Cross-exchange route is futures-to-futures:
   `FUTURES BUY -> FUTURES SELL`.
4. SPOT/FUTURES funding uses only the futures short funding rate.
5. FUTURES/FUTURES funding uses:
   `sellFunding - buyFunding`.
6. Funding is fail-closed when missing, unhealthy or stale.
7. Global hard spread floor remains at least 1%.
8. There is no administrator hard minimum volume.
9. User volume filtering remains user-specific.
10. Route volume is the minimum of the two route legs.
11. Repeated one-minute candle updates replace the existing bucket.
12. Zero-volume candles are valid known data.
13. Higher timeframes are derived locally from one-minute buckets.
14. Funding-time user filtering remains per-user.
15. User minimum spread remains clamped to the global hard floor.

## Tests added/retained

V6 includes tests for:

- MEXC protobuf spot-kline decoding;
- MEXC malformed/missing protobuf payload rejection;
- WebSocket heartbeat control-frame behavior;
- retry backoff cancellation/reset;
- Telegram target calculation without an initialized sender;
- existing funding mathematics;
- existing SPOT/FUTURE direction and lifecycle behavior;
- existing volume replacement/projection/zero-volume behavior;
- existing persistence, tracker, connector manager, config, domain and Telegram tests.

## Final release status

**V6 is the production candidate from static/code-review perspective.**

The remaining gate is execution on a machine with Go 1.26.4 and network access to dependencies:

```text
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

Then run the required live/soak validation:

- all 8 exchanges;
- all USDT spot/futures instruments supported by each adapter;
- 1m candles;
- multiple users;
- multiple timeframes;
- volume thresholds;
- funding thresholds;
- forced WebSocket reconnects;
- subscription rejection;
- DB outage;
- Telegram outage;
- connector replacement;
- shutdown/restart cycles.

No source-level P0/P1 finding remains from the V5 double-audit checklist after the V6 passes described above.
