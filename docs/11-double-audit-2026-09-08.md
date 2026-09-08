# Double audit after production hardening — 2026-09-08

## Scope

This pass re-audits the screener after the production-oriented changes discussed with the product owner. The review explicitly covers:

- canonical signal lifecycle: OPEN / MAX / CLOSE;
- compact PostgreSQL persistence;
- PostgreSQL outage/backpressure;
- per-user UPDATE thresholds in percentage points;
- Telegram shutdown and queue behaviour;
- Telegram stale-message revalidation;
- goroutine ownership and shutdown;
- channel closing/send races;
- mutex ordering and lock contention;
- memory retention / cleanup;
- startup and recovery failure paths;
- production exposure of observability endpoints.

The repository is intentionally kept as a single-process Go service for one VPS + Docker Compose. Kafka/Redis/Kubernetes were not introduced merely for convention.

## Changes applied

1. Replaced the bounded signal DB queue used by the application with a process-local deduplicating persistence buffer. The buffer keeps the newest snapshot per signal ID and therefore stores OPEN/MAX/CLOSE state rather than every peak event.
2. Added periodic batch-oriented flushing of buffered signal snapshots. A database outage no longer blocks the Tracker or market-data path merely because PostgreSQL is unavailable.
3. Added per-user `UpdateStep` and `/setupdates <percentage_points>`. `0.3` means +0.3 percentage points, not +30 percent.
4. Added in-memory per-user/per-signal UPDATE threshold tracking to suppress duplicate UPDATE notifications.
5. Changed the notification router from a bounded notification channel to a deduplicating pending map plus wake channel. This removes the previous hard queue-size loss mode in the router itself.
6. Added exact-route revalidation against the aggregator's latest in-memory prices/funding state immediately before an OPEN/UPDATE notification is sent. User filters are rechecked at that point as well.
7. Added an explicit aggregator janitor goroutine, so cleanup is not dependent on receiving market ticks.
8. Removed the global application-wide command mutex. User commands no longer serialize behind unrelated slow commands such as `/stats` or connector lifecycle operations.
9. Changed Telegram command/send channels to use a shutdown signal instead of being closed by `Bot.Close`, eliminating the send-on-closed-channel lifecycle race.
10. Fixed the startup geo-block status parser so short status strings cannot cause a slice-bounds panic.
11. Updated the Go toolchain requirement/container builder to Go 1.26.8.
12. Updated Prometheus to 3.13.3.
13. Bound Prometheus, Alertmanager, Grafana and the application's 9090 endpoint to localhost in Compose. They are no longer exposed directly on all VPS interfaces.
14. Removed the secret-bearing `.env` from the deliverable archive. Only `.env.example` remains.

## Audit A — static/concurrency audit

### PASS / materially improved

- No production channel is closed while a known producer is still intentionally writing to it in the Telegram Bot lifecycle.
- Telegram `commands` and `sendChan` are no longer closed by `Bot.Close`; `sendStop` is the shutdown signal.
- Connector streams have per-entry WaitGroups and cancellation ownership.
- Aggregator shards protect their maps with shard-local mutexes.
- UserManager callbacks now operate on a point-in-time snapshot, so long callbacks do not hold the UserManager read lock.
- Tracker owns active-signal state behind one mutex and snapshots signals before passing them to asynchronous components.
- Persistence snapshots are immutable copies; newer snapshots supersede older buffered snapshots by signal ID/version.
- Aggregator janitor is independent from tick arrival.
- Application command processing is no longer globally serialized.
- Exact-route revalidation does not scan the whole exchange universe.

### Remaining risks

1. `ProcessTick` can still block when the lifecycle event channel itself is saturated. This is intentional backpressure for critical OPEN/CLOSE state, but it must be covered by load tests.
2. `NotificationRouter` can accumulate pending jobs while Telegram is unavailable. The map is bounded by notification identity rather than raw transport attempts, but an extended outage with very high signal churn can still consume RAM.
3. Telegram's low-level `sendChan` still intentionally drops ordinary command replies when its queue is full. Signal delivery is handled separately at the router layer; this behaviour is acceptable for non-critical command responses but should remain observable.
4. The current Telegram interface does not provide an end-to-end acknowledgement from the Telegram API back to the router. Therefore transport acceptance and confirmed Telegram delivery are still distinct concepts.
5. The process-local DB buffer is lost if the process itself crashes while PostgreSQL is unavailable. This is an accepted product trade-off because historical signal durability is not critical.

## Audit B — adversarial scenario audit

### Scenario 1: PostgreSQL unavailable for hours

Expected after the changes:

`signal -> Tracker -> memory buffer (latest snapshot per ID)`

The Tracker does not wait for PostgreSQL and no 100k queue exhaustion is required for correctness. When PostgreSQL returns, snapshots are flushed and UPSERTed.

### Scenario 2: signal peak changes 1000 times during DB outage

Expected DB state:

- one row for the signal;
- initial spread from OPEN;
- latest peak spread;
- final spread/close time/duration after CLOSE.

No 1000-row update history is created.

### Scenario 3: Telegram unavailable before OPEN delivery

The router retains a pending notification rather than relying on a fixed-size FIFO. Immediately before a queued OPEN/UPDATE is sent, the exact route is revalidated against fresh in-memory prices/funding and current user filters. If the route no longer passes the global hard floor, the stale notification is discarded.

### Scenario 4: user changes filters during Telegram outage

The original target list is not trusted as the final authorization decision. The current UserManager state is checked again during revalidation before OPEN/UPDATE delivery.

### Scenario 5: spread jumps from 2.0% to 3.1%, user step = 0.3 percentage points

The UPDATE policy records threshold progress and sends the current peak once rather than replaying 2.3%, 2.6% and 2.9% as a burst.

### Scenario 6: concurrent peak updates

Per-signal/per-user UPDATE state is mutex-protected. The same threshold cannot be claimed twice by two concurrent observations.

### Scenario 7: SIGTERM during Telegram polling

Polling is stopped by context; main waits for the polling goroutine before deferred BotPool.Close. Bot channels are not closed, so the previous producer/consumer channel-close race is removed.

### Scenario 8: all market feeds stop

Periodic janitor execution still runs every 30 seconds. Cleanup is no longer coupled to `ticks % 10000`.

### Scenario 9: exchange availability endpoint returns `HTTP 500`

The status classifier uses `strings.HasPrefix/HasSuffix` instead of slicing the string from the end. Short status strings cannot trigger a slice-bounds panic.

## Tests not executable in this audit environment

The archive requires Go 1.26.8 after the hardening changes, while the audit container has Go 1.23.2. Network/DNS access is unavailable, so the required toolchain and modules could not be downloaded.

`gofmt` was run successfully over all changed Go files. A local compile/test attempt with the module Go version lowered only for the temporary test copy reached dependency resolution and failed because external modules were unavailable; it did not report a Go syntax error before dependency resolution.

Therefore the following are REQUIRED before merging/deploying:

```text
go test ./...
go test -race ./...
go vet ./...
govulncheck ./...
```

And runtime soak tests must cover:

- 100/500/1000 concurrent users;
- reconnect storms on all eight exchange adapters;
- Telegram outage/recovery;
- PostgreSQL outage/recovery;
- 1000+ peak updates on one signal;
- concurrent OPEN/UPDATE/CLOSE for the same route;
- SIGTERM during polling, DB flush and connector reconnect;
- goroutine/heap/file-descriptor measurements before and after repeated reconnect cycles.

## Final status

The most severe pre-change coupling — PostgreSQL/Telegram backpressure propagating into the market-data pipeline — has been substantially reduced. The signal persistence path now matches the agreed compact lifecycle model, and user-specific UPDATE thresholds are implemented.

The remaining highest-priority item is end-to-end Telegram delivery acknowledgement/retry semantics. The current code now revalidates stale OPEN/UPDATE notifications immediately before transport, but a failure occurring after a message enters the low-level Telegram queue is not yet represented as a durable router-level acknowledgement. This should be the next focused engineering task rather than introducing a new distributed infrastructure layer.
