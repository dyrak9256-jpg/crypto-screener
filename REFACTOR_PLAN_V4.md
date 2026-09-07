# Refactor V4 — execution order

## P0 — correctness / safety blockers
1. Fix compile/test API break (`UpdatePeak`) and normalize domain signal lifecycle.
2. Remove reverse SPOT/FUTURE route completely: only `SPOT BUY -> FUTURES SHORT` is generated.
3. Make funding route-aware:
   - SPOT/FUTURES: only futures-leg funding.
   - FUTURES/FUTURES: `net = spread + sellFunding - buyFunding`.
   - missing/stale/unhealthy funding is fail-closed.
   - signal carries both funding legs and both next funding timestamps.
4. Implement funding data for all 8 exchanges and connect health updates.
5. Make `/setfundingtime` an actual notification gate using the nearest funding timestamp.
6. Remove the administrator hard minimum volume entirely. Only spread has a global hard floor of 1%.
7. Fix application startup rollback so a connector/factory failure tears down all started workers/connectors.
8. Make websocket ingress latest-value/coalescing instead of dropping newest ticks.

## P1 — market-data integrity
9. Separate exchange `EventTime` from local `ReceivedAt`.
10. Add websocket heartbeat/read deadlines and reconnect jitter.
11. Respect conservative per-connection subscription limits:
    - Binance candles: 500 streams/connection.
    - BingX candles: 50/connection.
    - Bitget candles: 50/subscription batch.
    - Bybit: 10 spot / 50 futures subscription batches.
    - Gate: 100/connection batch.
    - KuCoin: 100/connection (below documented 200-topic ceiling).
    - MEXC spot: exactly 30/connection.
    - OKX: 100/batch, with total request payload kept far below the 64KB limit.
12. Run candle shards concurrently instead of sequentially blocking on the first shard.
13. Fix MEXC spot protobuf kline decoding with a minimal wire decoder matching the documented schema.
14. Fix Gate spot kline result shape and quote-volume field.
15. Fix OKX quote turnover index (`volCcyQuote`, index 7).
16. Validate REST status/error codes and reject empty symbol discovery where applicable.

## P1 — volume engine
17. Keep 1-minute quote turnover as the canonical source for interval volume.
18. Replace current-minute buckets instead of accumulating repeated updates.
19. Preserve known zero-volume buckets.
20. Add cold-start projection:
    `projected_window = observed_sum / known_minutes * requested_window_minutes`.
    Incomplete windows are explicitly marked as estimates; complete windows are exact.
21. Keep route volume as `min(buy_leg_volume, sell_leg_volume)`.
22. Add stale-data checks and series/bucket janitors.
23. Remove all global administrator volume floors.

## P2 — lifecycle / resilience
24. Fix connector replacement semantics so the old connector is retained until it actually stops.
25. Ensure shutdown drains coalesced ingress before closing the tick channel.
26. Bound persistence shutdown and continue cleanup on DB failure.
27. Prevent NotificationRouter send/close races using a stop channel instead of closing a live jobs channel.
28. Add stale funding/price/volume cleanup hooks.

## P3 — tests / verification
29. Add funding direction tests for SPOT/FUTURES and FUTURES/FUTURES.
30. Add funding-time notification tests.
31. Add volume cold-start/replacement/zero-volume tests.
32. Add adapter fixtures/tests for protobuf/object/quote-volume decoder paths and subscription sharding.
33. Run full CI verification with Go 1.26.4:
    `go test ./...`, `go test -race ./...`, `go vet ./...`, `go build ./...`.
34. Only after functional correctness is green, remove remaining obsolete compatibility/dead code and keep only fields required for persistence/backward migration.
