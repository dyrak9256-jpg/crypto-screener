# V5 Refactor Notes

## Main design changes

- FundingManager is the single fail-closed source of route funding state.
- SPOT/FUTURES is strictly SPOT BUY -> FUTURES SHORT.
- Interval volume is a prefilter before cross-exchange candidate generation and is rechecked per user before notification.
- Ingress is latest-value/coalescing, keyed by exchange/symbol/market.
- Lifecycle events remain ordered through Tracker and are persisted asynchronously.
- Persistence has independent shutdown lifetime so accepted events can drain during graceful shutdown.
- All exchange adapters use bounded reconnect behavior and explicit USDT filtering.

## Error policy

Errors at package/component boundaries are wrapped with `%w` and operation context. The original error remains available to `errors.Is` / `errors.As`.

Examples:

- `fetch symbols for FUTURES: ...`
- `read ticker shard: ...`
- `update Bybit funding for BTCUSDT: ...`
- `save signal <id>: ...`
- `shutdown: connector shutdown incomplete: ...`

## Important operational property

If a connector ignores cancellation and survives its shutdown timeout, ingress is marked stopped before the tick channel is closed. A late market-data producer is ignored instead of panicking on a closed channel.
