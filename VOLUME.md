# Interval volume engine

The screener now consumes native 1-minute candle WebSocket feeds from all configured exchanges. It does **not** subscribe to order-book depth or individual trades for the volume filter.

The hot path is:

`exchange WS -> candle decoder -> VolumeEngine -> minute bucket replacement -> route filter -> arbitrage engine`

For an open candle, exchanges send repeated updates. The engine replaces the value for the same `(exchange, symbol, market, minute)` bucket; it never adds repeated updates together. Therefore a 5-minute volume is the sum of five minute buckets, a 30-minute volume is thirty buckets, etc.

Supported user timeframes:

- 1m
- 5m
- 15m
- 30m
- 1h
- 4h
- 24h (existing rolling ticker volume)

For an arbitrage route, both legs are checked and the weaker leg wins:
`route_volume = min(buy_leg_volume, sell_leg_volume)`.

## Load

This is substantially lighter than consuming depth/trades. One 1m candle stream is maintained per symbol/market. Higher windows are calculated locally, so we do not subscribe separately to 5m, 15m, 30m, 1h or 4h streams.

The service uses a small number of long-lived WebSocket connections per exchange (Binance shards because its combined stream has a per-connection stream limit; the other adapters multiplex subscriptions over their public market-data connections).

## Cold start

A WebSocket candle stream normally starts with the current candle rather than the complete historical 24h candle set. Therefore an interval filter is intentionally conservative immediately after a cold start: a 30m filter becomes fully populated after 30 minutes of live candle updates unless a historical backfill is added for that exchange.

No historical volume is fabricated from 24h ticker volume.
