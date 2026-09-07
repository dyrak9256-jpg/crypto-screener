# V6 Refactor Notes

## Main changes

- Reworked generic WebSocket heartbeat to use gorilla/websocket `WriteControl` for Ping control frames.
- Added explicit read-deadline error handling.
- Added candle reconnect exponential backoff with jitter to all exchange adapters.
- Added protocol-level subscription error handling where supported.
- Fixed all production FundingSink error handling and wrapping.
- Added Bybit keepalive implementation.
- Added MEXC Spot protobuf decoder for `PushDataV3ApiWrapper` / `PublicSpotKlineV3Api`.
- Added MEXC protobuf fixture tests.
- Decoupled notification target calculation from Telegram initialization.
- Added notification worker pool and deferred delivery until Telegram becomes available.
- Added persistence worker pool.
- Added bounded tracker persistence enqueue timeout with explicit logging.
- Added tracker shutdown flush timeout.
- Explicitly marks funding streams unhealthy when connector funding loops exit.
- Removed misleading `GetSymbolVolume` legacy API.
- Added retry and heartbeat tests.
- Added notification target calculation test without Telegram transport.

## Error policy

Errors crossing adapter/application boundaries are wrapped with `%w` and include operation plus exchange/symbol context where available.

Background goroutine errors that cannot be returned synchronously are wrapped before logging and cause the owning connection to terminate/reconnect where appropriate.

No production ignored `UpdateFunding` or `SetReadDeadline` errors remain.
