# crypto-screener2 refactored snapshot

This is the refactored project snapshot produced from crypto-screener2.

Important: this environment has Go 1.23.2 while go.mod requires Go 1.26.4, and network access is unavailable. Therefore `go test ./...`, `go test -race ./...`, and `go vet ./...` could not be executed here. All Go sources were parsed successfully by `gofmt` after the changes.

The original `.env`, `.git`, coverage artifact and `test.txt` were intentionally excluded.

Main changes:
- unified market/funding connector capabilities;
- connector lifecycle/reconnect ownership fixed;
- executable bid/ask arbitrage route and direction;
- stale price/funding protection;
- funding keyed by exchange+symbol;
- tracker ordering and immutable snapshots;
- OPEN signal persistence;
- persistence retry and bounded shutdown;
- user pointer aliasing and command ordering fixed;
- admin authorization fail-closed;
- bounded Telegram command workers and idempotent shutdown;
- WebSocket read limits and MEXC single-writer protection;
- Bybit pagination and spot batching;
- Gate ticker subscriptions now use explicit symbol lists instead of `!all`;
- KuCoin volume parsing;
- strict config parsing;
- Dockerfile/compose/go-version consistency;
- schema extended for route/funding/volume audit fields;
- stale tests rewritten to match the new contracts.
