# V4 implementation notes

- The global volume floor is gone. `HARD_MIN_SPREAD` is the only administrator hard floor and is clamped to >= 0.01.
- `/setvol` is purely per-user.
- `/setfundingtime` is per-user and enforced in `NotificationRouter` using the nearest non-zero funding timestamp.
- Reverse spot-short/futures-long logic is intentionally absent.
- Funding is fail-closed: no funding snapshot means no futures-containing signal.
- FUT/FUT funding is route-directional: a long pays positive funding and a short receives positive funding, therefore `sellRate - buyRate` is added to the gross spread.
- Interval volume is projected during cold start from the observed minute average and becomes exact once the full interval is observed.
- Repeated updates to the same minute replace the bucket.
- Zero-volume minutes are stored as known data rather than treated as missing data.
- Market-data ingress keeps the newest value per `(exchange,symbol,market)` and emits it downstream through a bounded channel.
- Candle feeds use only 1-minute data; higher intervals are calculated locally.
- MEXC spot kline payloads are decoded as protobuf wire data rather than being incorrectly passed through a JSON decoder.
- Exchange-specific subscription batching is deliberately conservative to stay below documented limits.
