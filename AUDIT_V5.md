# V5 повторный аудит и рефакторинг

Дата: 2026-09-07

## Правило работы

Перед изменением кода повторно просмотрены все Go-файлы проекта и отдельно перепроверены:

- domain/config, market, signal, user, ports;
- Application lifecycle и shutdown;
- FundingManager;
- ShardedAggregator;
- VolumeEngine;
- Tracker / NotificationRouter / PersistenceWorker;
- ConnectorManager / ingress / retry;
- PostgreSQL / Telegram;
- все 8 exchange adapters;
- существующие unit/integration tests.

После изменений повторно выполнены форматирование, синтаксический parse всех Go-файлов и статические проверки по критическим регрессиям.

## Исправлено в V5

### P0 correctness

1. Удалено старое поле `userCrossSpread`, из-за которого V4 не компилировался.
2. Удалены дублирующиеся `CandleConnector` / `CandleSink` из `domain/ports.go`.
3. SPOT/FUTURES funding теперь route-aware:
   - SPOT BUY = funding 0;
   - FUTURES SHORT = funding sell leg;
   - net = spread + sellFunding.
4. Funding для FUT/FUT остался `spread + sellFunding - buyFunding`.
5. Funding fail-closed: отсутствие/старость/невалидность funding не превращается в прибыльный raw spread.
6. Bybit futures ticker теперь реально пишет `fundingRate` + `nextFundingTime` в FundingManager.
7. Bybit subscription args шардируются по сериализованному размеру с запасом относительно лимита 21k.
8. OKX funding вынесен в отдельные шардированные funding connections; ticker connection больше не дублирует funding subscriptions.
9. MEXC spot kline теперь декодируется по документированному JSON `publicspotkline`; неизвестный binary payload не silently принимается за угаданный protobuf.

### Volume

10. Интервальный volume prefilter выполняется до дорогого O(E²) поиска маршрутов.
11. Cold-start projection использует только непрерывную последовательность текущих минут; пропуски обрывают sample.
12. Повторное обновление той же свечи заменяет bucket.
13. Нулевой volume считается известным значением; адаптеры больше не отбрасывают zero-volume candles.
14. Route volume теперь настоящий `min(buy, sell)`, включая корректный случай нулевого объёма.
15. Административного hard minimum volume нет; volume остаётся пользовательским фильтром.

### Runtime configuration

16. `/sethardspread` сначала обновляет FundingManager, затем canonical ScreenerConfig; runtime hard floor больше не расходится с funding floor.
17. Пользовательский spread остаётся персональным и clamped к hard floor.

### Lifecycle / concurrency

18. Ingress coalescer больше не оставляет latest tick навсегда без wake-up.
19. Ingress имеет явное stopped-состояние: поздний producer после timeout shutdown не может писать в закрытый tick channel.
20. Registry очищается только после подтверждения остановки producers.
21. NotificationRouter при Close сначала прекращает intake, затем drains уже принятые jobs.
22. Funding/notification clock сделан injectable.
23. Persistence получает отдельный shutdown context и продолжает retry уже принятых DB events после отмены market-data context.
24. `NewApplication` больше не panic'ит на ошибке funding config; constructor возвращает error.
25. Reconnect loops переведены на connection-local exponential backoff с jitter и reset после длительного стабильного соединения.
26. Ошибки на adapter/repository/application boundaries обёрнуты через `%w` с контекстом операции.
27. Убраны `http.DefaultClient` из exchange adapters в пользу clients с timeout.

### Exchange filtering

28. Binance/BingX/MEXC ticker streams дополнительно фильтруют только USDT instruments до попадания в hot path.
29. Bitget spot symbol discovery фильтрует `quoteCoin=USDT`.
30. KuCoin futures discovery фильтрует `quoteCurrency=USDT` вместо loose `Contains("USDT")`.

## Дополнительные тесты

Добавлены/усилены проверки:

- adverse funding для SPOT/FUTURES;
- sparse volume gap;
- ingress latest-value delivery;
- dynamic hard-spread propagation в FundingManager;
- deterministic funding-time clock.

## Что нельзя честно объявить пройденным в sandbox

Среда не имеет Go 1.26.4 и не может скачать toolchain/dependencies из сети. Поэтому V5 не помечается как полностью build-verified.

Обязательно выполнить на машине с Go 1.26.4:

```bash
go version
go test ./...
go test -race ./...
go vet ./...
go build ./...
docker compose config
docker compose build
```

После этого нужен live/soak прогон всех 8 бирж с reconnect, DB outage, Telegram outage, rejected subscriptions и shutdown/restart cycles.
