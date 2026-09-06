# 📋 CHANGELOG — Изменения проекта crypto-screener

Ветка: `fix_file1.1`
Период документирования: доработка по итогам read-only аудита (P3–P13).

---

## 🆕 fix_file1.1 — исполняемый спред, lossless-конвейер, единый реконнект

Все шаги прошли `go build ./...`, `go vet ./...`, `go test -race ./...` (все пакеты `ok`), `gofmt`.

### P3 — Единый владелец переподключения
- **Файлы:** `internal/adapters/{binance,bingx,bitget,bybit,gateio,kucoin,mexc,okx}.go`
- **Было:** каждый адаптер крутил собственный `for{…reconnecting…}`, супервизор `runWithReconnect` был мёртвым кодом (двойной реконнект).
- **Стало:** `ConnectSpot/Futures/Funding` открывают и читают **одно** соединение и возвращают ошибку dial/read. Переподключение с backoff — **только** в `ConnectorManager.runWithReconnect`.

### P4/P5/P6/P8 — Исполняемый спред по bid/ask + валидация + свежесть + интервальный объём
- **Файл:** `internal/app/sharded_aggregator.go`
- Спреды по реальным ценам: cross = `(maxBid − minAsk)/minAsk`; intra — положительное направление базиса (не mid-price).
- Валидация `bid>0, ask>0, bid≤ask`; отсев устаревших тиков (`staleWindow`).
- `SpreadEvent.QuoteVolume` — интервальный объём из минутных дельт (TF_5m), а не 24h-rolling с биржи.

### P12/P13 — Пер-биржевой funding
- **Файлы:** `funding_manager.go`, `sharded_aggregator.go`, `ports.go`, `market.go`, `binance.go`
- `FundingRate.Exchange` + `rates[symbol][exchange]`; `IsArbProfitable(..., exchanges ...string)` проверяет каждую вовлечённую биржу.

### P7/P9/P10 — Lossless-конвейер и отслеживаемый жизненный цикл
- **Файлы:** `internal/app/tracker.go`, `sharded_aggregator.go`, `app.go`
- Отправка в `trackerChan`/`dbChan` — блокирующая и вне мьютекса (нет тихой потери).
- Горутины уведомлений отслеживаются `routerWg`; shutdown дожидается их завершения.
- Детерминированная цепочка: ingest → close `trackerChan` → tracker → router → close `dbChan` → persistence.
- Один tracker-воркер → последовательная обработка событий одного ключа.

### P11 — Безопасное закрытие Telegram
- **Файлы:** `internal/adapters/telegram/bot.go`, `cmd/screener/main.go`
- `Close()` идемпотентная (`closeOnce`), `sendChan` не закрывается (нет `send on closed channel`); `trySend` после Close — no-op; worker дочитывает очередь. В `main` добавлен `tgBot.Close()`.

### Новые автотесты
- `sharded_aggregator_test.go` — валидация bid/ask; отсев устаревших; пер-биржевой funding (не гасит сигнал по вовлечённым биржам).
- `app_test.go` — `Run` дожидается завершения горутин уведомлений до возврата.

---
