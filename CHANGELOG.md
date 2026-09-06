# 📋 CHANGELOG — Изменения проекта crypto-screener

Ветка: `fix_bag_if_progect`
Период документирования: рефакторинг по результатам аудита.

Изменения сгруппированы по типам. Для каждой правки указан файл-источник и характер изменения. Ниже — сводная таблица «проблема → что сделано → где».

---

## 🏷 Обзор
Проект приведён к **рабочему состоянию**: `go build`, `go vet`, `go test -race ./...` и `gofmt` проходят без ошибок. Устранены: ошибка сборки (`AddConnector`), краш-паника WaitGroup, 2 падавших теста Binance, data race в `Tracker`, потеря объёма сигнала при персистенции, дыра безопасности с админ-командами. Подключены **все 8 биржевых адаптеров** и унифицирована версия Go.

---

## 🧩 Архитектура и подключение бирж

### Новый реестр коннекторов
- **Файл:** `internal/adapters/registry.go` (новый)
- **Что сделано:**
  - `Supported() []Exchange` — единый список всех 8 бирж (Binance, Bitget, BingX, Bybit, Gate.io, KuCoin, MEXC, OKX) с фабриками `New func() domain.ExchangeConnector`.
  - `NewByName(name)` — создание коннектора по имени (без учёта регистра).
- **Зачем:** раньше только Binance был подключён, остальные 7 адаптеров были «мёртвым» кодом. Теперь подключение единообразное и расширяемое.

### Подключение всех бирж при старте
- **Файл:** `cmd/screener/main.go`
- **Что сделано:** вместо одиночного `AddConnector("BINANCE", ...)` выполняется цикл по `adapters.Supported()` — все 8 бирж стартуют (Spot/Futures/Funding) с авто-реконнектом. Также добавлен `cm.StopAll()` после `Run` для детерминированного завершения соединений.

### Hot-swap через реестр
- **Файл:** `internal/app/app.go` (`case "addex"`)
- **Что сделано:** `/addex` теперь через `adapters.NewByName(name)` поддерживает ЛЮБУЮ зарегистрированную биржу (раньше — только `BINANCE`). Обязательная проверка ошибки `AddConnector`.

### Реализация `ExchangeConnector` для всех адаптеров
- **Файлы:** `internal/adapters/{bingx,bitget,bybit,gateio,kucoin,mexc,okx}.go`
- **Что сделано:** добавлен `ConnectFunding(ctx, sink)` — блокирующий метод, ожидающий `ctx.Done()`, чтобы:
  - соответствовать интерфейсу `domain.ExchangeConnector` (иначе адаптер не подходил к `AddConnector`);
  - корректно работать с supervisor-ом (`runWithReconnect`): при отмене контекста возвращается `nil` и горутина штатно завершается.

---

## 🐞 Критические исправления (сборка/паники/расы)

### Порядок аргументов `AddConnector` — исправлена ошибка сборки
- **Файл:** `internal/app/connector_manager.go` (`func (cm *ConnectorManager) AddConnector`)
- **Было:** сигнатура `(parentCtx, name, conn)`, а все вызовы — `(name, conn, ctx)`.
- **Стало:** сигнатура приведена к `(name string, conn domain.ExchangeConnector, parentCtx context.Context)` — совпадает с `app.go:369`, `main.go`, тестом.
- **Результат:** проект снова компилируется.

### Паника `negative WaitGroup counter` в Telegram-боте
- **Файл:** `internal/adapters/telegram/bot_test.go`
- **Что сделано:** в тесте, создающем `Bot` напрямую (минуя `NewBot`), добавлен `bot.wg.Add(1)` перед `go bot.sendWorker()` — `defer b.wg.Done()` больше не уходит в минус.

### Data race в `Tracker` → `NotificationRouter`
- **Файл:** `internal/app/tracker.go`
- **Было:** `go t.router.ProcessSignal(signal, ...)` — горутина читала `PeakSpread/FinalSpread`, пока `HandleEvent` мог одновременно мутировать те же поля через `UpdatePeak/Close`.
- **Стало:** роутеру передаётся **копия-снапшот** (`snapshot := *signal` / `*newSignal`).
- **Результат:** `go test -race` по пакету `app` — чисто.

### Тест `ConnectorManager`
- **Файл:** `internal/app/connector_manager_test.go`
- **Что сделано:** переведён с несуществующего поля `cm.connectors` на `cm.entries`; переписан под реальную семантику hot-swap (замещение старого коннектора новым, отмена контекста старого, удаление). Устранены и ошибка gomock (превышение `Times(1)`), и перенос WaitGroup.

---

## 🔧 Надёжность и безопасность

### Ограничение админ-доступа к `/addex`/`/rmex`
- **Файл:** `internal/app/app.go` (`isAdmin`)
- **Было:** при пустом `ADMIN_CHAT_IDS` функция возвращала `true` — команды были доступны любому пользователю.
- **Стало:** при пустом списке возвращается `false` — админ-команды недоступны никому, пока не заданы `ADMIN_CHAT_IDS`.

### Nil-guard Telegram-отправителя в роутере
- **Файл:** `internal/app/notification_router.go` (`ProcessSignal`)
- **Что сделано:** перед `nr.telegram.Broadcast(...)` проверка `if nr.telegram == nil` (с логированием). Исключён риск nil-pointer dereference, если отправитель не был инжектирован.

### Сохранение объёма сигнала в БД
- **Файлы:** `schema.sql`, `internal/adapters/postgres/repo.go`
- **Что сделано:** добавлена колонка `quote_volume NUMERIC(20,8) NOT NULL DEFAULT 0` в `signals` (схема + INSERT ON CONFLICT). Раньше `ArbitrageSignal.QuoteVolume` терялся — историческую аналитику объёма было не восстановить.

---

## 💬 Telegram-бот: форматирование и отправка

### Убран двойной escape Markdown
- **Файл:** `internal/adapters/telegram/bot.go`
- **Было:** `ParseMode = ModeMarkdownV2`, а весь текст прогонялся через `EscapeMarkdownV2` (включая намеренную разметку шаблонов) — `*bold*`/`` `code` `` превращались в литеральные `\*`.
- **Стало:** `ParseMode = ModeMarkdown`; экранирование применяется к динамическим (пользовательским) данным, шаблоны сохраняют разметку.

### Неблокирующие отправки
- **Файл:** `internal/adapters/telegram/bot.go` (`SendPrivateMessage`, `Broadcast`)
- **Что сделано:** оба метода переведены на `select { case chan <- msg: default: }` — при заполненном буфере сообщение отбрасывается с логом, а не блокирует polling/обработку команд (соответствует задокументированной политике non-blocking). Тест `TestBot_NonBlockingDropWhenFull` — в соответствии.

---

## ⚙️ Инфраструктура и версии

### Унификация версии Go — `1.26`
- **Файлы:** `go.mod` (`go 1.26`), `Dockerfile` (`FROM golang:1.26-alpine`), `README.md` («Go 1.26+»).
- **Было:** `go.mod` требовал `1.26.4`, а Docker собирался из `golang:1.22` + `GOTOOLCHAIN=auto` (догрузка toolchain из сети при сборке).
- **Стало:** единая версия без необходимости догружать toolchain при сборке в контейнере.

### Переименование `dockerfile` → `Dockerfile`
- **Файл:** `docker-compose.yml` ссылается на `dockerfile: Dockerfile`, а файл назывался `dockerfile` (в нижнем регистре). На Linux (регистрозависимая ФС) `docker compose build` падал с «Dockerfile not found».
- **Стало:** `git mv dockerfile Dockerfile`.

### Переименование `REARME.md` → `README.md`
- **Файл:** `README.md` (был `REARME.md`) — исправлена опечатка в имени, контент дополнен (переменные окружения, команды бота, структура, все биржи).

### `.env.example`
- **Файл:** `.env.example` (новый) — шаблон переменных окружения (`DATABASE_URL`, `TELEGRAM_TOKEN`, `ADMIN_CHAT_IDS`, `HARD_MIN_SPREAD`, `HARD_MIN_VOLUME`).

### `STRUCTURE.md`
- **Файл:** `STRUCTURE.md` — обновлён под актуальное дерево (8 бирж, `registry.go`, `connector_manager`, `funding_manager`, `Dockerfile` и т.д.).

---

## 🧹 Гигиена и уборка

### `coverage` исключён из git
- **Файл:** `coverage`, `.gitignore`
- **Что сделано:** профиль покрытия удалён из индекса (`git rm --cached coverage`) и добавлен в `.gitignore`. (Локальная копия может оставаться на диске.)

### Точность тестов и форматирование
- `internal/adapters/binance/binance_test.go` — тестовые сообщения приведены к реальному протоколу `!ticker@arr` (массив тикеров); проверка текста ошибок согласована с фактическим `"dial: ..."`.
- `internal/app/app_test.go` — контекст для `atomic.Pointer` теперь ставится через `app.ctx.Store(&appCtx)`; проверки сообщений согласованы с фактическим (русскоязычным) текстом.
- Весь код прогнан через `gofmt`; `go mod tidy` (без изменения зависимостей).

---

## ✅ Итоговая проверка состояния

| Действие | Статус |
|----------|--------|
| `go build ./...` | ✅ |
| `go vet ./...` | ✅ |
| `go test -race ./...` | ✅ |
| `gofmt -l .` | ✅ (пусто) |
| `go mod tidy` | ✅ |

**Остаточные риски** (описаны в `AUDIT_PROBLEMS.md`, ID H1–H6): дублирующие WS-соединения из-за мгновенного возврата `Connect*`, funding без привязки к бирже, неверифицированные на живых биржах адаптеры, мёртвый код (`repo` в `NewApplication`, поля конфигурации, `GetUserByChatID`), неявный гистерезис порогов, отсутствие CI.

---

## 🔧 Второй этап рефакторинга (по повторному аудиту)

Цель: устранить оставшиеся находки второго аудита и покрыть их автотестами.

### H1 — устранены дубли WebSocket-соединений
- **Проблема:** `ConnectSpot/ConnectFutures` возвращали `nil` сразу после `go a.listen(...)`. Супервизор `runWithReconnect` трактовал это как «соединение оборвалось» и периодически перезапускал поток (лог: `disconnected after 0s: <nil>. Retry in 1s...`), порождая дубли WS-подключений.
- **Изменено:** все адаптеры (`binance, bingx, bitget, bybit, gateio, kucoin, mexc, okx`) теперь вызывают `listen`/`listenSpot`/`listenFutures`/`listenFunding` **синхронно** — метод блокируется на время жизни соединения. Супервизор вызывает `connectFn` ровно один раз.
- **Файлы:** все `internal/adapters/*/*.go`.
- **Тесты:** `connector_manager_test.go` (`TestRunWithReconnect_BlockingConnectFnCalledOnce`, `..._ExitsOnCancel`); по одному новому тесту на адаптер (`*_test.go`).

### H2 — funding с привязкой к бирже
- **Проблема:** `FundingManager` хранил одну ставку на символ; ставка затиралась любой последней биржей, фильтр прибыльности работал неверно для пар с разными ставками.
- **Изменено:** `FundingRate` получил поле `Exchange`; хранилище `map[symbol]map[exchange]FundingRate`; интерфейс `FundingSink.UpdateFunding(exchange, symbol, rate, nextTime)`; `IsArbProfitable(..., exchanges ...string)` проверяет каждую вовлечённую биржу (отсутствие данных по бирже = пермиссивно).
- **Файлы:** `internal/domain/market.go`, `internal/domain/ports.go`, `internal/app/funding_manager.go`, `internal/adapters/binance/binance.go`, `internal/app/sharded_aggregator.go`.
- **Моки:** перегенерированы `mock_ports.go` → добавлена директива `//go:generate` в `ports.go`.
- **Тесты:** `funding_manager_test.go` (пер-биржевые ставки, пермиссивность).

### M2 — убран неиспользуемый параметр `repo` из `NewApplication`
- Файлы: `internal/app/app.go`, `cmd/screener/main.go`, `internal/app/app_test.go`.

### M3 — очистка конфигурации
- Удалены `Config.TelegramChatID` и чтение `TELEGRAM_CHAT_ID` (`internal/config/config.go`).
- Удалены мёртвые `user*`-поля/геттеры в `ScreenerConfig`; `GetCloseThreshold` теперь от `hardMinSpread` с явным гистерезисом (`internal/domain/config.go`).
- Тесты обновлены (`config_test.go`, `signal_test.go`).

### M4 — валидация env-порогов
- `config.Load` проверяет парсинг `HARD_MIN_SPREAD`/`HARD_MIN_VOLUME`; при ошибке — лог и фолбэк на дефолт (раньше ошибка молча обнуляла порог).
- Тест: `TestConfig_Load_InvalidSpreadFallsBackToDefault`.

### M5 — убран неиспользуемый `GetUserByChatID`
- Файл: `internal/adapters/postgres/repo.go`.

### Новые автотесты
- `internal/adapters/registry_test.go` — `Supported()` (8 бирж), `NewByName` (регистронезависимо + ошибка), компиляционная проверка порта.
- `*_test.go` для bingx/bitget/bybit/gateio/kucoin/mexc/okx — промпт-возврат на отменённом контексте + реализация `ExchangeConnector`.
- `internal/app/funding_manager_test.go`, `internal/app/connector_manager_test.go`, `internal/config/config_test.go` — расширены.

### Verification
- `go build ./...` ✅, `go vet ./...` ✅, `go test -race ./...` ✅, `gofmt -l .` ✅ (чисто), `go test -cover` показывает покрытие: domain 100%, app 84%, config 82%, telegram 70%.
