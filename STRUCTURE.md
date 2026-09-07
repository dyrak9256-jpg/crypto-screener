# Structure

Полная карта кодовой базы: где живёт каждый компонент и за что отвечает.

```
crypto-screener/
├── cmd/
│   ├── screener/main.go        # точка входа: slog, мульти-боты Telegram, метрики, стартовая проверка бирж
│   └── smoke/main.go           # натурный smoke-тест: полный конвейер + метрики + graceful shutdown (ручной запуск)
├── internal/
│   ├── adapters/
│   │   ├── registry.go         # реестр бирж: фабрики коннекторов по имени
│   │   ├── binance/            # WS: !ticker@arr (spot/futures), markPrice funding, 1m klines
│   │   ├── bingx/              # WS тикеры spot/swap, REST funding
│   │   ├── bitget/             # WS тикеры, REST списки символов
│   │   ├── bybit/              # WS v5 tickers батчами, REST списки
│   │   ├── gateio/             # WS v4 spot/futures tickers, REST funding poll
│   │   ├── kucoin/             # WS spot/futures, REST funding poll
│   │   ├── mexc/               # WS spot/futures, ручной protobuf-декодер, REST funding poll
│   │   ├── okx/                # WS v5 tickers + funding-rate
│   │   ├── postgres/           # pgx-пул, schema.sql (embedded), signals/users/settings
│   │   └── telegram/           # Bot (id, send/command очереди), BotPool (мульти-токены)
│   ├── app/
│   │   ├── app.go              # композиция, Run/cleanup, команды, настройки, метрики, RouteBot
│   │   ├── sharded_aggregator.go # 1024 шарда, cross/intra кандидаты, net-spread (с комиссиями), lifecycle
│   │   ├── funding_manager.go  # funding-ставки, staleness, health, возраст для метрик
│   │   ├── volume_engine.go    # поминутные объёмы, окна 1m–4h, retention 4h+2m
│   │   ├── tracker.go          # активные сигналы, OPEN/PEAK/CLOSE, персистентность+уведомления
│   │   ├── notification_router.go # фильтры пользователей, формат сообщений, доставка
│   │   ├── persistence_worker.go  # запись сигналов с retry/backoff
│   │   └── connector_manager.go   # hot add/remove коннекторов, StopAll с таймаутами
│   ├── domain/                 # чистое ядро: порты, User, ScreenerConfig (fees), сигналы, моки
│   ├── ingress/latest.go       # коалесцер тиков (latest-wins) + безопасный shutdown
│   ├── observability/          # Prometheus-метрики, /healthz, /api/status, pprof
│   ├── config/config.go        # парсинг .env (TELEGRAM_TOKEN(S), FEES, METRICS_ADDR, LOG_*)
│   ├── retry/backoff.go        # экспоненциальный backoff с джиттером
│   └── wsutil/heartbeat.go     # WS ping/pong, read-deadline
├── docs/                       # отчёты: 01-fix … 07-audit, 08-improvements-next
├── .github/workflows/ci.yml    # vet/build/test/race + guard .env + артефакт
├── scripts/dev-setup.sh        # идемпотентное dev-окружение (Go, Postgres, .env, гейт)
├── docker-compose.yml          # postgres + app (порт 9090 метрики)
├── Dockerfile                  # multi-stage → scratch
├── schema.sql                  # копия схемы для initdb (каноническая — embedded в postgres)
├── ARCHITECTURE.md             # архитектурные решения
└── README.md                   # быстрый старт, команды, наблюдаемость
```
