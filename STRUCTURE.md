# 📂 Карта кодовой базы

Полная карта проекта: где живёт каждый компонент, его ответственность и как они взаимодействуют.

## Дерево каталогов
```
crypto-screener/
├── cmd/screener/main.go                    # Точка входа: конфиг, БД, бот, запуск всех бирж
├── internal/
│   ├── adapters/
│   │   ├── registry.go                     # Фабрика биржевых коннекторов (Supported/NewByName)
│   │   ├── binance/binance.go              # Binance (Spot/Futures/Funding)
│   │   ├── bingx/bingx.go                  # BingX
│   │   ├── bitget/bitget.go                # Bitget
│   │   ├── bybit/bybit.go                  # Bybit
│   │   ├── gateio/gateio.go                # Gate.io
│   │   ├── kucoin/kucoin.go                # KuCoin
│   │   ├── mexc/mexc.go                    # MEXC
│   │   ├── okx/okx.go                      # OKX
│   │   ├── postgres/repo.go                # Репозиторий (signals, users)
│   │   └── telegram/bot.go                 # Telegram-бот (polling + рассылка)
│   ├── app/
│   │   ├── app.go                          # Оркестрация, команды бота, graceful shutdown
│   │   ├── connector_manager.go            # Hot-swap + reconnect коннекторов бирж
│   │   ├── funding_manager.go              # Ставки финансирования и фильтр прибыльности
│   │   ├── notification_router.go          # Группировка пользователей по таймфреймам
│   │   ├── persistence_worker.go           # Асинхронная запись сигналов в БД
│   │   ├── sharded_aggregator.go           # 1024 шарда, Min/Max спред, объёмы по таймфреймам
│   │   └── tracker.go                      # Жизненный цикл сигнала (open/update/close)
│   ├── config/config.go                    # Конфигурация из переменных окружения
│   └── domain/
│       ├── config.go                       # ScreenerConfig (пороги, funding buffer)
│       ├── market.go                       # MarketTick, MarketType, FundingRate
│       ├── ports.go                        # Порты (интерфейсы) гексагональной архитектуры
│       ├── signal.go                       # SpreadEvent, ArbitrageSignal
│       ├── user.go                         # User + потокобезопасный UserManager
│       └── mocks/                          # Сгенерированные gomock-моки
├── Dockerfile                              # Многоэтапная сборка (golang:1.26-alpine → scratch)
├── docker-compose.yml                      # Телегами-compose: postgres + app
├── schema.sql                              # Схемы БД (signals, users)
├── go.mod / go.sum
├── ARCHITECTURE.md                         # Описание архитектуры и оптимизаций
├── README.md                               # Описание проекта, установка, команды
└── STRUCTURE.md                            # Этот файл
```

## Поток данных
1. **Ingestion:** каждый адаптер биржи через WebSocket (`internal/adapters/*`) открывает **одно** соединение, парсит тики и шлёт `MarketTick` в `tickChan`; переподключение выполняет только `ConnectorManager.runWithReconnect`.
2. **Обработка:** пул ingestion-воркеров (`app.go`) → `ShardedAggregator.ProcessTick` (валидация bid/ask + отсев устаревших).
3. **Математика:** агрегатор обновляет состояние шарда, считает исполняемый спред по bid/ask (cross/intra), проверяет funding по вовлечённым биржам (`FundingManager`) и шлёт `SpreadEvent` в `trackerChan`.
4. **Трекинг:** `Tracker` открывает/обновляет/закрывает сигнал и шлёт его в `dbChan` (блокирующе, без потерь) и `NotificationRouter` (отслеживаемая горутина).
5. **Маршрутизация и сохранение:** `NotificationRouter` фильтрует по настройкам пользователей → Telegram; `PersistenceWorker` пишет сигналы в PostgreSQL.

## Горячая смена коннекторов
`/addex`/`/rmex` (только для админов) через `ConnectorManager` добавляет/удаляет биржу на лету, останавливая старый коннектор и запуская новый без перезапуска приложения.

## Завершение работы
`Application.Run` при отмене контекста выполняет детерминированную цепочку: останавливает приём тиков (`ingestWg`) → закрывает `trackerChan` и ждёт трекер → ждёт горутины уведомлений (`routerWg`) → закрывает `dbChan` → дожидается drain в БД (`persistWg`). Затем `ConnectorManager.StopAll` завершает все биржевые соединения, а `tgBot.Close()` корректно останавливает Telegram-бота (без `send on closed channel`).
