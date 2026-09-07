# Crypto Arbitrage Screener

Backend скринер арбитражных возможностей на Go.

## Что делает

- получает BBO/24h volume через WebSocket adapters;
- нормализует данные разных бирж в единый `MarketTick`;
- ищет futures/futures между разными биржами;
- ищет spot/futures внутри одной биржи;
- вычитает taker-комиссии бирж из спреда — все сигналы показывают чистый (net) спред;
- защищается от stale/out-of-order quotes;
- отслеживает OPEN -> peak -> CLOSE;
- сохраняет lifecycle сигналов в PostgreSQL;
- отправляет персональные Telegram alerts по пользовательским spread/volume filters;
- поддерживает hot add/remove connectors для администратора;
- отдаёт метрики Prometheus, health-эндпоинты и статусный JSON.

## Поддерживаемые adapters

BINANCE, BINGX, BITGET, BYBIT, GATEIO, KUCOIN, MEXC, OKX.

## Важно

Текущая версия считает rolling 24h quote volume. Настоящие 1m/5m/15m/30m/1h/4h volume filters требуют trade/kline stream и пока не включены.

Funding feed реализован только для Binance. Для остальных бирж funding является optional и не блокирует raw spot/futures detection.

Spread — executable BBO spread за вычетом комиссий, но всё ещё не гарантированный PnL: slippage, depth, latency и position limits не входят в расчёт.

**Гео-блоки:** REST/WebSocket API Binance и Bybit закрывают доступ из ряда датацентровых IP (HTTP 403/451). При старте скринер пингует все 8 бирж и громко логирует гео-блоки — результат также виден в `/api/status` (`exchange_availability`). Если биржа заблокирована, разворачивайся в разрешённом регионе или за прокси.

## Запуск

Требуется Go 1.26.4 и Docker/Compose.

```bash
cp .env.example .env
# заполнить TELEGRAM_TOKEN (или TELEGRAM_TOKENS), ADMIN_CHAT_IDS, DB_* / DATABASE_URL

docker compose up --build
```

### Быстрый старт разработки (Linux, без Docker)

`scripts/dev-setup.sh` идемпотентно разворачивает окружение с нуля: Go 1.26.4,
зависимости, PostgreSQL с базой `screener_db` и схемой, `.env` из шаблона,
затем выполняет проверочный гейт (build / vet / test) и собирает `./screener`.

```bash
./scripts/dev-setup.sh            # всё
./scripts/dev-setup.sh --race     # тесты с детектором гонок
./scripts/dev-setup.sh --no-db    # пропустить установку PostgreSQL
```

Пароль БД, если не задан `DB_PASSWORD`, генерируется случайно и вписывается в
`.env`. Крединалы переопределяются переменными `DB_USER` / `DB_PASSWORD` / `DB_NAME`.
Подробная отчётность по проекту — в каталоге [docs/](docs/).

## Наблюдаемость

Скринер поднимает HTTP-сервер (`METRICS_ADDR`, по умолчанию `:9090`):

| Эндпоинт         | Что отдаёт                                                      |
|------------------|-----------------------------------------------------------------|
| `/metrics`       | Prometheus-метрики: тики по биржам, сигналы open/close, доставка Telegram, ошибки БД, глубина очередей, funding-возраст по биржам |
| `/healthz`       | liveness-проба (JSON)                                           |
| `/api/status`    | uptime, goroutines, очереди, активные сигналы, биржи, доступность бирж на старте |
| `/debug/pprof/`  | профилирование Go (heap, goroutine, cpu и т.д.)                 |

Логи — структурированные `slog`: формат `LOG_FORMAT=text|json`, уровень `LOG_LEVEL=debug|info|warn|error`.

### Гео-блоки бирж и как проект их обходит

| Биржа | Проблема | Решение |
|---|---|---|
| **Binance** | `api.binance.com`/`stream/fstream` — 451 в ряде регионов | Спот-данные идут через официальные зеркала `data-api`/`data-stream.binance.vision` (гео-независимы); фьючерсы — `BINANCE_SPOT_ONLY=1` или прокси-хосты через env `BINANCE_*` |
| **Bybit** | REST гео-блок (CloudFront 403), WS работает | `internal/adapters/bybit/seed.go` (топ-символы по ликвидности) + дисковый кэш `SYMBOLS_CACHE_DIR` + шардирование подписок по 10 символов; лимит числа символов — `BYBIT_SYMBOL_LIMIT` (по умолчанию 100) |
| **MEXC** | WS-блок дата-центровых IP (funding REST работает) | зависит от IP прода |

Команды пользователя: `/start /stop /setcross /setvol /settimeframe /setfundingtime /signals` —
активные и последние закрытые сигналы. Алерты мониторинга (Alertmanager) доставляются
вебхуком в приложение и пересылаются администраторам в Telegram.

### Grafana + Prometheus (панель мониторинга)

В `docker-compose.yml` есть сервисы `prometheus` (порт **9091**) и `grafana` (порт **3000**, вход `admin`, пароль `GRAFANA_ADMIN_PASSWORD`, по умолчанию `admin`; анонимный просмотр разрешён). Конфигурация — в `observability/`:

```
observability/
  prometheus.yml                          # scrape app:9090/metrics каждые 15с + self
  alerts.yml                              # правила: ScreenerDown, ExchangeSilent (>5 мин),
                                          #   FundingStale (>120с), TelegramDropped, DbErrors
  grafana/provisioning/                   # автоподключение datasource и дашбордов
  grafana/dashboards/crypto-screener.json # 13 панелей: тики по биржам, funding age, сигналы,
                                          #   Telegram, очереди, алерты, heap/goroutines
```

Запуск: `docker compose up -d prometheus grafana alertmanager` → дашборд «Crypto Screener — обзор» на http://localhost:3000 (автообновление 30с, история с Prometheus TSDB, retention 15 дней). Алерты (биржа молчит >5 мин, funding >120с, потеря Telegram-уведомлений, ошибки БД, скринер недоступен) доставляются цепочкой Prometheus → **Alertmanager (:9093)** → вебхук `POST /alerts` приложения → Telegram администраторам. UI алертов: http://localhost:9093.

## Команды Telegram

Пользовательские:

- `/start`, `/stop` — подписка/отписка;
- `/setcross <%>` — личный минимальный спред (net);
- `/setvol <USDT>` — минимальный quote turnover;
- `/settimeframe <1m|5m|15m|30m|1h|4h|24h>` — окно volume-фильтра;
- `/setfundingtime <minutes>` — не слать сигнал ближе N минут до funding.

Администраторские (`ADMIN_CHAT_IDS`):

- `/sethardspread <%>` — глобальный floor спреда; персистится в PostgreSQL и переживает рестарты;
- `/setfees [БИРЖА:ДОЛЯ,...]` — taker-комиссии; без аргументов показывает текущие; персистится;
- `/addex <exchange>` / `/rmex <exchange>` — hot add/remove коннекторов;
- `/stats` — статистика сигналов за 24 часа (открыто/закрыто/средний пик/длительность).

Комиссии по умолчанию — консервативные 5 б.п. на сторону (`DEFAULT:0.0005`);
глобально задаются переменной `FEES`, точечно — командой `/setfees`. Из спреда
вычитаются обе стороны сделки до funding-оценки, порогов и фильтров.

## Мульти-ботовый режим

Telegram ограничивает рассылку на одного бота. `TELEGRAM_TOKENS=tok1,tok2,...`
запускает пул ботов: пользователь закрепляется за ботом, через которого
подписался (`users.bot_id`), и все его уведомления идут через него. Команды
каждый бот обрабатывает сам;Broadcast разбивается по ботам.

## Проверка

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

CI (`.github/workflows/ci.yml`) выполняет тот же набор на каждый push/PR: vet,
build, test, test -race, сборка release-бинарника и проверка, что `.env` не
попал в git.
