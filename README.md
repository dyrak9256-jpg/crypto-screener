# Crypto Arbitrage Screener

Backend скринер арбитражных возможностей на Go.

## Что делает

- получает BBO/24h volume через WebSocket adapters;
- нормализует данные разных бирж в единый `MarketTick`;
- ищет futures/futures между разными биржами;
- ищет spot/futures внутри одной биржи;
- защищается от stale/out-of-order quotes;
- отслеживает OPEN -> peak -> CLOSE;
- сохраняет lifecycle сигналов в PostgreSQL;
- отправляет персональные Telegram alerts по пользовательским spread/volume filters;
- поддерживает hot add/remove connectors для администратора.

## Поддерживаемые adapters

BINANCE, BINGX, BITGET, BYBIT, GATEIO, KUCOIN, MEXC, OKX.

## Важно

Текущая версия считает rolling 24h quote volume. Настоящие 1m/5m/15m/30m/1h/4h volume filters требуют trade/kline stream и пока не включены.

Funding feed реализован только для Binance. Для остальных бирж funding является optional и не блокирует raw spot/futures detection.

Spread — это executable BBO spread, а не гарантированный PnL: комиссии, slippage, depth, latency и position limits пока не входят в расчёт.

## Запуск

Требуется Go 1.26.4 и Docker/Compose.

```bash
cp .env.example .env
# заполнить TELEGRAM_TOKEN, ADMIN_CHAT_IDS, DB_* / DATABASE_URL

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

Крединалы БД переопределяются переменными `DB_USER` / `DB_PASSWORD` / `DB_NAME`.
Подробная отчётность по проекту — в каталоге [docs/](docs/).

## Проверка

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

Полный test/race/build gate должен выполняться на машине с Go 1.26.4 и доступом к зависимостям.

## Volume commands

`/setvol <USDT>` — minimum quote turnover.
`/settimeframe <1m|5m|15m|30m|1h|4h|24h>` — interval for the volume filter.
`/setfundingtime <minutes>` — do not send a notification when the nearest funding is closer than this threshold.

The global spread floor is configured with `HARD_MIN_SPREAD` (minimum 1%). Administrators can also change the runtime value with `/sethardspread <percent>`; this runtime change is not persisted to PostgreSQL yet and will revert to the environment value after restart.
