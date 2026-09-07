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

Объёмные фильтры 1m/5m/15m/30m/1h/4h строятся локально из 1-минутных candle streams. При холодном старте допускается консервативная проекция по непрерывной серии текущих минут; пропуски не считаются нулевым объёмом. 24h использует rolling quote volume из ticker.

Funding feed реализован для всех восьми поддерживаемых бирж. Funding проверяется fail-closed: отсутствующий, устаревший или unhealthy funding stream не позволяет сформировать funding-adjusted сигнал.

Spread — это executable BBO spread, а не гарантированный PnL: комиссии, slippage, depth, latency и position limits пока не входят в расчёт.

## Запуск

Требуется Go 1.26.4 и Docker/Compose.

```bash
cp .env.example .env
# заполнить TELEGRAM_TOKEN, ADMIN_CHAT_IDS, DB_* / DATABASE_URL

docker compose up --build
```

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
