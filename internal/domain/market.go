package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

// MarketType описывает тип рынка (Спот или Фьючерс)
type MarketType string

const (
	MarketTypeSpot    MarketType = "SPOT"
	MarketTypeFutures MarketType = "FUTURES"
)

// MarketTick — единый нормализованный тикер цен с биржи
type MarketTick struct {
	Exchange   string          // Название биржи ("BINANCE", "BYBIT")
	Symbol     string          // Торговая пара ("BTCUSDT")
	MarketType MarketType      // SPOT или FUTURES
	BestBid    decimal.Decimal // Лучшая цена покупки (по ней продаем)
	BestAsk    decimal.Decimal // Лучшая цена продажи (по ней покупаем)
	Volume24h  decimal.Decimal // Суточный объем с биржи в USD (для отсева неликвида)
	Timestamp  time.Time       // Время получения события
}
