package main

import (
	"fmt"
	"time"

	"crypto-screener/internal/domain"

	"github.com/shopspring/decimal"
)

func main() {
	fmt.Println("🚀 Инициализация скринера...")

	tick := domain.MarketTick{
		Exchange:   "BINANCE",
		Symbol:     "BTCUSDT",
		MarketType: domain.MarketTypeSpot,
		BestBid:    decimal.NewFromFloat(65000.50),
		BestAsk:    decimal.NewFromFloat(65001.00),
		Volume24h:  decimal.NewFromFloat(50000000.00),
		Timestamp:  time.Now(),
	}

	fmt.Printf("Тикер: %s %s [%s] | Bid: %s | Ask: %s | Vol24h: $%s\n",
		tick.Exchange, tick.Symbol, tick.MarketType, tick.BestBid, tick.BestAsk, tick.Volume24h)
}
