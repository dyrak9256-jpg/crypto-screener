package okx

import (
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestToMarketTick_ConvertsDerivativeBaseVolumeToQuoteVolume(t *testing.T) {
	tick, ok := toMarketTick(&tickerData{
		InstID:    "BTC-USDT-SWAP",
		LastPx:    "50000",
		BidPx:     "49999",
		AskPx:     "50001",
		VolCcy24h: "2",
		Ts:        time.Now().UnixMilli(),
	}, domain.MarketTypeFutures, time.Now())
	require.True(t, ok)
	require.True(t, tick.QuoteVolume.Equal(decimal.NewFromInt(100000)))
}
