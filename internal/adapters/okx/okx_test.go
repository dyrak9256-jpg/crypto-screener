package okx

import (
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"github.com/bytedance/sonic"
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

func TestOKX_FundingPayload_Unmarshal(t *testing.T) {
	// Регрессия: fundingRate и nextFundingTime приходят строками в JSON,
	// оба поля критичны для funding-оценки. Когда-то nextFundingTime парсился
	// как число и молча терялся — тест фиксирует строковый формат.
	payload := `{"arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","fundingRate":"0.000125","nextFundingTime":"1736410500000","ts":"1736410000000"}]}`
	var resp wsResponse
	require.NoError(t, sonic.Unmarshal([]byte(payload), &resp))
	require.Len(t, resp.Data, 1)
	d := resp.Data[0]
	require.Equal(t, "BTC-USDT-SWAP", d.InstID)
	require.Equal(t, "0.000125", d.FundingRate)
	require.Equal(t, int64(1736410500000), d.NextFundingTime)
	require.Equal(t, int64(1736410000000), d.Ts)

	rate, err := decimal.NewFromString(d.FundingRate)
	require.NoError(t, err)
	require.True(t, rate.Equal(decimal.RequireFromString("0.000125")))
	require.Equal(t, int64(1736410500), time.UnixMilli(d.NextFundingTime).Unix())
}

func TestOKX_ToMarketTick_RejectsInvalidQuotes(t *testing.T) {
	now := time.Now()
	// Перекрёстный рынок (bid > ask) — не исполняемая котировка.
	_, ok := toMarketTick(&tickerData{InstID: "BTC-USDT-SWAP", LastPx: "100", BidPx: "102", AskPx: "101", VolCcy24h: "10"}, domain.MarketTypeFutures, now)
	require.False(t, ok)
	// Нулевой bid.
	_, ok = toMarketTick(&tickerData{InstID: "BTC-USDT-SWAP", LastPx: "100", BidPx: "0", AskPx: "101", VolCcy24h: "10"}, domain.MarketTypeFutures, now)
	require.False(t, ok)
	// Не-USDT perpetual.
	_, ok = toMarketTick(&tickerData{InstID: "BTC-USD-SWAP", LastPx: "100", BidPx: "99", AskPx: "101", VolCcy24h: "10"}, domain.MarketTypeFutures, now)
	require.False(t, ok)
	// Спот без пары к USDT.
	_, ok = toMarketTick(&tickerData{InstID: "BTC-ETH", BidPx: "99", AskPx: "101", VolCcy24h: "10"}, domain.MarketTypeSpot, now)
	require.False(t, ok)
}
