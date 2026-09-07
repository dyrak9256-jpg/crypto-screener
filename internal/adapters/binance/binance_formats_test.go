package binance

import (
	"testing"
	"time"

	"crypto-screener/internal/domain"

	"github.com/bytedance/sonic"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

// Живые форматы Binance-потоков (зонды 08.09.2026, зеркало
// data-stream.binance.vision). Регрессия на кейс-инсенситивный матчинг sonic:
// "e" обязана падать в строковое поле (иначе Unmarkor падает), "B"/"A" —
// размеры заявок, не цены.
func TestBinance_WsUpdateFormats(t *testing.T) {
	// Комбинированный спот-поток @ticker.
	raw := `{"stream":"solusdt@ticker","data":{"e":"24hrTicker","E":1788808833795,"s":"SOLUSDT","p":"-1.69","c":"103.89","Q":"48.15","b":"103.88","B":"531.455","a":"103.89","A":"187.872","q":"236357090.5"}}`
	var env struct {
		Stream string        `json:"stream"`
		Data   tickerPayload `json:"data"`
	}
	require.NoError(t, sonic.Unmarshal([]byte(raw), &env))
	require.Equal(t, "solusdt@ticker", env.Stream)
	require.Equal(t, "SOLUSDT", env.Data.Symbol)
	require.Equal(t, "103.88", env.Data.BestBid, "b — цена bid, не размер B")
	require.Equal(t, "103.89", env.Data.BestAsk, "a — цена ask, не размер A")
	require.Equal(t, "236357090.5", env.Data.QVolume)
	require.EqualValues(t, 1788808833795, env.Data.EventTime)

	// Фьючерсный !ticker@arr (массив).
	arr := `[{"e":"24hrTicker","E":1788808833795,"s":"BTCUSDT","b":"79158.01","B":"4.85","a":"79158.02","A":"1.21","q":"1029317.5"}]`
	var payloads []tickerPayload
	require.NoError(t, sonic.Unmarshal([]byte(arr), &payloads))
	require.Len(t, payloads, 1)
	require.Equal(t, "79158.01", payloads[0].BestBid)

	// Funding: !markPrice@arr.
	mark := `[{"e":"markPriceUpdate","E":1788808833795,"s":"BTCUSDT","p":"79158.5","i":"79158.6","P":"79158.7","r":"0.00010000","T":1788825600000}]`
	var fp []fundingPayload
	require.NoError(t, sonic.Unmarshal([]byte(mark), &fp))
	require.Len(t, fp, 1)
	require.Equal(t, "0.00010000", fp[0].FundingRate)
	require.EqualValues(t, 1788825600000, fp[0].NextFundingTime)
}

func TestBinance_ToMarketTickUsesPricesNotSizes(t *testing.T) {
	p := &tickerPayload{Symbol: "BTCUSDT", BestBid: "79158.01", BestAsk: "79158.02", QVolume: "1000000", BidQty: "4.85", AskQty: "1.21"}
	tick, ok := toMarketTick(p, domain.MarketTypeSpot, time.UnixMilli(1788808833795))
	require.True(t, ok)
	require.True(t, tick.BestBid.Equal(decimal.RequireFromString("79158.01")))
	require.True(t, tick.BestAsk.Equal(decimal.RequireFromString("79158.02")))
	require.True(t, tick.QuoteVolume.Equal(decimal.NewFromInt(1000000)))
}
