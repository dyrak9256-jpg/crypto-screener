package bingx

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"crypto-screener/internal/domain"

	"github.com/bytedance/sonic"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Connect* методы после H1-фикса БЛОКИРУЮТСЯ на время жизни соединения.
// Предотменённый контекст должен заставить их вернуться сразу (без сети и без зависания).
func TestBingx_ConnectMethods_ReturnPromptlyOnCanceledContext(t *testing.T) {
	adapter := NewAdapter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменён до вызова => процессы должны сразу выйти

	tickChan := make(chan domain.MarketTick, 1)

	done := make(chan error, 3)
	go func() { done <- adapter.ConnectSpot(ctx, tickChan) }()
	go func() { done <- adapter.ConnectFutures(ctx, tickChan) }()
	go func() { done <- adapter.ConnectFunding(ctx, nil) }()

	for i := 0; i < 3; i++ {
		select {
		case err := <-done:
			_ = err // prompt return is the contract; a cancelled-ctx dial error is expected
		case <-time.After(2 * time.Second):
			t.Fatalf("Connect* did not return promptly on cancelled context")
		}
	}
}

func TestBingx_ImplementsExchangeConnector(t *testing.T) {
	var _ domain.ExchangeConnector = NewAdapter()
	assert.NotNil(t, NewAdapter())
}

func TestBingx_BboToTick(t *testing.T) {
	ts := time.Now()
	// Спот и фьючерсы используют один формат @bookTicker.
	mt, ok := bboToTick(&bingxBookTicker{Symbol: "BTC-USDT", Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(101)}, domain.MarketTypeSpot, ts, decimal.NewFromInt(5000))
	require.True(t, ok)
	require.Equal(t, "BINGX", mt.Exchange)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	require.True(t, mt.QuoteVolume.Equal(decimal.NewFromInt(5000)))
	// Объём из @ticker может ещё не прийти — BBO валиден и с нулём.
	mt, ok = bboToTick(&bingxBookTicker{Symbol: "BTC-USDT", Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(101)}, domain.MarketTypeFutures, ts, decimal.Zero)
	require.True(t, ok)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	// Не-USDT символ отбрасывается.
	_, ok = bboToTick(&bingxBookTicker{Symbol: "BTC-ETH", Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(101)}, domain.MarketTypeSpot, ts, decimal.Zero)
	require.False(t, ok)
	// Перекрёстный рынок.
	_, ok = bboToTick(&bingxBookTicker{Symbol: "BTC-USDT", Bid: decimal.NewFromInt(102), Ask: decimal.NewFromInt(101)}, domain.MarketTypeSpot, ts, decimal.Zero)
	require.False(t, ok)
}

// Живые форматы BingX WS (зонды 08.09.2026): подписка по одному символу;
// @bookTicker — BBO, @ticker — 24hTicker с q. У свопа значения строками,
// у спота встречаются числа — decimal парсит оба варианта.
func TestBingx_WsUpdateFormats(t *testing.T) {
	futBBO := `{"id":"1","code":0,"dataType":"BTC-USDT@bookTicker","data":{"e":"bookTicker","E":1757337600000,"s":"BTC-USDT","b":"79138.1","B":"34.8052","a":"79138.2","A":"42.3995"}}`
	var raw struct {
		DataType string          `json:"dataType"`
		Data     json.RawMessage `json:"data"`
	}
	require.NoError(t, sonic.Unmarshal([]byte(futBBO), &raw))
	require.Equal(t, "BTC-USDT@bookTicker", raw.DataType)
	var b bingxBookTicker
	require.NoError(t, sonic.Unmarshal(raw.Data, &b))
	require.Equal(t, "BTC-USDT", b.Symbol)
	require.True(t, b.Bid.Equal(decimal.RequireFromString("79138.1")))
	require.True(t, b.Ask.Equal(decimal.RequireFromString("79138.2")))

	spotTicker := `{"dataType":"ETH-USDT@ticker","data":{"e":"24hTicker","E":1757337600000,"s":"ETH-USDT","p":-0.01,"c":3000.5,"q":123456.78}}`
	require.NoError(t, sonic.Unmarshal([]byte(spotTicker), &raw))
	require.Equal(t, "ETH-USDT@ticker", raw.DataType)
	var d bingxDayTicker
	require.NoError(t, sonic.Unmarshal(raw.Data, &d))
	require.Equal(t, "ETH-USDT", d.Symbol)
	require.True(t, d.QuoteVolume.Equal(decimal.RequireFromString("123456.78")))
}
