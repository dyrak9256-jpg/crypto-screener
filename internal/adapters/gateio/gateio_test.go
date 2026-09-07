package gateio

import (
	"context"
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
func TestGateio_ConnectMethods_ReturnPromptlyOnCanceledContext(t *testing.T) {
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

func TestGateio_ImplementsExchangeConnector(t *testing.T) {
	var _ domain.ExchangeConnector = NewAdapter()
	assert.NotNil(t, NewAdapter())
}

func TestGateio_TickerToTick(t *testing.T) {
	ts := time.Now()
	// Спот.
	mt, ok := spotTickerToTick(&tickerData{CurrencyPair: "BTC_USDT", HighestBid: "100", LowestAsk: "101", QuoteVolume: "3000"}, ts)
	require.True(t, ok)
	require.Equal(t, "GATEIO", mt.Exchange)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	require.True(t, mt.QuoteVolume.Equal(decimal.NewFromInt(3000)))
	// Фьючерс BBO из futures.book_ticker: BTC_USDT → BTCUSDT.
	// Объём приходит из futures.tickers и дозаполняется вызывающим кодом —
	// здесь проверяем конвертацию BBO (объём по умолчанию нулевой).
	mt, ok = bookTickerToTick(&bookTickerData{Contract: "BTC_USDT", Bid: "100", Ask: "101"}, ts)
	require.True(t, ok)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	require.True(t, mt.BestBid.Equal(decimal.NewFromInt(100)))
	require.True(t, mt.BestAsk.Equal(decimal.NewFromInt(101)))
	require.True(t, mt.QuoteVolume.IsZero())
	// Нулевой bid и перекрёстный рынок отбраковываются на обоих рынках.
	_, ok = spotTickerToTick(&tickerData{CurrencyPair: "BTC_USDT", HighestBid: "0", LowestAsk: "101", QuoteVolume: "1"}, ts)
	require.False(t, ok)
	_, ok = bookTickerToTick(&bookTickerData{Contract: "BTC_USDT", Bid: "102", Ask: "101"}, ts)
	require.False(t, ok)
}

// Живые форматы Gate.io WS (зафиксированы зондами 08.09.2026): спот-апдейт —
// одиночный объект; futures.book_ticker — объект с b/a; futures.tickers —
// массив с объёмом. Регрессия на смену форматов биржей.
func TestGateio_WsUpdateFormats(t *testing.T) {
	// Спот: update c result-объектом.
	spotUpdate := `{"time":1,"time_ms":2,"channel":"spot.tickers","event":"update","result":{"currency_pair":"BTC_USDT","last":"100","lowest_ask":"101","highest_bid":"99.5","quote_volume":"3000"}}`
	var env gateWsEnvelope
	require.NoError(t, sonic.Unmarshal([]byte(spotUpdate), &env))
	require.Equal(t, "update", env.Event)
	var st tickerData
	require.NoError(t, sonic.Unmarshal(env.Result, &st))
	require.Equal(t, "BTC_USDT", st.CurrencyPair)
	require.Equal(t, "99.5", st.HighestBid)
	require.Equal(t, "3000", st.QuoteVolume)

	// Futures book_ticker: result-объект {s,b,a}.
	futBBO := `{"time":1,"time_ms":2,"channel":"futures.book_ticker","event":"update","result":{"t":3,"u":4,"s":"BTC_USDT","b":"79176.3","B":625,"a":"79176.4","A":17623}}`
	require.NoError(t, sonic.Unmarshal([]byte(futBBO), &env))
	require.Equal(t, "futures.book_ticker", env.Channel)
	var b bookTickerData
	require.NoError(t, sonic.Unmarshal(env.Result, &b))
	require.Equal(t, "BTC_USDT", b.Contract)
	require.Equal(t, "79176.3", b.Bid)

	// Futures tickers: result-массив с объёмом.
	futVol := `{"time":1,"time_ms":2,"channel":"futures.tickers","event":"update","result":[{"contract":"BTC_USDT","last":"79080","volume_24h_quote":"412432155"}]}`
	require.NoError(t, sonic.Unmarshal([]byte(futVol), &env))
	require.Equal(t, "futures.tickers", env.Channel)
	var rows []futuresTickerData
	require.NoError(t, sonic.Unmarshal(env.Result, &rows))
	require.Len(t, rows, 1)
	require.Equal(t, "412432155", rows[0].Volume)

	// Ack подписки: result-объект со status.
	ack := `{"time":1,"time_ms":2,"channel":"spot.tickers","event":"subscribe","result":{"status":"success"}}`
	require.NoError(t, sonic.Unmarshal([]byte(ack), &env))
	var g gateAck
	require.NoError(t, sonic.Unmarshal(env.Result, &g))
	require.Equal(t, "success", g.Status)
	require.Nil(t, g.Error)
}
