package binance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"crypto-screener/internal/domain"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func TestBinanceAdapter_ConnectAndRead_TickerStream(t *testing.T) {
	t.Parallel()

	// Setup local WebSocket test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// 1. Send valid ticker payload
		validMsg := tickerPayload{
			Symbol:  "BTCUSDT",
			BestBid: "65000.50",
			BestAsk: "65001.50",
			QVolume: "1234567.89",
		}
		bytes, _ := sonic.Marshal(validMsg)
		_ = conn.WriteMessage(websocket.TextMessage, bytes)

		// 2. Send ticker payload with zero bid (should be filtered out)
		zeroBidMsg := tickerPayload{
			Symbol:  "ETHUSDT",
			BestBid: "0",
			BestAsk: "3500.00",
			QVolume: "500000",
		}
		bytes, _ = sonic.Marshal(zeroBidMsg)
		_ = conn.WriteMessage(websocket.TextMessage, bytes)

		// 3. Send malformed JSON message (should be gracefully ignored)
		_ = conn.WriteMessage(websocket.TextMessage, []byte("{invalid-json}"))

		// Keep connection alive until closed by client
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	adapter := NewAdapter()
	require.NotNil(t, adapter)

	outChan := make(chan domain.MarketTick, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = adapter.connectAndRead(ctx, wsURL, domain.MarketTypeSpot, outChan)
	}()

	// Wait to receive the valid tick
	select {
	case tick := <-outChan:
		assert.Equal(t, "BINANCE", tick.Exchange)
		assert.Equal(t, "BTCUSDT", tick.Symbol)
		assert.Equal(t, domain.MarketTypeSpot, tick.MarketType)
		assert.True(t, tick.BestBid.Equal(decimal.RequireFromString("65000.50")))
		assert.True(t, tick.BestAsk.Equal(decimal.RequireFromString("65001.50")))
		assert.True(t, tick.QuoteVolume.Equal(decimal.RequireFromString("1234567.89")))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for market tick from test server")
	}

	// Verify no second tick was sent for the zero bid payload
	select {
	case unexpected := <-outChan:
		t.Fatalf("unexpected tick received: %+v", unexpected)
	case <-time.After(100 * time.Millisecond):
		// Expected: zero bid was skipped
	}

	// Cancel context to cleanly shut down connection
	cancel()
	wg.Wait()
}

type testFundingSink struct {
	mu      sync.Mutex
	updates map[string]decimal.Decimal
}

func (s *testFundingSink) UpdateFunding(exchange, symbol string, rate decimal.Decimal, nextTime, eventTime time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates[symbol] = rate
	return nil
}
func (s *testFundingSink) SetStreamHealth(string, bool) {}

func TestBinanceAdapter_FundingPayload_UnmarshalAndDispatch(t *testing.T) {
	t.Parallel()

	targetTime := time.Date(2026, 9, 5, 16, 0, 0, 0, time.UTC)
	rawJSON := `[
		{"s":"BTCUSDT","r":"0.00010000","T":1788624000000},
		{"s":"ETHUSDT","r":"-0.00025000","T":1788624000000}
	]`

	var payloads []fundingPayload
	err := sonic.Unmarshal([]byte(rawJSON), &payloads)
	require.NoError(t, err)
	require.Len(t, payloads, 2)

	sink := &testFundingSink{updates: make(map[string]decimal.Decimal)}

	for _, p := range payloads {
		rate, err := decimal.NewFromString(p.FundingRate)
		require.NoError(t, err)
		nextTime := time.UnixMilli(p.NextFundingTime)
		require.NoError(t, sink.UpdateFunding("BINANCE", p.Symbol, rate, nextTime, time.Time{}))
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	assert.True(t, sink.updates["BTCUSDT"].Equal(decimal.RequireFromString("0.0001")))
	assert.True(t, sink.updates["ETHUSDT"].Equal(decimal.RequireFromString("-0.00025")))
	assert.Equal(t, targetTime.UnixMilli(), int64(1788624000000))
}

func TestBinanceAdapter_PublicConnectMethods(t *testing.T) {
	t.Parallel()

	adapter := NewAdapter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Immediately cancelled to prevent actual network dialing

	tickChan := make(chan domain.MarketTick, 1)
	sink := &testFundingSink{updates: make(map[string]decimal.Decimal)}

	assert.NoError(t, adapter.ConnectSpot(ctx, tickChan))
	assert.NoError(t, adapter.ConnectFutures(ctx, tickChan))
	assert.NoError(t, adapter.ConnectFunding(ctx, sink))
}

func TestBinanceAdapter_Listen_ContextCancelled(t *testing.T) {
	t.Parallel()

	adapter := NewAdapter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Pre-cancelled context

	outChan := make(chan domain.MarketTick, 1)
	adapter.listen(ctx, "ws://localhost:9999", domain.MarketTypeSpot, outChan)

	sink := &testFundingSink{updates: make(map[string]decimal.Decimal)}
	adapter.listenFunding(ctx, sink)
}

func TestBinanceAdapter_ConnectAndRead_DialError(t *testing.T) {
	t.Parallel()

	adapter := NewAdapter()
	ctx := context.Background()
	outChan := make(chan domain.MarketTick, 1)

	// Invalid port to trigger dial error
	err := adapter.connectAndRead(ctx, "ws://127.0.0.1:1", domain.MarketTypeSpot, outChan)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dial error")
}

func TestBinanceAdapter_Listen_ReconnectionContextDone(t *testing.T) {
	t.Parallel()

	adapter := NewAdapter()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	outChan := make(chan domain.MarketTick, 1)
	// Invalid URL triggers reconnect branch, which exits when ctx times out after 50ms
	adapter.listen(ctx, "ws://127.0.0.1:1", domain.MarketTypeFutures, outChan)
}

func TestBinanceAdapter_ConnectAndRead_ChannelFullDrop(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		msg := tickerPayload{
			Symbol:  "BTCUSDT",
			BestBid: "65000",
			BestAsk: "65001",
			QVolume: "100",
		}
		bytes, _ := sonic.Marshal(msg)
		_ = conn.WriteMessage(websocket.TextMessage, bytes)

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	adapter := NewAdapter()

	// Fill channel to capacity 1
	fullChan := make(chan domain.MarketTick, 1)
	fullChan <- domain.MarketTick{Symbol: "PRE_FILLED"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = adapter.connectAndRead(ctx, wsURL, domain.MarketTypeSpot, fullChan)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	// The pre-filled tick is still there, meaning the new tick was dropped in default branch
	assert.Len(t, fullChan, 1)
	firstTick := <-fullChan
	assert.Equal(t, "PRE_FILLED", firstTick.Symbol)
}
