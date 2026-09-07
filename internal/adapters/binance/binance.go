package binance

import (
	"context"
	"crypto-screener/internal/domain"
	"crypto-screener/internal/ingress"
	"crypto-screener/internal/retry"
	"crypto-screener/internal/wsutil"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	spotWS    = "wss://stream.binance.com:9443/ws/!ticker@arr"
	futuresWS = "wss://fstream.binance.com/ws/!ticker@arr"
	fundingWS = "wss://fstream.binance.com/ws/!markPrice@arr"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 3 * time.Minute
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var (
	dialer     = websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	httpClient = &http.Client{Timeout: 10 * time.Second}
)

type tickerPayload struct {
	Symbol    string `json:"s"`
	EventTime int64  `json:"E"`
	BestBid   string `json:"b"`
	BestAsk   string `json:"a"`
	QVolume   string `json:"q"`
}

type fundingPayload struct {
	Symbol          string `json:"s"`
	EventTime       int64  `json:"E"`
	FundingRate     string `json:"r"`
	NextFundingTime int64  `json:"T"`
}

type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, spotWS, domain.MarketTypeSpot, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, futuresWS, domain.MarketTypeFutures, out)
	return nil
}

func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	a.listenFunding(ctx, sink)
	return nil
}

func (a *Adapter) ConnectCandles(ctx context.Context, sink domain.CandleSink) error {
	for _, spec := range []struct {
		url, rest string
		market    domain.MarketType
	}{
		{"wss://stream.binance.com:9443/stream", "https://api.binance.com/api/v3/exchangeInfo", domain.MarketTypeSpot},
		{"wss://fstream.binance.com/stream", "https://fapi.binance.com/fapi/v1/exchangeInfo", domain.MarketTypeFutures},
	} {
		go a.runCandleFeed(ctx, sink, spec.url, spec.rest, spec.market)
	}
	<-ctx.Done()
	return nil
}

type binanceSpotInfo struct {
	Symbols []struct{ Symbol, Status, QuoteAsset string } `json:"symbols"`
}

type binanceKlineEnvelope struct {
	Data binanceKlinePayload `json:"data"`
}
type binanceKlinePayload struct {
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	K         struct {
		Start  int64  `json:"t"`
		End    int64  `json:"T"`
		Quote  string `json:"q"`
		Closed bool   `json:"x"`
	} `json:"k"`
}

func (a *Adapter) runCandleFeed(ctx context.Context, sink domain.CandleSink, wsURL, restURL string, market domain.MarketType) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		symbols, err := binanceSymbols(ctx, restURL)
		if err != nil {
			log.Printf("⚠️ Binance %s candle symbols: %v", market, err)
			select {
			case <-ctx.Done():
				return
			case <-retry.After(reconnectDelay):
			}
			continue
		}
		var wg sync.WaitGroup
		for i := 0; i < len(symbols); i += 500 {
			end := i + 500
			if end > len(symbols) {
				end = len(symbols)
			}
			streams := make([]string, 0, end-i)
			for _, sym := range symbols[i:end] {
				streams = append(streams, strings.ToLower(sym)+"@kline_1m")
			}
			streamsCopy := append([]string(nil), streams...)
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.runBinanceCandleShard(ctx, sink, wsURL, streamsCopy, market)
			}()
		}
		wg.Wait()
		if ctx.Err() == nil && !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func binanceSymbols(ctx context.Context, endpoint string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("binanceSymbols: %w", err)
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("binanceSymbols: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("binance exchangeInfo HTTP %s", r.Status)
	}
	var x binanceSpotInfo
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return nil, fmt.Errorf("binanceSymbols: %w", err)
	}
	out := make([]string, 0, len(x.Symbols))
	for _, v := range x.Symbols {
		if v.Status == "TRADING" && v.QuoteAsset == "USDT" {
			out = append(out, v.Symbol)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("binance exchangeInfo returned no USDT trading symbols")
	}
	return out, nil
}

func (a *Adapter) runBinanceCandleShard(ctx context.Context, sink domain.CandleSink, wsURL string, streams []string, market domain.MarketType) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		if err := a.readBinanceCandleShard(ctx, sink, wsURL, streams, market); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ Binance %s candle WS: %v", market, err)
		}
		if ctx.Err() == nil && !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) readBinanceCandleShard(ctx context.Context, sink domain.CandleSink, wsURL string, streams []string, market domain.MarketType) error {
	u := wsURL + "?streams=" + strings.Join(streams, "/")
	conn, _, err := dialer.DialContext(ctx, u, nil)
	if err != nil {
		return fmt.Errorf("readBinanceCandleShard: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := wsutil.StartHeartbeat(connCtx, conn, 20*time.Second, 60*time.Second); err != nil {
		return fmt.Errorf("start websocket heartbeat: %w", err)
	}
	go closeOnCtx(connCtx, conn)
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("readBinanceCandleShard: %w", err)
		}
		if err := wsutil.TouchReadDeadline(conn, 60*time.Second); err != nil {
			return fmt.Errorf("refresh websocket read deadline: %w", err)
		}
		var e binanceKlineEnvelope
		if sonic.Unmarshal(msg, &e) != nil || e.Data.Symbol == "" {
			continue
		}
		q, err := decimal.NewFromString(e.Data.K.Quote)
		if err != nil || q.IsNegative() {
			continue
		}
		et := time.UnixMilli(e.Data.EventTime)
		if e.Data.EventTime == 0 {
			et = time.Now()
		}
		if err := sink.UpdateCandle(domain.MarketCandle{Exchange: "BINANCE", Symbol: e.Data.Symbol, MarketType: market, OpenTime: time.UnixMilli(e.Data.K.Start), CloseTime: time.UnixMilli(e.Data.K.End), QuoteVolume: q, EventTime: et, Closed: e.Data.K.Closed}); err != nil {
			return fmt.Errorf("readBinanceCandleShard: %w", err)
		}
	}
}

// --- Ticker ---

func (a *Adapter) listen(
	ctx context.Context,
	url string,
	mType domain.MarketType,
	out chan<- domain.MarketTick,
) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for {
		startedAt := time.Now()
		if ctx.Err() != nil {
			return
		}
		if err := a.connectAndRead(ctx, url, mType, out); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  Binance %s WS: %v — reconnecting in %s", mType, err, reconnectDelay)
		}
		if time.Since(startedAt) >= 30*time.Second {
			backoff.Reset()
		}
		if !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) connectAndRead(
	ctx context.Context,
	url string,
	mType domain.MarketType,
	out chan<- domain.MarketTick,
) error {

	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	defer conn.Close()

	log.Printf("✅ Binance %s connected", mType)

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	conn.SetPongHandler(func(string) error {
		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("set pong read deadline: %w", err)
		}
		return nil
	})

	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	go keepAlive(connCtx, conn, string(mType))

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}

		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("refresh Binance %s read deadline: %w", mType, err)
		}

		var payloads []tickerPayload
		if err := sonic.Unmarshal(msg, &payloads); err != nil {
			continue
		}

		now := time.Now()
		for i := range payloads {
			eventTime := now
			if payloads[i].EventTime > 0 {
				eventTime = time.UnixMilli(payloads[i].EventTime)
			}
			tick, ok := toMarketTick(&payloads[i], mType, eventTime)
			if !ok {
				continue
			}
			ingress.Submit(out, tick)
		}
	}
}

func toMarketTick(p *tickerPayload, mType domain.MarketType, ts time.Time) (domain.MarketTick, bool) {
	if !strings.HasSuffix(strings.ToUpper(p.Symbol), "USDT") {
		return domain.MarketTick{}, false
	}
	bid, err := decimal.NewFromString(p.BestBid)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(p.BestAsk)
	if err != nil || ask.IsZero() || bid.GreaterThan(ask) {
		return domain.MarketTick{}, false
	}

	qVol, err := decimal.NewFromString(p.QVolume)
	if err != nil {
		return domain.MarketTick{}, false
	}

	return domain.MarketTick{
		Exchange:    "BINANCE",
		Symbol:      p.Symbol,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		EventTime:   ts,
		ReceivedAt:  time.Now(),
		Timestamp:   ts,
	}, true
}

// --- Funding ---

func (a *Adapter) listenFunding(ctx context.Context, sink domain.FundingSink) {
	sink.SetStreamHealth("BINANCE", false)
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for {
		if ctx.Err() != nil {
			return
		}
		if err := a.connectAndReadFunding(ctx, sink); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  Binance Funding WS: %v — reconnecting in %s", err, reconnectDelay)
		}
		if !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) connectAndReadFunding(ctx context.Context, sink domain.FundingSink) error {
	sink.SetStreamHealth("BINANCE", false)
	conn, _, err := dialer.DialContext(ctx, fundingWS, nil)
	if err != nil {
		return fmt.Errorf("dial funding: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	defer conn.Close()

	log.Printf("✅ Binance Funding connected")

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	conn.SetPongHandler(func(string) error {
		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("set pong read deadline: %w", err)
		}
		return nil
	})

	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	go keepAlive(connCtx, conn, "funding")

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read funding: %w", err)
		}

		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("refresh Binance funding read deadline: %w", err)
		}

		var payloads []fundingPayload
		if err := sonic.Unmarshal(msg, &payloads); err != nil {
			continue
		}

		for _, p := range payloads {
			if !strings.HasSuffix(strings.ToUpper(p.Symbol), "USDT") {
				continue
			}
			rate, err := decimal.NewFromString(p.FundingRate)
			if err != nil {
				log.Printf("⚠️ Binance invalid funding rate for %s: %v", p.Symbol, err)
				continue
			}
			eventTime := time.Time{}
			if p.EventTime > 0 {
				eventTime = time.UnixMilli(p.EventTime)
			}
			if err := sink.UpdateFunding("BINANCE", p.Symbol, rate, time.UnixMilli(p.NextFundingTime), eventTime); err != nil {
				log.Printf("⚠️ Binance funding update: %v", err)
			}
		}
	}
}

// --- Helpers ---

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}

func keepAlive(ctx context.Context, conn *websocket.Conn, label string) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := conn.WriteControl(
				websocket.PingMessage,
				nil,
				time.Now().Add(5*time.Second),
			); err != nil {
				wrapped := fmt.Errorf("Binance %s keepalive: %w", label, err)
				log.Printf("⚠️  %v", wrapped)
				return
			}
		}
	}
}
