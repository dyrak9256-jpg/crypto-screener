package mexc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crypto-screener/internal/domain"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	spotWS    = "wss://wbs.mexc.com/ws"
	futuresWS = "wss://contract.mexc.com/edge"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 15 * time.Second
	pongWait         = 20 * time.Second
	reconnectDelay   = 3 * time.Second
)

var (
	dialer         = websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	spotPingMsg    = []byte(`{"method":"PING"}`)
	futuresPingMsg = []byte(`{"method":"ping"}`)
	futuresPongMsg = []byte(`{"method":"pong"}`)
)

type subscribeMsg struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
}

type spotTickerData struct {
	Symbol  string `json:"s"`
	Bid     string `json:"b"`
	Ask     string `json:"a"`
	QVolume string `json:"qv"`
}

type futuresResponse struct {
	Channel string            `json:"channel"`
	Data    futuresTickerData `json:"data"`
}

// futuresTickerData: bid1/ask1 приходят как float64
// Используем float64 + конвертацию через strconv для точности
type futuresTickerData struct {
	Symbol string          `json:"symbol"`
	Bid1   json.RawMessage `json:"bid1"`
	Ask1   json.RawMessage `json:"ask1"`
	Volume json.RawMessage `json:"volume24"`
}

type Adapter struct {
	droppedTicks atomic.Uint64
	writeMu      sync.Mutex
}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listenSpot(ctx, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listenFutures(ctx, out)
	return nil
}

func (a *Adapter) listenSpot(ctx context.Context, out chan<- domain.MarketTick) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := a.connectSpot(ctx, out); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  MEXC Spot WS: %v — reconnecting in %s", err, reconnectDelay)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectDelay):
		}
	}
}

func (a *Adapter) listenFutures(ctx context.Context, out chan<- domain.MarketTick) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := a.connectFutures(ctx, out); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  MEXC Futures WS: %v — reconnecting in %s", err, reconnectDelay)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectDelay):
		}
	}
}

func (a *Adapter) connectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	conn, _, err := dialer.DialContext(ctx, spotWS, nil)
	if err != nil {
		return fmt.Errorf("dial spot: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	defer conn.Close()

	log.Printf("✅ MEXC Spot connected")

	sub := subscribeMsg{
		Method: "SUBSCRIPTION",
		Params: []string{"spot@public.miniTickers.v3.api"},
	}
	if err := a.writeJSON(conn, sub); err != nil {
		return fmt.Errorf("subscribe spot: %w", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	go a.spotKeepAlive(connCtx, conn)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read spot: %w", err)
		}

		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		// MEXC Spot шлёт PONG как TextMessage — не Control Frame
		// Просто обновляем дедлайн (уже сделано выше) и пропускаем
		if strings.Contains(string(msg), "PONG") {
			continue
		}

		var raw struct {
			Data []spotTickerData `json:"d"`
		}
		if err := sonic.Unmarshal(msg, &raw); err != nil || len(raw.Data) == 0 {
			continue
		}

		now := time.Now()
		for i := range raw.Data {
			tick, ok := spotToTick(&raw.Data[i], now)
			if !ok {
				continue
			}
			select {
			case out <- tick:
			case <-connCtx.Done():
				return nil
			default:
				if n := a.droppedTicks.Add(1); n%1000 == 0 {
					log.Printf("⚠️  MEXC Spot: dropped %d ticks (channel full)", n)
				}
			}
		}
	}
}

func (a *Adapter) connectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	conn, _, err := dialer.DialContext(ctx, futuresWS, nil)
	if err != nil {
		return fmt.Errorf("dial futures: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	defer conn.Close()

	log.Printf("✅ MEXC Futures connected")

	sub := subscribeMsg{
		Method: "sub.ticker",
		Params: []string{"all"},
	}
	if err := a.writeJSON(conn, sub); err != nil {
		return fmt.Errorf("subscribe futures: %w", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	go a.futuresKeepAlive(connCtx, conn)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read futures: %w", err)
		}

		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		// ✅ Отвечаем на серверный ping
		// Используем conn.WriteMessage — WriteTextMessage не существует в gorilla
		if strings.Contains(string(msg), `"ping"`) {
			if err := a.writeMessage(conn, websocket.TextMessage, futuresPongMsg); err != nil {
				return fmt.Errorf("write pong: %w", err)
			}
			continue
		}

		var resp futuresResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil {
			continue
		}

		if resp.Channel != "push.ticker" {
			continue
		}

		tick, ok := futuresToTick(&resp.Data)
		if !ok {
			continue
		}

		select {
		case out <- tick:
		case <-connCtx.Done():
			return nil
		default:
			if n := a.droppedTicks.Add(1); n%1000 == 0 {
				log.Printf("⚠️  MEXC Futures: dropped %d ticks (channel full)", n)
			}
		}
	}
}

func spotToTick(d *spotTickerData, ts time.Time) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(d.Bid)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(d.Ask)
	if err != nil || ask.IsZero() || bid.GreaterThan(ask) {
		return domain.MarketTick{}, false
	}

	qVol, err := decimal.NewFromString(d.QVolume)
	if err != nil {
		return domain.MarketTick{}, false
	}

	return domain.MarketTick{
		Exchange:    "MEXC",
		Symbol:      d.Symbol, // Spot уже в формате BTCUSDT
		MarketType:  domain.MarketTypeSpot,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		EventTime:   ts,
		ReceivedAt:  ts,
		Timestamp:   ts,
	}, true
}

func futuresToTick(d *futuresTickerData) (domain.MarketTick, bool) {
	bid, err := rawDecimal(d.Bid1)
	if err != nil || !bid.IsPositive() {
		return domain.MarketTick{}, false
	}
	ask, err := rawDecimal(d.Ask1)
	if err != nil || !ask.IsPositive() || bid.GreaterThan(ask) {
		return domain.MarketTick{}, false
	}
	vol, err := rawDecimal(d.Volume)
	if err != nil {
		return domain.MarketTick{}, false
	}
	symbol := strings.ReplaceAll(d.Symbol, "_", "")
	now := time.Now()
	return domain.MarketTick{Exchange: "MEXC", Symbol: symbol, MarketType: domain.MarketTypeFutures, BestBid: bid, BestAsk: ask, QuoteVolume: vol, EventTime: now, ReceivedAt: now}, true
}

func rawDecimal(raw json.RawMessage) (decimal.Decimal, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return decimal.Zero, fmt.Errorf("empty numeric value")
	}
	return decimal.NewFromString(string(raw))
}

func (a *Adapter) spotKeepAlive(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := a.writeMessage(conn, websocket.TextMessage, spotPingMsg); err != nil {
				log.Printf("⚠️  MEXC Spot ping error: %v", err)
				return
			}
		}
	}
}

func (a *Adapter) futuresKeepAlive(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := a.writeMessage(conn, websocket.TextMessage, futuresPingMsg); err != nil {
				log.Printf("⚠️  MEXC Futures ping error: %v", err)
				return
			}
		}
	}
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}

func (a *Adapter) writeMessage(conn *websocket.Conn, messageType int, data []byte) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return conn.WriteMessage(messageType, data)
}

func (a *Adapter) writeJSON(conn *websocket.Conn, v any) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return conn.WriteJSON(v)
}

func (a *Adapter) ConnectCandles(ctx context.Context, sink domain.CandleSink) error {
	go a.runCandleFeed(ctx, sink, true)
	go a.runCandleFeed(ctx, sink, false)
	<-ctx.Done()
	return nil
}

type mexcCandleResponse struct {
	Channel string `json:"channel"`
	Data    struct {
		Symbol string          `json:"symbol"`
		T      int64           `json:"t"`
		Q      json.RawMessage `json:"q"`
		Amount json.RawMessage `json:"a"`
	} `json:"data"`
	PublicSpot struct {
		Symbol      string `json:"symbol"`
		WindowStart int64  `json:"windowstart"`
		Amount      string `json:"amount"`
	} `json:"publicspotkline"`
	Ts int64 `json:"ts"`
}

type mexcSpotInfo struct {
	Symbols []struct {
		Symbol     string `json:"symbol"`
		Status     int    `json:"status"`
		QuoteAsset string `json:"quoteAsset"`
	} `json:"symbols"`
}
type mexcFuturesInfo struct {
	Data []struct {
		Symbol    string `json:"symbol"`
		QuoteCoin string `json:"quoteCoin"`
	} `json:"data"`
}

func (a *Adapter) runCandleFeed(ctx context.Context, sink domain.CandleSink, spot bool) {
	for ctx.Err() == nil {
		symbols, err := mexcCandleSymbols(ctx, spot)
		if err != nil {
			log.Printf("⚠️ MEXC candle symbols: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}
		url := spotWS
		for i := 0; i < len(symbols); i += 50 {
			end := i + 50
			if end > len(symbols) {
				end = len(symbols)
			}
			if err := a.readCandleShard(ctx, sink, url, symbols[i:end], spot); err != nil && ctx.Err() == nil {
				log.Printf("⚠️ MEXC candle WS: %v", err)
			}
		}
	}
}

func mexcCandleSymbols(ctx context.Context, spot bool) ([]string, error) {
	if spot {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.mexc.com/api/v3/exchangeInfo", nil)
		if err != nil {
			return nil, err
		}
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer r.Body.Close()
		var x mexcSpotInfo
		if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
			return nil, err
		}
		out := make([]string, 0, len(x.Symbols))
		for _, v := range x.Symbols {
			if v.QuoteAsset == "USDT" && v.Status == 1 {
				out = append(out, v.Symbol)
			}
		}
		return out, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://contract.mexc.com/api/v1/contract/detail", nil)
	if err != nil {
		return nil, err
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	var x mexcFuturesInfo
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(x.Data))
	for _, v := range x.Data {
		if v.QuoteCoin == "USDT" {
			out = append(out, v.Symbol)
		}
	}
	return out, nil
}

func (a *Adapter) readCandleShard(ctx context.Context, sink domain.CandleSink, _ string, symbols []string, spot bool) error {
	url := spotWS
	if !spot {
		url = futuresWS
	}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	go closeOnCtx(ctx, conn)
	if spot {
		params := make([]string, 0, len(symbols))
		for _, sym := range symbols {
			params = append(params, "spot@public.kline.v3.api.pb@"+sym+"@Min1")
		}
		if err := a.writeJSON(conn, subscribeMsg{Method: "SUBSCRIPTION", Params: params}); err != nil {
			return err
		}
	} else {
		for _, sym := range symbols {
			if err := a.writeJSON(conn, map[string]any{"method": "sub.kline", "param": map[string]string{"symbol": sym, "interval": "Min1"}}); err != nil {
				return err
			}
		}
	}
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		var r mexcCandleResponse
		if sonic.Unmarshal(msg, &r) != nil {
			continue
		}
		symbol := r.Data.Symbol
		start := r.Data.T
		raw := r.Data.Amount
		if spot {
			symbol = r.PublicSpot.Symbol
			start = r.PublicSpot.WindowStart
			raw = json.RawMessage(r.PublicSpot.Amount)
		} else if len(raw) == 0 {
			raw = r.Data.Q
		}
		if symbol == "" || start == 0 {
			continue
		}
		var vol decimal.Decimal
		if err := json.Unmarshal(raw, &vol); err != nil || !vol.IsPositive() {
			continue
		}
		et := time.UnixMilli(r.Ts)
		if r.Ts == 0 {
			et = time.Now()
		}
		sym := strings.ReplaceAll(symbol, "_", "")
		mt := domain.MarketTypeFutures
		if spot {
			mt = domain.MarketTypeSpot
		}
		sink.UpdateCandle(domain.MarketCandle{Exchange: "MEXC", Symbol: sym, MarketType: mt, OpenTime: time.Unix(start, 0), CloseTime: time.Unix(start, 0).Add(time.Minute - time.Millisecond), QuoteVolume: vol, EventTime: et})
	}
}
