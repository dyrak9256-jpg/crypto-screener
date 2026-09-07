package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/ingress"
	"crypto-screener/internal/retry"
	"crypto-screener/internal/wsutil"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	wsURL = "wss://ws.okx.com:8443/ws/v5/public"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 20 * time.Second
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var (
	dialer     = websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	httpClient = &http.Client{Timeout: 10 * time.Second}
	pingMsg    = []byte("ping")
)

type subscribeMsg struct {
	Op   string    `json:"op"`
	Args []argItem `json:"args"`
}

type argItem struct {
	Channel  string `json:"channel"`
	InstID   string `json:"instId,omitempty"`
	InstType string `json:"instType,omitempty"`
}

type wsResponse struct {
	Event string       `json:"event,omitempty"`
	Arg   argItem      `json:"arg"`
	Data  []tickerData `json:"data"`
}

type tickerData struct {
	InstID    string `json:"instId"`
	LastPx    string `json:"last"`
	BidPx     string `json:"bidPx"`
	AskPx     string `json:"askPx"`
	VolCcy24h string `json:"volCcy24h"`
	Ts        int64  `json:"ts,string"`
}

type Adapter struct {
	droppedTicks atomic.Uint64
	fundingSink  atomic.Value
	fundingReady chan struct{}
	fundingOnce  sync.Once
}

func NewAdapter() *Adapter { return &Adapter{fundingReady: make(chan struct{})} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, "SPOT", domain.MarketTypeSpot, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	select {
	case <-a.fundingReady:
	case <-ctx.Done():
		return nil
	}
	a.listen(ctx, "SWAP", domain.MarketTypeFutures, out)
	return nil
}
func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	if sink == nil {
		return fmt.Errorf("OKX funding sink is nil")
	}
	a.fundingSink.Store(sink)
	a.fundingOnce.Do(func() { close(a.fundingReady) })
	a.listenFunding(ctx)
	return nil
}

func (a *Adapter) listenFunding(ctx context.Context) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		startedAt := time.Now()
		if err := a.connectFundingWS(ctx); err != nil && ctx.Err() == nil {
			if v := a.fundingSink.Load(); v != nil {
				v.(domain.FundingSink).SetStreamHealth("OKX", false)
			}
			log.Printf("⚠️ OKX funding WS: %v", err)
		}
		if time.Since(startedAt) >= 30*time.Second {
			backoff.Reset()
		}
		if !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) connectFundingWS(ctx context.Context) error {
	raw, err := okxRawSwapSymbols(ctx)
	if err != nil {
		return fmt.Errorf("load swap symbols for funding: %w", err)
	}
	args := make([]argItem, 0, len(raw))
	for _, id := range raw {
		args = append(args, argItem{Channel: "funding-rate", InstID: id})
	}
	shards := shardArgItems(args, 50000)
	if len(shards) == 0 {
		return fmt.Errorf("OKX returned no USDT swap symbols for funding")
	}
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, len(shards))
	var wg sync.WaitGroup
	for _, shard := range shards {
		shard := append([]argItem(nil), shard...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.readFundingShard(connCtx, shard); err != nil && connCtx.Err() == nil {
				errCh <- err
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return fmt.Errorf("connectFundingWS: %w", err)
	}
	return nil
}

func shardArgItems(args []argItem, maxBytes int) [][]argItem {
	var out [][]argItem
	var current []argItem
	for _, arg := range args {
		candidate := append(append([]argItem(nil), current...), arg)
		payload, err := json.Marshal(subscribeMsg{Op: "subscribe", Args: candidate})
		if err != nil {
			continue
		}
		if len(current) > 0 && len(payload) > maxBytes {
			out = append(out, current)
			current = []argItem{arg}
			continue
		}
		current = candidate
	}
	if len(current) > 0 {
		out = append(out, current)
	}
	return out
}

func (a *Adapter) readFundingShard(ctx context.Context, args []argItem) error {
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial funding shard: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	if err := conn.WriteJSON(subscribeMsg{Op: "subscribe", Args: args}); err != nil {
		return fmt.Errorf("subscribe funding shard: %w", err)
	}
	go closeOnCtx(ctx, conn)
	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set funding read deadline: %w", err)
	}
	go okxKeepAlive(ctx, conn)
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read funding shard: %w", err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("refresh funding read deadline: %w", err)
		}
		var resp wsResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil || resp.Arg.Channel != "funding-rate" {
			continue
		}
		v := a.fundingSink.Load()
		if v == nil {
			continue
		}
		sink := v.(domain.FundingSink)
		for _, d := range resp.Data {
			rate, err := decimal.NewFromString(d.FundingRate)
			if err != nil {
				continue
			}
			next := time.UnixMilli(d.NextFundingTime)
			et := time.UnixMilli(d.Ts)
			if d.Ts == 0 {
				et = time.Now()
			}
			if err := sink.UpdateFunding("OKX", strings.ReplaceAll(d.InstID, "-", ""), rate, next, et); err != nil {
				return fmt.Errorf("update OKX funding %s: %w", d.InstID, err)
			}
		}
	}
}

func (a *Adapter) listen(
	ctx context.Context,
	instType string,
	mType domain.MarketType,
	out chan<- domain.MarketTick,
) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for {
		if ctx.Err() != nil {
			return
		}
		startedAt := time.Now()
		if err := a.connectAndRead(ctx, instType, mType, out); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  OKX %s WS: %v — reconnecting in %s", mType, err, reconnectDelay)
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
	instType string,
	mType domain.MarketType,
	out chan<- domain.MarketTick,
) error {
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	defer conn.Close()

	log.Printf("✅ OKX %s connected", mType)

	args := []argItem{{Channel: "tickers", InstType: instType}}
	for i := 0; i < len(args); i += 100 {
		end := i + 100
		if end > len(args) {
			end = len(args)
		}
		if err := conn.WriteJSON(subscribeMsg{Op: "subscribe", Args: args[i:end]}); err != nil {
			return fmt.Errorf("subscribe batch: %w", err)
		}
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	// ✅ Нет SetPongHandler — OKX шлёт "pong" как TextMessage
	// Дедлайн сбрасывается в цикле при каждом сообщении
	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	go okxKeepAlive(connCtx, conn)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("refresh OKX read deadline: %w", err)
		}

		// OKX pong приходит как текст — не Control Frame
		if string(msg) == "pong" {
			continue
		}

		var resp wsResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil {
			continue
		}

		if resp.Event != "" || len(resp.Data) == 0 {
			continue
		}
		now := time.Now()
		for i := range resp.Data {
			eventTime := now
			if resp.Data[i].Ts > 0 {
				eventTime = time.UnixMilli(resp.Data[i].Ts)
			}
			tick, ok := toMarketTick(&resp.Data[i], mType, eventTime)
			if !ok {
				continue
			}
			ingress.Submit(out, tick)
		}
	}
}

func toMarketTick(d *tickerData, mType domain.MarketType, ts time.Time) (domain.MarketTick, bool) {
	// Нормализация и фильтрация — до парсинга decimal
	// Не тратим ресурсы на парсинг если символ нам не нужен
	symbol, ok := normalizeSymbol(d.InstID, mType)
	if !ok {
		return domain.MarketTick{}, false
	}

	bid, err := decimal.NewFromString(d.BidPx)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(d.AskPx)
	if err != nil || ask.IsZero() || bid.GreaterThan(ask) {
		return domain.MarketTick{}, false
	}

	qVol, err := decimal.NewFromString(d.VolCcy24h)
	if err != nil || qVol.IsNegative() {
		return domain.MarketTick{}, false
	}
	// OKX reports volCcy24h in base currency for derivatives, not quote
	// currency. The screener's volume contract is quote notional, so convert
	// derivative volume using the latest traded price. This is an estimate for
	// the rolling 24h window; using the raw base amount would be dimensionally
	// wrong and could make user volume filters pass by ~price multiples.
	if mType == domain.MarketTypeFutures {
		last, parseErr := decimal.NewFromString(d.LastPx)
		if parseErr != nil || !last.IsPositive() {
			last = bid.Add(ask).Div(decimal.NewFromInt(2))
		}
		if !last.IsPositive() {
			return domain.MarketTick{}, false
		}
		qVol = qVol.Mul(last)
	}

	return domain.MarketTick{
		Exchange:    "OKX",
		Symbol:      symbol,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		EventTime:   ts,
		ReceivedAt:  time.Now(),
		Timestamp:   ts,
	}, true
}

// normalizeSymbol приводит OKX instId к формату BTCUSDT
//
// Spot:
//
//	"BTC-USDT"       → "BTCUSDT",  true
//	"BTC-USDC"       → "",         false  (не USDT)
//	"BTC-DAI"        → "",         false  (не USDT)
//
// Futures (SWAP):
//
//	"BTC-USDT-SWAP"  → "BTCUSDT",  true
//	"BTC-USD-SWAP"   → "",         false  (инверсный контракт)
//	"BTC-USDC-SWAP"  → "",         false  (не USDT)
func normalizeSymbol(instID string, mType domain.MarketType) (string, bool) {
	if mType == domain.MarketTypeSpot {
		if !strings.HasSuffix(instID, "-USDT") {
			return "", false
		}
		return strings.ReplaceAll(instID, "-", ""), true
	}

	// Futures: только USDT perpetual контракты
	if !strings.HasSuffix(instID, "-USDT-SWAP") {
		return "", false
	}

	// "BTC-USDT-SWAP" → убираем "-SWAP" → "BTC-USDT" → убираем "-" → "BTCUSDT"
	s := strings.TrimSuffix(instID, "-SWAP")
	return strings.ReplaceAll(s, "-", ""), true
}

func okxKeepAlive(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := conn.WriteMessage(websocket.TextMessage, pingMsg); err != nil {
				wrapped := fmt.Errorf("OKX keepalive: %w", err)
				log.Printf("⚠️  %v", wrapped)
				return
			}
		}
	}
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}

func (a *Adapter) ConnectCandles(ctx context.Context, sink domain.CandleSink) error {
	go a.runCandleFeed(ctx, sink, "SPOT", domain.MarketTypeSpot)
	go a.runCandleFeed(ctx, sink, "SWAP", domain.MarketTypeFutures)
	<-ctx.Done()
	return nil
}

type okxCandleResponse struct {
	Event string     `json:"event"`
	Arg   argItem    `json:"arg"`
	Data  [][]string `json:"data"`
	Ts    int64      `json:"ts,string"`
}

type okxInstrument struct {
	InstID   string `json:"instId"`
	State    string `json:"state"`
	QuoteCcy string `json:"quoteCcy"`
	InstType string `json:"instType"`
}
type okxInstrumentResponse struct {
	Code string          `json:"code"`
	Data []okxInstrument `json:"data"`
}

func (a *Adapter) runCandleFeed(ctx context.Context, sink domain.CandleSink, instType string, market domain.MarketType) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		symbols, err := okxSymbols(ctx, instType)
		if err != nil {
			log.Printf("⚠️ OKX %s candle symbols: %v", market, err)
			select {
			case <-ctx.Done():
				return
			case <-retry.After(reconnectDelay):
			}
			continue
		}
		if err := a.readCandleShard(ctx, sink, instType, symbols, market); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ OKX %s candle WS: %v", market, err)
		}
		if ctx.Err() == nil && !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func okxRawSwapSymbols(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.okx.com/api/v5/public/instruments?instType=SWAP", nil)
	if err != nil {
		return nil, fmt.Errorf("okxRawSwapSymbols: %w", err)
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("okxRawSwapSymbols: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %s", r.Status)
	}
	var x okxInstrumentResponse
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return nil, fmt.Errorf("okxRawSwapSymbols: %w", err)
	}
	if x.Code != "0" {
		return nil, fmt.Errorf("API code %s", x.Code)
	}
	out := make([]string, 0, len(x.Data))
	for _, v := range x.Data {
		if v.State == "live" && strings.HasSuffix(v.InstID, "-USDT-SWAP") {
			out = append(out, v.InstID)
		}
	}
	return out, nil
}

func okxSymbols(ctx context.Context, instType string) ([]string, error) {
	u := "https://www.okx.com/api/v5/public/instruments?instType=" + instType
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("okxSymbols: %w", err)
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("okxSymbols: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return nil, fmt.Errorf("okxSymbols: HTTP %s", r.Status)
	}
	var x okxInstrumentResponse
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return nil, fmt.Errorf("okxSymbols: %w", err)
	}
	if x.Code != "0" {
		return nil, fmt.Errorf("okxSymbols: API code %s", x.Code)
	}
	out := make([]string, 0, len(x.Data))
	for _, v := range x.Data {
		if v.State == "live" && (instType != "SPOT" || v.QuoteCcy == "USDT") {
			out = append(out, v.InstID)
		}
	}
	return out, nil
}

func (a *Adapter) readCandleShard(ctx context.Context, sink domain.CandleSink, instType string, symbols []string, market domain.MarketType) error {
	conn, _, err := dialer.DialContext(ctx, "wss://ws.okx.com:8443/ws/v5/business", nil)
	if err != nil {
		return fmt.Errorf("readCandleShard: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := wsutil.StartHeartbeat(connCtx, conn, 20*time.Second, 60*time.Second); err != nil {
		return fmt.Errorf("start websocket heartbeat: %w", err)
	}
	go closeOnCtx(connCtx, conn)
	args := make([]argItem, 0, len(symbols))
	for _, sym := range symbols {
		args = append(args, argItem{Channel: "candle1m", InstID: sym})
	}
	for i := 0; i < len(args); i += 100 {
		end := i + 100
		if end > len(args) {
			end = len(args)
		}
		if err := conn.WriteJSON(subscribeMsg{Op: "subscribe", Args: args[i:end]}); err != nil {
			return fmt.Errorf("readCandleShard: %w", err)
		}
	}
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("readCandleShard: %w", err)
		}
		if err := wsutil.TouchReadDeadline(conn, 60*time.Second); err != nil {
			return fmt.Errorf("refresh websocket read deadline: %w", err)
		}
		var r okxCandleResponse
		if err := sonic.Unmarshal(msg, &r); err != nil {
			continue
		}
		if r.Event == "error" {
			return fmt.Errorf("OKX candle subscription rejected: %s", string(msg))
		}
		if r.Event == "subscribe" {
			continue
		}
		if len(r.Data) == 0 || r.Arg.InstID == "" {
			continue
		}
		for _, d := range r.Data {
			if len(d) < 8 {
				continue
			}
			start, err := strconv.ParseInt(d[0], 10, 64)
			if err != nil {
				continue
			}
			q, err := decimal.NewFromString(d[7])
			if err != nil || q.IsNegative() {
				continue
			}
			et := time.UnixMilli(r.Ts)
			if err := sink.UpdateCandle(domain.MarketCandle{Exchange: "OKX", Symbol: strings.ReplaceAll(r.Arg.InstID, "-", ""), MarketType: market, OpenTime: time.UnixMilli(start), CloseTime: time.UnixMilli(start).Add(time.Minute - time.Millisecond), QuoteVolume: q, EventTime: et, Closed: false}); err != nil {
				return fmt.Errorf("readCandleShard: %w", err)
			}
		}
	}
}
