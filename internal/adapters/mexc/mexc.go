package mexc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/ingress"
	"crypto-screener/internal/retry"

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
	httpClient     = &http.Client{Timeout: 10 * time.Second}
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
	Symbol    string          `json:"symbol"`
	LastPrice json.RawMessage `json:"lastPrice"`
	Bid1      json.RawMessage `json:"bid1"`
	Ask1      json.RawMessage `json:"ask1"`
	Volume    json.RawMessage `json:"volume24"`
}

type Adapter struct {
	droppedTicks atomic.Uint64
	writeMu      sync.Mutex
	contractsMu  sync.RWMutex
	contractSize map[string]decimal.Decimal
	contractsAt  time.Time
}

func NewAdapter() *Adapter {
	return &Adapter{contractSize: make(map[string]decimal.Decimal)}
}
func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	if sink == nil {
		return fmt.Errorf("MEXC funding sink is nil")
	}
	// Список фьючерсных символов нужен до старта поллинга. Раньше единичный
	// сбой REST здесь завершал funding-поток навсегда; теперь ретраим.
	backoff := retry.New(reconnectDelay, 30*time.Second)
	var symbols []string
	for {
		loaded, err := mexcCandleSymbols(ctx, false)
		if err == nil && len(loaded) > 0 {
			symbols = loaded
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("MEXC returned no futures symbols")
		}
		log.Printf("⚠️ MEXC funding symbols: %v — retrying", err)
		if !backoff.Wait(ctx.Done()) {
			return nil
		}
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	cursor := 0
	for {
		end := cursor + 10
		if end > len(symbols) {
			end = len(symbols)
		}
		for _, sym := range symbols[cursor:end] {
			if err := a.pollOneFunding(ctx, sink, sym); err != nil && ctx.Err() == nil {
				log.Printf("⚠️ MEXC funding %s: %v", sym, err)
			}
		}
		cursor = end
		if cursor >= len(symbols) {
			cursor = 0
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
func (a *Adapter) pollOneFunding(ctx context.Context, sink domain.FundingSink, symbol string) error {
	u := "https://contract.mexc.com/api/v1/contract/funding_rate/" + symbol
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("pollOneFunding: %w", err)
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("pollOneFunding: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s", r.Status)
	}
	var x struct {
		Success bool `json:"success"`
		Data    struct {
			Symbol         string          `json:"symbol"`
			FundingRate    decimal.Decimal `json:"fundingRate"`
			NextSettleTime int64           `json:"nextSettleTime"`
			Timestamp      int64           `json:"timestamp"`
		} `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return fmt.Errorf("pollOneFunding: %w", err)
	}
	if !x.Success {
		return fmt.Errorf("API returned success=false")
	}
	et := time.UnixMilli(x.Data.Timestamp)
	if x.Data.Timestamp == 0 {
		et = time.Now()
	}
	return sink.UpdateFunding("MEXC", strings.ReplaceAll(x.Data.Symbol, "_", ""), x.Data.FundingRate, time.UnixMilli(x.Data.NextSettleTime), et)
}

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listenSpot(ctx, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	// Раньше временный сбой REST при старте навсегда убивал фьючерсный поток
	// MEXC (connector goroutine завершался, менеджер его не перезапускал).
	// Теперь загрузка размеров контрактов ретраится с backoff до успеха или
	// отмены контекста — как это делают остальные адаптеры.
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for {
		if err := a.ensureContractSizes(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("⚠️ MEXC futures contract sizes: %v — retrying", err)
			if !backoff.Wait(ctx.Done()) {
				return nil
			}
			continue
		}
		break
	}
	a.listenFutures(ctx, out)
	return nil
}

func (a *Adapter) listenSpot(ctx context.Context, out chan<- domain.MarketTick) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
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
		if !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) listenFutures(ctx context.Context, out chan<- domain.MarketTick) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
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
		if !backoff.Wait(ctx.Done()) {
			return
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

		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("refresh MEXC read deadline: %w", err)
		}

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
			ingress.Submit(out, tick)
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

		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("refresh MEXC read deadline: %w", err)
		}

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

		tick, ok := a.futuresToTick(&resp.Data)
		if !ok {
			continue
		}
		ingress.Submit(out, tick)
	}
}

func spotToTick(d *spotTickerData, ts time.Time) (domain.MarketTick, bool) {
	if !strings.HasSuffix(strings.ToUpper(d.Symbol), "USDT") {
		return domain.MarketTick{}, false
	}
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
		ReceivedAt:  time.Now(),
		Timestamp:   ts,
	}, true
}

func (a *Adapter) futuresToTick(d *futuresTickerData) (domain.MarketTick, bool) {
	bid, err := rawDecimal(d.Bid1)
	if err != nil || !bid.IsPositive() {
		return domain.MarketTick{}, false
	}
	ask, err := rawDecimal(d.Ask1)
	if err != nil || !ask.IsPositive() || bid.GreaterThan(ask) {
		return domain.MarketTick{}, false
	}
	last, err := rawDecimal(d.LastPrice)
	if err != nil || !last.IsPositive() {
		last = bid.Add(ask).Div(decimal.NewFromInt(2))
	}
	contracts, err := rawDecimal(d.Volume)
	if err != nil || contracts.IsNegative() {
		return domain.MarketTick{}, false
	}
	symbol := strings.NewReplacer("_", "", "-", "").Replace(d.Symbol)
	if !strings.HasSuffix(strings.ToUpper(symbol), "USDT") {
		return domain.MarketTick{}, false
	}
	rawSymbol := strings.ToUpper(strings.TrimSpace(d.Symbol))
	a.contractsMu.RLock()
	contractSize, ok := a.contractSize[rawSymbol]
	a.contractsMu.RUnlock()
	if !ok || !contractSize.IsPositive() {
		// MEXC volume24 is a contract count, not quote notional. Without the
		// contract size we cannot safely convert it to the screener's USDT volume
		// contract, so fail closed rather than producing a wildly wrong filter.
		return domain.MarketTick{}, false
	}
	quoteVolume := contracts.Mul(contractSize).Mul(last)
	now := time.Now()
	return domain.MarketTick{Exchange: "MEXC", Symbol: symbol, MarketType: domain.MarketTypeFutures, BestBid: bid, BestAsk: ask, QuoteVolume: quoteVolume, EventTime: now, ReceivedAt: now, Timestamp: now}, true
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
				log.Printf("⚠️  %v", fmt.Errorf("MEXC spot keepalive: %w", err))
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
				log.Printf("⚠️  %v", fmt.Errorf("MEXC futures keepalive: %w", err))
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
	if err := conn.WriteMessage(messageType, data); err != nil {
		return fmt.Errorf("write websocket message: %w", err)
	}
	return nil
}

func (a *Adapter) writeJSON(conn *websocket.Conn, v any) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if err := conn.WriteJSON(v); err != nil {
		return fmt.Errorf("write websocket JSON: %w", err)
	}
	return nil
}

func (a *Adapter) ConnectCandles(ctx context.Context, sink domain.CandleSink) error {
	go a.runCandleFeed(ctx, sink, true)
	go a.runCandleFeed(ctx, sink, false)
	<-ctx.Done()
	return nil
}

type mexcCandleResponse struct {
	Channel string `json:"channel"`
	Symbol  string `json:"symbol"`
	Data    struct {
		Symbol string          `json:"symbol"`
		T      int64           `json:"t"`
		Q      json.RawMessage `json:"q"`
		Amount json.RawMessage `json:"a"`
	} `json:"data"`
	PublicSpot struct {
		WindowStart int64  `json:"windowstart"`
		Amount      string `json:"amount"`
	} `json:"publicspotkline"`
	Ts         int64 `json:"ts"`
	Createtime int64 `json:"createtime"`
}

type mexcSpotInfo struct {
	Symbols []struct {
		Symbol string `json:"symbol"`
		// MEXC сменил тип status с int на строку ("1" = активна) —
		// подтверждено живым API 08.09.2026.
		Status     string `json:"status"`
		QuoteAsset string `json:"quoteAsset"`
	} `json:"symbols"`
}
type mexcFuturesInfo struct {
	Success bool `json:"success"`
	Code    int  `json:"code"`
	Data    []struct {
		Symbol       string          `json:"symbol"`
		QuoteCoin    string          `json:"quoteCoin"`
		ContractSize decimal.Decimal `json:"contractSize"`
		State        int             `json:"state"`
	} `json:"data"`
}

func (a *Adapter) runCandleFeed(ctx context.Context, sink domain.CandleSink, spot bool) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		symbols, err := mexcCandleSymbols(ctx, spot)
		if err != nil {
			log.Printf("⚠️ MEXC candle symbols: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-retry.After(reconnectDelay):
			}
			continue
		}
		url := spotWS
		var wg sync.WaitGroup
		for i := 0; i < len(symbols); i += 30 {
			end := i + 30
			if end > len(symbols) {
				end = len(symbols)
			}
			part := append([]string(nil), symbols[i:end]...)
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.runMEXCCandleShard(ctx, sink, url, part, spot)
			}()
		}
		wg.Wait()
		if ctx.Err() == nil && !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) ensureContractSizes(ctx context.Context) error {
	now := time.Now()
	a.contractsMu.RLock()
	ready := len(a.contractSize) > 0 && now.Sub(a.contractsAt) < 5*time.Minute
	a.contractsMu.RUnlock()
	if ready {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://contract.mexc.com/api/v1/contract/detail", nil)
	if err != nil {
		return fmt.Errorf("create contract detail request: %w", err)
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request contract detail: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return fmt.Errorf("contract detail HTTP %s", r.Status)
	}
	var x mexcFuturesInfo
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return fmt.Errorf("decode contract detail: %w", err)
	}
	if !x.Success || x.Code != 0 {
		return fmt.Errorf("contract detail API error: success=%t code=%d", x.Success, x.Code)
	}
	sizes := make(map[string]decimal.Decimal, len(x.Data))
	for _, v := range x.Data {
		if v.State != 0 || v.QuoteCoin != "USDT" || !v.ContractSize.IsPositive() {
			continue
		}
		sizes[strings.ToUpper(strings.TrimSpace(v.Symbol))] = v.ContractSize
	}
	if len(sizes) == 0 {
		return errors.New("contract detail returned no active USDT contracts with valid contract size")
	}
	a.contractsMu.Lock()
	a.contractSize = sizes
	a.contractsAt = now
	a.contractsMu.Unlock()
	return nil
}

func mexcCandleSymbols(ctx context.Context, spot bool) ([]string, error) {
	if spot {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.mexc.com/api/v3/exchangeInfo", nil)
		if err != nil {
			return nil, fmt.Errorf("mexcCandleSymbols: %w", err)
		}
		r, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("mexcCandleSymbols: %w", err)
		}
		defer r.Body.Close()
		if r.StatusCode < 200 || r.StatusCode >= 300 {
			return nil, fmt.Errorf("mexcCandleSymbols: HTTP %s", r.Status)
		}
		var x mexcSpotInfo
		if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
			return nil, fmt.Errorf("mexcCandleSymbols: %w", err)
		}
		out := make([]string, 0, len(x.Symbols))
		for _, v := range x.Symbols {
			if v.QuoteAsset == "USDT" && v.Status == "1" {
				out = append(out, v.Symbol)
			}
		}
		return out, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://contract.mexc.com/api/v1/contract/detail", nil)
	if err != nil {
		return nil, fmt.Errorf("mexcCandleSymbols: %w", err)
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mexcCandleSymbols: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return nil, fmt.Errorf("mexcCandleSymbols: HTTP %s", r.Status)
	}
	var x mexcFuturesInfo
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return nil, fmt.Errorf("mexcCandleSymbols: %w", err)
	}
	if !x.Success || x.Code != 0 {
		return nil, fmt.Errorf("mexcCandleSymbols: API error success=%t code=%d", x.Success, x.Code)
	}
	out := make([]string, 0, len(x.Data))
	for _, v := range x.Data {
		if v.QuoteCoin == "USDT" && v.State == 0 {
			out = append(out, v.Symbol)
		}
	}
	return out, nil
}

func (a *Adapter) runMEXCCandleShard(ctx context.Context, sink domain.CandleSink, url string, symbols []string, spot bool) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		if err := a.readCandleShard(ctx, sink, url, symbols, spot); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ MEXC candle WS: %v", err)
		}
		if ctx.Err() == nil && !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) readCandleShard(ctx context.Context, sink domain.CandleSink, _ string, symbols []string, spot bool) error {
	url := spotWS
	if !spot {
		url = futuresWS
	}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial candle websocket: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go closeOnCtx(connCtx, conn)
	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set MEXC candle read deadline: %w", err)
	}

	if spot {
		params := make([]string, 0, len(symbols))
		for _, sym := range symbols {
			params = append(params, "spot@public.kline.v3.api.pb@"+sym+"@Min1")
		}
		if err := a.writeJSON(conn, subscribeMsg{Method: "SUBSCRIPTION", Params: params}); err != nil {
			return fmt.Errorf("subscribe MEXC spot candles: %w", err)
		}
	} else {
		for _, sym := range symbols {
			if err := a.writeJSON(conn, map[string]any{"method": "sub.kline", "param": map[string]string{"symbol": sym, "interval": "Min1"}}); err != nil {
				return fmt.Errorf("subscribe MEXC futures candle %s: %w", sym, err)
			}
		}
	}
	go a.candleKeepAlive(connCtx, cancel, conn, spot)

	for {
		messageType, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read MEXC candle websocket: %w", err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("refresh MEXC candle read deadline: %w", err)
		}

		if messageType == websocket.TextMessage {
			var ack struct {
				ID   int    `json:"id"`
				Code int    `json:"code"`
				Msg  string `json:"msg"`
			}
			if err := sonic.Unmarshal(msg, &ack); err == nil && (ack.Code != 0 || ack.Msg != "") {
				if ack.Code != 0 {
					return fmt.Errorf("MEXC candle subscription response code=%d msg=%q", ack.Code, ack.Msg)
				}
				continue
			}
			if !spot {
				var r mexcCandleResponse
				if err := sonic.Unmarshal(msg, &r); err != nil {
					continue
				}
				if err := a.handleMEXCFuturesCandle(sink, r); err != nil {
					return fmt.Errorf("handle MEXC futures candle: %w", err)
				}
			}
			continue
		}
		if !spot || messageType != websocket.BinaryMessage {
			continue
		}
		decoded, err := decodeMEXCSpotKline(msg)
		if err != nil {
			return fmt.Errorf("decode MEXC spot kline protobuf: %w", err)
		}
		if decoded.Symbol == "" || decoded.WindowStart == 0 || decoded.Amount == "" {
			continue
		}
		volume, err := decimal.NewFromString(decoded.Amount)
		if err != nil {
			return fmt.Errorf("parse MEXC spot quote volume %s: %w", decoded.Symbol, err)
		}
		if volume.IsNegative() {
			return fmt.Errorf("MEXC spot quote volume is negative for %s: %s", decoded.Symbol, volume.String())
		}
		eventTime := time.UnixMilli(decoded.SendTime)
		if decoded.SendTime == 0 {
			eventTime = time.UnixMilli(decoded.CreateTime)
		}
		if eventTime.IsZero() {
			eventTime = time.Now()
		}
		symbol := strings.NewReplacer("_", "", "-", "").Replace(decoded.Symbol)
		if err := sink.UpdateCandle(domain.MarketCandle{
			Exchange: "MEXC", Symbol: symbol, MarketType: domain.MarketTypeSpot,
			OpenTime:    time.Unix(decoded.WindowStart, 0),
			CloseTime:   time.Unix(decoded.WindowEnd, 0),
			QuoteVolume: volume, EventTime: eventTime,
		}); err != nil {
			return fmt.Errorf("update MEXC spot candle %s: %w", symbol, err)
		}
	}
}

func (a *Adapter) handleMEXCFuturesCandle(sink domain.CandleSink, r mexcCandleResponse) error {
	symbol := r.Data.Symbol
	start := r.Data.T
	raw := r.Data.Amount
	if len(raw) == 0 {
		raw = r.Data.Q
	}
	if symbol == "" || start == 0 || len(raw) == 0 {
		return nil
	}
	var volume decimal.Decimal
	if err := json.Unmarshal(raw, &volume); err != nil {
		return fmt.Errorf("parse MEXC futures candle %s volume: %w", symbol, err)
	}
	if volume.IsNegative() {
		return fmt.Errorf("MEXC futures candle %s volume is negative: %s", symbol, volume.String())
	}
	eventTime := time.UnixMilli(r.Ts)
	if r.Ts == 0 {
		eventTime = time.Now()
	}
	symbol = strings.NewReplacer("_", "", "-", "").Replace(symbol)
	if err := sink.UpdateCandle(domain.MarketCandle{
		Exchange: "MEXC", Symbol: symbol, MarketType: domain.MarketTypeFutures,
		OpenTime: time.Unix(start, 0), CloseTime: time.Unix(start, 0).Add(time.Minute - time.Millisecond),
		QuoteVolume: volume, EventTime: eventTime,
	}); err != nil {
		return fmt.Errorf("update MEXC futures candle %s: %w", symbol, err)
	}
	return nil
}

func (a *Adapter) candleKeepAlive(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn, spot bool) {
	interval := pingInterval
	method := futuresPingMsg
	if spot {
		method = spotPingMsg
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.writeMessage(conn, websocket.TextMessage, method); err != nil {
				wrapped := fmt.Errorf("MEXC candle keepalive: %w", err)
				log.Printf("⚠️  %v", wrapped)
				cancel()
				return
			}
		}
	}
}
