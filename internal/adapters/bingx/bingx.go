package bingx

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto-screener/internal/domain"
	"crypto-screener/internal/ingress"
	"crypto-screener/internal/retry"
	"crypto-screener/internal/wsutil"
	"encoding/json"
	"fmt"
	"io"
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
	spotWS    = "wss://open-api-ws.bingx.com/market"
	futuresWS = "wss://open-api-swap.bingx.com/swap-market"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 5 * time.Second
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var (
	dialer     = websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	httpClient = &http.Client{Timeout: 10 * time.Second}
)

// ✅ Pool для переиспользования gzip.Reader
// gzip.NewReader аллоцирует буферы при каждом вызове
// При сотнях сообщений/сек это создаёт серьёзное давление на GC
var gzipReaderPool = sync.Pool{
	New: func() any {
		// Создаём reader с пустым источником
		// Reset() будет вызван перед каждым использованием
		return new(gzip.Reader)
	},
}

type subscribeMsg struct {
	ID       string `json:"id"`
	ReqType  string `json:"reqType"`
	DataType string `json:"dataType"`
}

// BingX Spot и Futures используют разные имена полей
type tickerData struct {
	Symbol     string `json:"s"`          // "BTC-USDT" (Spot)
	BidPr      string `json:"b"`          // BestBid (Spot)
	AskPr      string `json:"a"`          // BestAsk (Spot)
	BidPrice   string `json:"bidPrice"`   // BestBid (Futures)
	AskPrice   string `json:"askPrice"`   // BestAsk (Futures)
	TradePrice string `json:"tradePrice"` // Last price (Futures fallback)
	QVolume    string `json:"q"`          // Quote volume
}

type Adapter struct {
	writeMu sync.Mutex
}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	if sink == nil {
		return fmt.Errorf("BingX funding sink is nil")
	}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		if err := a.pollFunding(ctx, sink); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ BingX funding poll: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
func (a *Adapter) pollFunding(ctx context.Context, sink domain.FundingSink) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://open-api.bingx.com/openApi/swap/v2/quote/fundingRate", nil)
	if err != nil {
		return fmt.Errorf("pollFunding: %w", err)
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("pollFunding: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s", r.Status)
	}
	var x struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return fmt.Errorf("pollFunding: %w", err)
	}
	if x.Code != 0 {
		return fmt.Errorf("API code %d", x.Code)
	}
	type row struct {
		Symbol          string `json:"symbol"`
		FundingRate     string `json:"fundingRate"`
		NextFundingTime int64  `json:"nextFundingTime"`
	}
	var rows []row
	trim := bytes.TrimSpace(x.Data)
	if len(trim) > 0 && trim[0] == '[' {
		if err := json.Unmarshal(trim, &rows); err != nil {
			return fmt.Errorf("pollFunding: %w", err)
		}
	} else {
		var one row
		if err := json.Unmarshal(trim, &one); err != nil {
			return fmt.Errorf("pollFunding: %w", err)
		}
		rows = []row{one}
	}
	now := time.Now()
	for _, v := range rows {
		rate, err := decimal.NewFromString(v.FundingRate)
		if err != nil {
			continue
		}
		sym := strings.ReplaceAll(v.Symbol, "-", "")
		if err := sink.UpdateFunding("BINGX", sym, rate, time.UnixMilli(v.NextFundingTime), now); err != nil {
			return fmt.Errorf("update BingX funding %s: %w", sym, err)
		}
	}
	return nil
}

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, spotWS, domain.MarketTypeSpot, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, futuresWS, domain.MarketTypeFutures, out)
	return nil
}

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
			log.Printf("⚠️  BingX %s WS: %v — reconnecting in %s", mType, err, reconnectDelay)
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

	log.Printf("✅ BingX %s connected", mType)

	dataType := "spot.tickers"
	if mType == domain.MarketTypeFutures {
		dataType = "swap.tickers"
	}

	sub := subscribeMsg{
		ID:       fmt.Sprintf("sub-%d", time.Now().UnixNano()),
		ReqType:  "sub",
		DataType: dataType,
	}
	if err := a.writeJSON(conn, sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	// Дедлайн: если сервер молчит дольше чем pingInterval+pongWait
	// значит что-то пошло не так (соединение зависло)
	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	// ✅ Нет keepAlive горутины — BingX сам инициирует Ping
	// Нам нужно только отвечать на серверный Ping

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}

		// Сбрасываем дедлайн при каждом сообщении
		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("refresh BingX read deadline: %w", err)
		}

		// Декомпрессия gzip
		data, err := decompressGzip(msg)
		if err != nil {
			// Не gzip — используем как есть (служебные фреймы)
			data = msg
		}

		strData := string(data)

		// ✅ Критично: отвечаем на серверный Ping
		// BingX разрывает соединение если не получает Pong
		if strData == "Ping" || strings.Contains(strData, `"ping"`) {
			if err := a.writeMessage(conn, websocket.TextMessage, []byte("Pong")); err != nil {
				return fmt.Errorf("write pong: %w", err)
			}
			continue
		}

		var raw struct {
			DataType string       `json:"dataType"`
			Data     []tickerData `json:"data"`
		}
		if err := sonic.Unmarshal(data, &raw); err != nil {
			continue
		}

		if len(raw.Data) == 0 {
			continue
		}

		now := time.Now()
		for i := range raw.Data {
			tick, ok := toMarketTick(&raw.Data[i], mType, now)
			if !ok {
				continue
			}
			ingress.Submit(out, tick)
		}
	}
}

func toMarketTick(d *tickerData, mType domain.MarketType, ts time.Time) (domain.MarketTick, bool) {
	// Spot: b / a; Futures: bidPrice / askPrice.
	// Last-trade price is never substituted for BBO because that creates a
	// non-executable arbitrage quote.
	bidStr := d.BidPr
	askStr := d.AskPr
	if mType == domain.MarketTypeFutures {
		bidStr = d.BidPrice
		askStr = d.AskPrice
	}

	bid, err := decimal.NewFromString(bidStr)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(askStr)
	if err != nil || ask.IsZero() || bid.GreaterThan(ask) {
		return domain.MarketTick{}, false
	}

	qVol, err := decimal.NewFromString(d.QVolume)
	if err != nil {
		return domain.MarketTick{}, false
	}

	// ✅ Нормализация: "BTC-USDT" → "BTCUSDT"
	symbol := strings.ReplaceAll(d.Symbol, "-", "")
	if !strings.HasSuffix(strings.ToUpper(symbol), "USDT") {
		return domain.MarketTick{}, false
	}

	return domain.MarketTick{
		Exchange:    "BINGX",
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

// firstNonEmpty возвращает первую непустую строку из списка
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// ✅ Исправленный порядок: Close → Put (не Put → Close)
func decompressGzip(data []byte) ([]byte, error) {
	gr := gzipReaderPool.Get().(*gzip.Reader)

	if err := gr.Reset(bytes.NewReader(data)); err != nil {
		gzipReaderPool.Put(gr)
		return nil, fmt.Errorf("decompressGzip: %w", err)
	}

	result, err := io.ReadAll(io.LimitReader(gr, 4<<20))

	// ✅ Сначала Close, потом Put обратно в пул
	// Иначе: объект уже в пуле, но defer gr.Close() его модифицирует
	gr.Close()
	gzipReaderPool.Put(gr)

	return result, err
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

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}

func (a *Adapter) ConnectCandles(ctx context.Context, sink domain.CandleSink) error {
	go a.runCandleFeed(ctx, sink, true)
	go a.runCandleFeed(ctx, sink, false)
	<-ctx.Done()
	return nil
}

type bingxCandleMessage struct {
	Code     int    `json:"code"`
	Msg      string `json:"msg"`
	DataType string `json:"dataType"`
	Data     struct {
		EventTime int64  `json:"E"`
		Symbol    string `json:"s"`
		K         struct {
			Start int64  `json:"t"`
			End   int64  `json:"T"`
			Quote string `json:"q"`
		} `json:"K"`
	} `json:"data"`
}
type bingxSpotSymbolsResponse struct {
	Code int `json:"code"`
	Data []struct {
		Symbol string `json:"symbol"`
		Status string `json:"status"`
	} `json:"data"`
}
type bingxSwapContractsResponse struct {
	Code int `json:"code"`
	Data []struct {
		Symbol string `json:"symbol"`
		Status int    `json:"status"`
	} `json:"data"`
}

func (a *Adapter) runCandleFeed(ctx context.Context, sink domain.CandleSink, spot bool) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		symbols, err := bingxCandleSymbols(ctx, spot)
		if err != nil {
			log.Printf("⚠️ BingX candle symbols: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-retry.After(reconnectDelay):
			}
			continue
		}
		var wg sync.WaitGroup
		for i := 0; i < len(symbols); i += 50 {
			end := i + 50
			if end > len(symbols) {
				end = len(symbols)
			}
			part := append([]string(nil), symbols[i:end]...)
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.runBingXCandleShard(ctx, sink, part, spot)
			}()
		}
		wg.Wait()
		if ctx.Err() == nil && !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func bingxCandleSymbols(ctx context.Context, spot bool) ([]string, error) {
	url := "https://open-api.bingx.com/openApi/spot/v1/common/symbols"
	if !spot {
		url = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("bingxCandleSymbols: %w", err)
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bingxCandleSymbols: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return nil, fmt.Errorf("bingx candle symbols HTTP %s", r.Status)
	}
	if spot {
		var x bingxSpotSymbolsResponse
		if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
			return nil, fmt.Errorf("bingxCandleSymbols: %w", err)
		}
		if x.Code != 0 {
			return nil, fmt.Errorf("bingx candle symbols API code %d", x.Code)
		}
		out := make([]string, 0, len(x.Data))
		for _, v := range x.Data {
			if v.Status == "1" || strings.EqualFold(v.Status, "trading") {
				out = append(out, v.Symbol)
			}
		}
		return out, nil
	}
	var x bingxSwapContractsResponse
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return nil, fmt.Errorf("bingxCandleSymbols: %w", err)
	}
	if x.Code != 0 {
		return nil, fmt.Errorf("bingx candle symbols API code %d", x.Code)
	}
	out := make([]string, 0, len(x.Data))
	for _, v := range x.Data {
		if v.Status == 1 {
			out = append(out, v.Symbol)
		}
	}
	return out, nil
}

func (a *Adapter) runBingXCandleShard(ctx context.Context, sink domain.CandleSink, symbols []string, spot bool) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		if err := a.readCandleShard(ctx, sink, symbols, spot); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ BingX candle WS: %v", err)
		}
		if ctx.Err() == nil && !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) readCandleShard(ctx context.Context, sink domain.CandleSink, symbols []string, spot bool) error {
	url := spotWS
	if !spot {
		url = futuresWS
	}
	conn, _, err := dialer.DialContext(ctx, url, nil)
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
	for _, sym := range symbols {
		interval := "1min"
		if !spot {
			interval = "1m"
		}
		if err := a.writeJSON(conn, subscribeMsg{ID: fmt.Sprintf("candle-%d", time.Now().UnixNano()), ReqType: "sub", DataType: sym + "@kline_" + interval}); err != nil {
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
		raw, err := decompressGzip(msg)
		if err != nil {
			raw = msg
		}
		var r bingxCandleMessage
		if err := sonic.Unmarshal(raw, &r); err != nil {
			continue
		}
		if r.Code != 0 {
			return fmt.Errorf("BingX candle subscription rejected: code=%d msg=%q", r.Code, r.Msg)
		}
		if r.Data.Symbol == "" {
			continue
		}
		q, err := decimal.NewFromString(r.Data.K.Quote)
		if err != nil || q.IsNegative() {
			continue
		}
		et := time.UnixMilli(r.Data.EventTime)
		if err := sink.UpdateCandle(domain.MarketCandle{Exchange: "BINGX", Symbol: strings.ReplaceAll(r.Data.Symbol, "-", ""), MarketType: func() domain.MarketType {
			if spot {
				return domain.MarketTypeSpot
			}
			return domain.MarketTypeFutures
		}(), OpenTime: time.UnixMilli(r.Data.K.Start), CloseTime: time.UnixMilli(r.Data.K.End), QuoteVolume: q, EventTime: et}); err != nil {
			return fmt.Errorf("readCandleShard: %w", err)
		}
	}
}
