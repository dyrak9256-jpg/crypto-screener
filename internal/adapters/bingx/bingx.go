package bingx

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto-screener/internal/domain"
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

var dialer = websocket.Dialer{HandshakeTimeout: handshakeTimeout}

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
	for {
		if ctx.Err() != nil {
			return
		}
		if err := a.connectAndRead(ctx, url, mType, out); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  BingX %s WS: %v — reconnecting in %s", mType, err, reconnectDelay)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectDelay):
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
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

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
			select {
			case out <- tick:
			case <-connCtx.Done():
				return nil
			default:
			}
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

	return domain.MarketTick{
		Exchange:    "BINGX",
		Symbol:      symbol,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		EventTime:   ts,
		ReceivedAt:  ts,
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
		return nil, err
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
	return conn.WriteMessage(messageType, data)
}

func (a *Adapter) writeJSON(conn *websocket.Conn, v any) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return conn.WriteJSON(v)
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
	for ctx.Err() == nil {
		symbols, err := bingxCandleSymbols(ctx, spot)
		if err != nil {
			log.Printf("⚠️ BingX candle symbols: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}
		for i := 0; i < len(symbols); i += 50 {
			end := i + 50
			if end > len(symbols) {
				end = len(symbols)
			}
			part := append([]string(nil), symbols[i:end]...)
			if err := a.readCandleShard(ctx, sink, part, spot); err != nil && ctx.Err() == nil {
				log.Printf("⚠️ BingX candle WS: %v", err)
			}
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
		return nil, err
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if spot {
		var x bingxSpotSymbolsResponse
		if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
			return nil, err
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
		return nil, err
	}
	out := make([]string, 0, len(x.Data))
	for _, v := range x.Data {
		if v.Status == 1 {
			out = append(out, v.Symbol)
		}
	}
	return out, nil
}

func (a *Adapter) readCandleShard(ctx context.Context, sink domain.CandleSink, symbols []string, spot bool) error {
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
	for _, sym := range symbols {
		interval := "1min"
		if !spot {
			interval = "1m"
		}
		if err := a.writeJSON(conn, subscribeMsg{ID: fmt.Sprintf("candle-%d", time.Now().UnixNano()), ReqType: "sub", DataType: sym + "@kline_" + interval}); err != nil {
			return err
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
		raw, err := decompressGzip(msg)
		if err != nil {
			raw = msg
		}
		var r bingxCandleMessage
		if sonic.Unmarshal(raw, &r) != nil || r.Data.Symbol == "" {
			continue
		}
		q, err := decimal.NewFromString(r.Data.K.Quote)
		if err != nil || !q.IsPositive() {
			continue
		}
		et := time.UnixMilli(r.Data.EventTime)
		sink.UpdateCandle(domain.MarketCandle{Exchange: "BINGX", Symbol: strings.ReplaceAll(r.Data.Symbol, "-", ""), MarketType: func() domain.MarketType {
			if spot {
				return domain.MarketTypeSpot
			}
			return domain.MarketTypeFutures
		}(), OpenTime: time.UnixMilli(r.Data.K.Start), CloseTime: time.UnixMilli(r.Data.K.End), QuoteVolume: q, EventTime: et})
	}
}
