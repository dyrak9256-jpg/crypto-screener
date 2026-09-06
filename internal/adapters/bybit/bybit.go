package bybit

import (
	"context"
	"crypto-screener/internal/domain"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	spotWS    = "wss://stream.bybit.com/v5/public/spot"
	futuresWS = "wss://stream.bybit.com/v5/public/linear"

	spotSymbolsURL    = "https://api.bybit.com/v5/market/instruments-info?category=spot&status=Trading"
	futuresSymbolsURL = "https://api.bybit.com/v5/market/instruments-info?category=linear&status=Trading"

	handshakeTimeout    = 10 * time.Second
	pingInterval        = 20 * time.Second
	pongWait            = 10 * time.Second
	reconnectDelay      = 3 * time.Second
	spotSubBatchSize    = 10
	futuresSubBatchSize = 50
)

var dialer = websocket.Dialer{HandshakeTimeout: handshakeTimeout}

type subscribeMsg struct {
	Op   string   `json:"op"`
	Args []string `json:"args"`
}

type pingMsg struct {
	Op string `json:"op"`
}

// wsResponse: Data как RawMessage т.к. нужно накладывать delta
type wsResponse struct {
	Topic string          `json:"topic"`
	Type  string          `json:"type"` // "snapshot" | "delta"
	Ts    int64           `json:"ts"`
	Data  json.RawMessage `json:"data"`
}

// pongResponse: для обработки JSON pong от Bybit
type pongResponse struct {
	Op string `json:"op"` // "pong"
}

type tickerPayload struct {
	Symbol   string `json:"symbol"`
	Bid1     string `json:"bid1Price"`
	Ask1     string `json:"ask1Price"`
	TurnOver string `json:"turnover24h"`
}

type Adapter struct {
	client *http.Client
}

func NewAdapter() *Adapter {
	return &Adapter{
		client: &http.Client{Timeout: 15 * time.Second},
	}
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
	for {
		if ctx.Err() != nil {
			return
		}
		if err := a.connectAndRead(ctx, url, mType, out); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  Bybit %s WS: %v — reconnecting in %s", mType, err, reconnectDelay)
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

	log.Printf("✅ Bybit %s connected", mType)

	// Получаем символы через REST для подписки
	restURL := futuresSymbolsURL
	if mType == domain.MarketTypeSpot {
		restURL = spotSymbolsURL
	}

	symbols, err := a.fetchSymbols(ctx, restURL)
	if err != nil {
		return fmt.Errorf("fetch symbols: %w", err)
	}

	log.Printf("📋 Bybit %s: subscribing to %d symbols", mType, len(symbols))

	// Формируем топики
	args := make([]string, 0, len(symbols))
	for _, s := range symbols {
		args = append(args, "tickers."+s)
	}

	// Отправляем подписку батчами по 50
	batchSize := futuresSubBatchSize
	if mType == domain.MarketTypeSpot {
		batchSize = spotSubBatchSize
	}
	for i := 0; i < len(args); i += batchSize {
		end := i + batchSize
		if end > len(args) {
			end = len(args)
		}
		sub := subscribeMsg{Op: "subscribe", Args: args[i:end]}
		if err := conn.WriteJSON(sub); err != nil {
			return fmt.Errorf("subscribe batch [%d:%d]: %w", i, end, err)
		}
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	// ✅ НЕ ставим SetPongHandler — Bybit шлёт JSON pong через ReadMessage
	// Дедлайн сбрасываем при каждом сообщении в цикле
	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	go bybitKeepAlive(connCtx, conn)

	// ✅ Локальный кэш: накладываем delta на snapshot
	cache := make(map[string]*tickerPayload, len(symbols))

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}

		// Сбрасываем дедлайн при любом входящем сообщении (включая pong)
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		var resp wsResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil {
			continue
		}

		// Пропускаем служебные сообщения: pong, ack подписки
		if resp.Topic == "" {
			continue
		}

		var delta tickerPayload
		if err := sonic.Unmarshal(resp.Data, &delta); err != nil {
			continue
		}

		if delta.Symbol == "" {
			continue
		}

		// ✅ Merge delta в кэш
		current, exists := cache[delta.Symbol]
		if !exists {
			current = &tickerPayload{Symbol: delta.Symbol}
			cache[delta.Symbol] = current
		}

		// Обновляем только непустые поля (delta может содержать только часть)
		if delta.Bid1 != "" {
			current.Bid1 = delta.Bid1
		}
		if delta.Ask1 != "" {
			current.Ask1 = delta.Ask1
		}
		if delta.TurnOver != "" {
			current.TurnOver = delta.TurnOver
		}

		// После первого snapshot у нас есть оба значения
		eventTime := time.Now()
		if resp.Ts > 0 {
			eventTime = time.UnixMilli(resp.Ts)
		}
		tick, ok := toMarketTick(current, mType, eventTime)
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

// fetchSymbols получает список торгующихся символов через REST
func (a *Adapter) fetchSymbols(ctx context.Context, url string) ([]string, error) {
	var symbols []string
	cursor := ""
	for {
		reqURL := url
		if cursor != "" {
			reqURL += "&cursor=" + url.QueryEscape(cursor)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}
		resp, err := a.client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("bybit REST status %s", resp.Status)
		}
		var parsed struct {
			RetCode int    `json:"retCode"`
			RetMsg  string `json:"retMsg"`
			Result  struct {
				List []struct {
					Symbol string `json:"symbol"`
					Status string `json:"status"`
				} `json:"list"`
				NextPageCursor string `json:"nextPageCursor"`
			} `json:"result"`
		}
		if err := sonic.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("parse response: %w", err)
		}
		if parsed.RetCode != 0 {
			return nil, fmt.Errorf("bybit API error %d: %s", parsed.RetCode, parsed.RetMsg)
		}
		for _, item := range parsed.Result.List {
			if item.Status == "Trading" {
				symbols = append(symbols, item.Symbol)
			}
		}
		if parsed.Result.NextPageCursor == "" || parsed.Result.NextPageCursor == cursor {
			break
		}
		cursor = parsed.Result.NextPageCursor
	}
	return symbols, nil
}

// ConnectCandles subscribes only to 1-minute klines. Higher timeframes are
// calculated locally from these minute buckets, so the exchange sends no
// duplicate 5m/15m/30m streams.
func (a *Adapter) ConnectCandles(ctx context.Context, sink domain.CandleSink) error {
	for _, market := range []domain.MarketType{domain.MarketTypeSpot, domain.MarketTypeFutures} {
		go a.runCandleFeed(ctx, sink, market)
	}
	<-ctx.Done()
	return nil
}

type bybitCandleResponse struct {
	Topic string `json:"topic"`
	Ts    int64  `json:"ts"`
	Data  []struct {
		Start     int64  `json:"start"`
		End       int64  `json:"end"`
		Turnover  string `json:"turnover"`
		Confirm   bool   `json:"confirm"`
		Timestamp int64  `json:"timestamp"`
	} `json:"data"`
}

func (a *Adapter) runCandleFeed(ctx context.Context, sink domain.CandleSink, market domain.MarketType) {
	url := futuresWS
	rest := futuresSymbolsURL
	batch := futuresSubBatchSize
	if market == domain.MarketTypeSpot {
		url = spotWS
		rest = spotSymbolsURL
		batch = spotSubBatchSize
	}
	for ctx.Err() == nil {
		symbols, err := a.fetchSymbols(ctx, rest)
		if err != nil {
			log.Printf("⚠️ Bybit %s candle symbols: %v", market, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}
		if err := a.readCandleShard(ctx, sink, url, symbols, market, batch); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ Bybit %s candle WS: %v", market, err)
		}
	}
}

func (a *Adapter) readCandleShard(ctx context.Context, sink domain.CandleSink, url string, symbols []string, market domain.MarketType, batch int) error {
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	go closeOnCtx(ctx, conn)
	args := make([]string, 0, len(symbols))
	for _, s := range symbols {
		args = append(args, "kline.1."+s)
	}
	for i := 0; i < len(args); i += batch {
		end := i + batch
		if end > len(args) {
			end = len(args)
		}
		if err := conn.WriteJSON(subscribeMsg{Op: "subscribe", Args: args[i:end]}); err != nil {
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
		var r bybitCandleResponse
		if sonic.Unmarshal(msg, &r) != nil || len(r.Data) == 0 || r.Topic == "" {
			continue
		}
		parts := strings.Split(r.Topic, ".")
		if len(parts) != 3 {
			continue
		}
		symbol := parts[2]
		for _, d := range r.Data {
			q, err := decimal.NewFromString(d.Turnover)
			if err != nil || !q.IsPositive() {
				continue
			}
			et := time.UnixMilli(d.Timestamp)
			if d.Timestamp == 0 {
				et = time.UnixMilli(r.Ts)
			}
			sink.UpdateCandle(domain.MarketCandle{Exchange: "BYBIT", Symbol: symbol, MarketType: market, OpenTime: time.UnixMilli(d.Start), CloseTime: time.UnixMilli(d.End), QuoteVolume: q, EventTime: et, Closed: d.Confirm})
		}
	}
}
