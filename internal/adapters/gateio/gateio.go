package gateio

import (
	"context"
	"crypto-screener/internal/domain"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	spotWS            = "wss://api.gateio.ws/ws/v4/"
	futuresWS         = "wss://fx-ws.gateio.ws/v4/ws/usdt"
	spotSymbolsURL    = "https://api.gateio.ws/api/v4/spot/currency_pairs"
	futuresSymbolsURL = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
	symbolBatchSize   = 100

	handshakeTimeout = 10 * time.Second
	pingInterval     = 10 * time.Second
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var (
	dialer         = websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	symbolReplacer = strings.NewReplacer("_", "", "-", "")
)

type wsRequest struct {
	Time    int64    `json:"time"`
	Channel string   `json:"channel"`
	Event   string   `json:"event"`
	Payload []string `json:"payload,omitempty"`
}

// ✅ Spot и Futures имеют разные структуры ответа
type spotWsResponse struct {
	Channel string       `json:"channel"`
	Event   string       `json:"event"`  // "update" | "subscribe" | "pong"
	Result  []tickerData `json:"result"` // ✅ массив, не объект
}

type futuresWsResponse struct {
	Channel string              `json:"channel"`
	Event   string              `json:"event"`
	Result  []futuresTickerData `json:"result"`
}

// Gate.io Spot ticker fields
type tickerData struct {
	CurrencyPair string `json:"currency_pair"` // "BTC_USDT"
	HighestBid   string `json:"highest_bid"`
	LowestAsk    string `json:"lowest_ask"`
	QuoteVolume  string `json:"quote_volume"`
}

// Gate.io Futures ticker fields
// Поля подтверждены документацией Gate.io Futures WS V4
type futuresTickerData struct {
	Contract string `json:"contract"`          // "BTC_USDT"
	Bid1     string `json:"highest_bid"`       // highest bid price
	Ask1     string `json:"lowest_ask"`        // lowest ask price
	Volume   string `json:"volume_24h_settle"` // ✅ объём в USDT (расчётная валюта)
}

type Adapter struct {
	droppedTicks    atomic.Uint64
	symbolsMu       sync.Mutex
	spotSymbols     []string
	futuresSymbols  []string
	symbolsLoadedAt time.Time
}

func NewAdapter() *Adapter {
	return &Adapter{}
}

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
			log.Printf("⚠️  Gate.io Spot WS: %v — reconnecting", err)
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
			log.Printf("⚠️  Gate.io Futures WS: %v — reconnecting", err)
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
		return fmt.Errorf("dial: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	defer conn.Close()

	log.Printf("✅ Gate.io Spot connected")

	symbols, err := a.getSymbols(ctx, true)
	if err != nil {
		return fmt.Errorf("fetch spot symbols: %w", err)
	}
	for i := 0; i < len(symbols); i += symbolBatchSize {
		end := i + symbolBatchSize
		if end > len(symbols) {
			end = len(symbols)
		}
		sub := wsRequest{Time: time.Now().Unix(), Channel: "spot.tickers", Event: "subscribe", Payload: symbols[i:end]}
		if err := conn.WriteJSON(sub); err != nil {
			return fmt.Errorf("subscribe spot batch: %w", err)
		}
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	go gateKeepAlive(connCtx, conn, "spot.ping")

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		var resp spotWsResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil {
			continue
		}

		// ✅ Фильтруем: нужны только update от ticker канала
		// Пропускаем: pong, subscribe-ack и другие служебные сообщения
		if resp.Event != "update" || resp.Channel != "spot.tickers" {
			continue
		}

		if len(resp.Result) == 0 {
			continue
		}

		now := time.Now()
		for i := range resp.Result {
			tick, ok := spotTickerToTick(&resp.Result[i], now)
			if !ok {
				continue
			}
			select {
			case out <- tick:
			case <-connCtx.Done():
				return nil
			default:
				if n := a.droppedTicks.Add(1); n%1000 == 0 {
					log.Printf("⚠️  Gate.io Spot: dropped %d ticks (channel full)", n)
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

	log.Printf("✅ Gate.io Futures connected")

	symbols, err := a.getSymbols(ctx, false)
	if err != nil {
		return fmt.Errorf("fetch futures symbols: %w", err)
	}
	for i := 0; i < len(symbols); i += symbolBatchSize {
		end := i + symbolBatchSize
		if end > len(symbols) {
			end = len(symbols)
		}
		sub := wsRequest{Time: time.Now().Unix(), Channel: "futures.tickers", Event: "subscribe", Payload: symbols[i:end]}
		if err := conn.WriteJSON(sub); err != nil {
			return fmt.Errorf("subscribe futures batch: %w", err)
		}
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	go gateKeepAlive(connCtx, conn, "futures.ping")

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read futures: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		var resp futuresWsResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil {
			continue
		}

		// ✅ Фильтруем только futures ticker updates
		if resp.Event != "update" || resp.Channel != "futures.tickers" {
			continue
		}

		if len(resp.Result) == 0 {
			continue
		}

		now := time.Now()
		for i := range resp.Result {
			tick, ok := futuresTickerToTick(&resp.Result[i], now)
			if !ok {
				continue
			}
			select {
			case out <- tick:
			case <-connCtx.Done():
				return nil
			default:
				if n := a.droppedTicks.Add(1); n%1000 == 0 {
					log.Printf("⚠️  Gate.io Futures: dropped %d ticks (channel full)", n)
				}
			}
		}
	}
}

func spotTickerToTick(d *tickerData, ts time.Time) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(d.HighestBid)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(d.LowestAsk)
	if err != nil || ask.IsZero() || bid.GreaterThan(ask) {
		return domain.MarketTick{}, false
	}

	qVol, err := decimal.NewFromString(d.QuoteVolume)
	if err != nil {
		return domain.MarketTick{}, false
	}

	return domain.MarketTick{
		Exchange:    "GATEIO",
		Symbol:      symbolReplacer.Replace(d.CurrencyPair),
		MarketType:  domain.MarketTypeSpot,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		EventTime:   ts,
		ReceivedAt:  ts,
		Timestamp:   ts,
	}, true
}

func futuresTickerToTick(d *futuresTickerData, ts time.Time) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(d.Bid1)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(d.Ask1)
	if err != nil || ask.IsZero() || bid.GreaterThan(ask) {
		return domain.MarketTick{}, false
	}

	qVol, err := decimal.NewFromString(d.Volume)
	if err != nil {
		return domain.MarketTick{}, false
	}

	return domain.MarketTick{
		Exchange:    "GATEIO",
		Symbol:      symbolReplacer.Replace(d.Contract),
		MarketType:  domain.MarketTypeFutures,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		EventTime:   ts,
		ReceivedAt:  ts,
		Timestamp:   ts,
	}, true
}

func gateKeepAlive(ctx context.Context, conn *websocket.Conn, pingChannel string) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ping := wsRequest{
				Time:    time.Now().Unix(),
				Channel: pingChannel,
				Event:   "ping",
			}
			if err := conn.WriteJSON(ping); err != nil {
				log.Printf("⚠️  Gate.io ping error [%s]: %v", pingChannel, err)
				return
			}
		}
	}
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}

func (a *Adapter) getSymbols(ctx context.Context, spot bool) ([]string, error) {
	a.symbolsMu.Lock()
	defer a.symbolsMu.Unlock()
	if time.Since(a.symbolsLoadedAt) < 5*time.Minute {
		if spot && len(a.spotSymbols) > 0 {
			return append([]string(nil), a.spotSymbols...), nil
		}
		if !spot && len(a.futuresSymbols) > 0 {
			return append([]string(nil), a.futuresSymbols...), nil
		}
	}
	url := futuresSymbolsURL
	if spot {
		url = spotSymbolsURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("gate REST status %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var symbols []string
	if spot {
		var rows []struct {
			ID          string `json:"id"`
			Quote       string `json:"quote"`
			TradeStatus string `json:"trade_status"`
		}
		if err := sonic.Unmarshal(body, &rows); err != nil {
			return nil, err
		}
		for _, row := range rows {
			if row.Quote == "USDT" && (row.TradeStatus == "tradable" || row.TradeStatus == "trading" || row.TradeStatus == "") {
				symbols = append(symbols, row.ID)
			}
		}
	} else {
		var rows []struct {
			Name        string `json:"name"`
			Type        string `json:"type"`
			InDelisting bool   `json:"in_delisting"`
			Status      string `json:"status"`
		}
		if err := sonic.Unmarshal(body, &rows); err != nil {
			return nil, err
		}
		for _, row := range rows {
			if row.Type == "direct" && !row.InDelisting && (row.Status == "open" || row.Status == "trading" || row.Status == "") {
				symbols = append(symbols, row.Name)
			}
		}
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("no USDT symbols returned")
	}
	if spot {
		a.spotSymbols = symbols
	} else {
		a.futuresSymbols = symbols
	}
	a.symbolsLoadedAt = time.Now()
	return append([]string(nil), symbols...), nil
}

func (a *Adapter) ConnectCandles(ctx context.Context, sink domain.CandleSink) error {
	go a.runCandleFeed(ctx, sink, true)
	go a.runCandleFeed(ctx, sink, false)
	<-ctx.Done()
	return nil
}

type gateCandleResponse struct {
	Channel string `json:"channel"`
	Event   string `json:"event"`
	Result  []struct {
		T int64  `json:"t"`
		N string `json:"n"`
		A string `json:"a"`
		W bool   `json:"w"`
	} `json:"result"`
	TimeMS int64 `json:"time_ms"`
}

func (a *Adapter) runCandleFeed(ctx context.Context, sink domain.CandleSink, spot bool) {
	url := spotWS
	channel := "spot.candlesticks"
	market := domain.MarketTypeSpot
	if !spot {
		url = futuresWS
		channel = "futures.candlesticks"
		market = domain.MarketTypeFutures
	}
	for ctx.Err() == nil {
		symbols, err := a.getSymbols(ctx, spot)
		if err != nil {
			log.Printf("⚠️ Gate %s candle symbols: %v", market, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}
		if err := a.readCandleShard(ctx, sink, url, channel, symbols, market); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ Gate %s candle WS: %v", market, err)
		}
	}
}

func (a *Adapter) readCandleShard(ctx context.Context, sink domain.CandleSink, url, channel string, symbols []string, market domain.MarketType) error {
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	go closeOnCtx(ctx, conn)
	for _, sym := range symbols {
		if err := conn.WriteJSON(wsRequest{Time: time.Now().Unix(), Channel: channel, Event: "subscribe", Payload: []string{"1m", sym}}); err != nil {
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
		var r gateCandleResponse
		if sonic.Unmarshal(msg, &r) != nil || r.Event != "update" || r.Channel != channel {
			continue
		}
		for _, d := range r.Result {
			v, err := decimal.NewFromString(d.A)
			if err != nil || !v.IsPositive() {
				continue
			}
			parts := strings.SplitN(d.N, "_", 2)
			if len(parts) != 2 {
				continue
			}
			sym := parts[1]
			et := time.UnixMilli(r.TimeMS)
			if r.TimeMS == 0 {
				et = time.Now()
			}
			sink.UpdateCandle(domain.MarketCandle{Exchange: "GATEIO", Symbol: strings.ReplaceAll(sym, "_", ""), MarketType: market, OpenTime: time.Unix(d.T, 0), CloseTime: time.Unix(d.T, 0).Add(time.Minute - time.Millisecond), QuoteVolume: v, EventTime: et, Closed: d.W})
		}
	}
}
