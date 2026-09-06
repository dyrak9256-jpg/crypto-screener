package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"crypto-screener/internal/domain"

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
	dialer  = websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	pingMsg = []byte("ping")
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
	BidPx     string `json:"bidPx"`
	AskPx     string `json:"askPx"`
	VolCcy24h string `json:"volCcy24h"`
	Ts        int64  `json:"ts,string"`
}

type Adapter struct {
	droppedTicks atomic.Uint64
}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, "SPOT", domain.MarketTypeSpot, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, "SWAP", domain.MarketTypeFutures, out)
	return nil
}

func (a *Adapter) listen(
	ctx context.Context,
	instType string,
	mType domain.MarketType,
	out chan<- domain.MarketTick,
) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := a.connectAndRead(ctx, instType, mType, out); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  OKX %s WS: %v — reconnecting in %s", mType, err, reconnectDelay)
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

	sub := subscribeMsg{
		Op: "subscribe",
		Args: []argItem{
			{Channel: "tickers", InstType: instType},
		},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
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
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		// OKX pong приходит как текст — не Control Frame
		if string(msg) == "pong" {
			continue
		}

		var resp wsResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil {
			continue
		}

		// Пропускаем служебные: ack подписки, ошибки
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
			select {
			case out <- tick:
			case <-connCtx.Done():
				return nil
			default:
				if n := a.droppedTicks.Add(1); n%1000 == 0 {
					log.Printf("⚠️  OKX %s: dropped %d ticks (channel full)", mType, n)
				}
			}
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
	if err != nil {
		return domain.MarketTick{}, false
	}

	return domain.MarketTick{
		Exchange:    "OKX",
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
				log.Printf("⚠️  OKX ping error: %v", err)
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
	for ctx.Err() == nil {
		symbols, err := okxSymbols(ctx, instType)
		if err != nil {
			log.Printf("⚠️ OKX %s candle symbols: %v", market, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}
		if err := a.readCandleShard(ctx, sink, instType, symbols, market); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ OKX %s candle WS: %v", market, err)
		}
	}
}

func okxSymbols(ctx context.Context, instType string) ([]string, error) {
	u := "https://www.okx.com/api/v5/public/instruments?instType=" + instType
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	var x okxInstrumentResponse
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return nil, err
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
		return err
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	go closeOnCtx(ctx, conn)
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
		var r okxCandleResponse
		if sonic.Unmarshal(msg, &r) != nil || len(r.Data) == 0 || r.Arg.InstID == "" {
			continue
		}
		for _, d := range r.Data {
			if len(d) < 7 {
				continue
			}
			start, err := strconv.ParseInt(d[0], 10, 64)
			if err != nil {
				continue
			}
			q, err := decimal.NewFromString(d[6])
			if err != nil || !q.IsPositive() {
				continue
			}
			et := time.UnixMilli(r.Ts)
			sink.UpdateCandle(domain.MarketCandle{Exchange: "OKX", Symbol: strings.ReplaceAll(r.Arg.InstID, "-", ""), MarketType: market, OpenTime: time.UnixMilli(start), CloseTime: time.UnixMilli(start).Add(time.Minute - time.Millisecond), QuoteVolume: q, EventTime: et, Closed: false})
		}
	}
}
