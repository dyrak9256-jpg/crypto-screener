package okx

import (
	"context"
	"crypto-screener/internal/domain"
	"fmt"
	"log"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	// OKX использует один endpoint для всего
	wsURL = "wss://ws.okx.com:8443/ws/v5/public"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 25 * time.Second // OKX требует ping каждые 30 сек
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var dialer = websocket.Dialer{HandshakeTimeout: handshakeTimeout}

type subscribeMsg struct {
	Op   string    `json:"op"`
	Args []argItem `json:"args"`
}

type argItem struct {
	Channel  string `json:"channel"`
	InstID   string `json:"instId,omitempty"`
	InstType string `json:"instType,omitempty"`
}

// OKX тикер ответ
type wsResponse struct {
	Arg  argItem      `json:"arg"`
	Data []tickerData `json:"data"`
}

type tickerData struct {
	InstID    string `json:"instId"`    // "BTC-USDT"
	BidPx     string `json:"bidPx"`     // BestBid
	AskPx     string `json:"askPx"`     // BestAsk
	VolCcy24h string `json:"volCcy24h"` // Quote volume 24h
}

type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	go a.listen(ctx, "SPOT", domain.MarketTypeSpot, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	go a.listen(ctx, "SWAP", domain.MarketTypeFutures, out) // SWAP = perpetual futures
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
	defer conn.Close()

	log.Printf("✅ OKX %s connected", mType)

	// OKX подписка на все тикеры по типу инструмента
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

	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))
	})
	_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

	// OKX использует текстовый "ping", ответ "pong"
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

		// OKX шлёт "pong" как текст
		if string(msg) == "pong" {
			continue
		}

		var resp wsResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil {
			continue
		}

		now := time.Now()
		for i := range resp.Data {
			tick, ok := toMarketTick(&resp.Data[i], mType, now)
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
	bid, err := decimal.NewFromString(d.BidPx)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(d.AskPx)
	if err != nil || ask.IsZero() {
		return domain.MarketTick{}, false
	}

	qVol, _ := decimal.NewFromString(d.VolCcy24h)

	// OKX символ "BTC-USDT" → унифицируем в "BTCUSDT"
	symbol := normalizeSymbol(d.InstID)

	return domain.MarketTick{
		Exchange:    "OKX",
		Symbol:      symbol,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		Timestamp:   ts,
	}, true
}

// "BTC-USDT" → "BTCUSDT", "BTC-USDT-SWAP" → "BTCUSDT"
func normalizeSymbol(instID string) string {
	result := make([]byte, 0, len(instID))
	for i := 0; i < len(instID); i++ {
		if instID[i] != '-' {
			result = append(result, instID[i])
		}
	}
	// Убираем суффикс SWAP если есть
	s := string(result)
	if len(s) > 4 && s[len(s)-4:] == "SWAP" {
		s = s[:len(s)-4]
	}
	return s
}

func okxKeepAlive(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
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
