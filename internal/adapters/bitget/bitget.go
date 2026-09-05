package bitget

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
	wsURL = "wss://ws.bitget.com/v2/ws/public"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 25 * time.Second
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var dialer = websocket.Dialer{HandshakeTimeout: handshakeTimeout}

type subscribeMsg struct {
	Op   string    `json:"op"`
	Args []argItem `json:"args"`
}

type argItem struct {
	InstType string `json:"instType"`
	Channel  string `json:"channel"`
	InstID   string `json:"instId"`
}

type wsResponse struct {
	Action string       `json:"action"` // "snapshot" | "update"
	Arg    argItem      `json:"arg"`
	Data   []tickerData `json:"data"`
}

type tickerData struct {
	InstID   string `json:"instId"`
	BidPr    string `json:"bidPr"`       // BestBid
	AskPr    string `json:"askPr"`       // BestAsk
	QuoteVol string `json:"quoteVolume"` // Quote volume 24h
}

type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	go a.listen(ctx, "SPOT", domain.MarketTypeSpot, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	go a.listen(ctx, "USDT-FUTURES", domain.MarketTypeFutures, out)
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
			log.Printf("⚠️  Bitget %s WS: %v — reconnecting in %s", mType, err, reconnectDelay)
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

	log.Printf("✅ Bitget %s connected", mType)

	// Bitget: instId "default" = все символы данного типа
	sub := subscribeMsg{
		Op: "subscribe",
		Args: []argItem{
			{InstType: instType, Channel: "ticker", InstID: "default"},
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

	go keepAlive(connCtx, conn, "BITGET")

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

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
	bid, err := decimal.NewFromString(d.BidPr)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(d.AskPr)
	if err != nil || ask.IsZero() {
		return domain.MarketTick{}, false
	}

	qVol, _ := decimal.NewFromString(d.QuoteVol)

	return domain.MarketTick{
		Exchange:    "BITGET",
		Symbol:      d.InstID,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		Timestamp:   ts,
	}, true
}

func keepAlive(ctx context.Context, conn *websocket.Conn, label string) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
				log.Printf("⚠️  %s ping error: %v", label, err)
				return
			}
		}
	}
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}
