package mexc

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
	spotWS    = "wss://wbs.mexc.com/ws"
	futuresWS = "wss://contract.mexc.com/edge"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 15 * time.Second
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var dialer = websocket.Dialer{HandshakeTimeout: handshakeTimeout}

type subscribeMsg struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
}

// MEXC Spot ticker
type spotResponse struct {
	Channel string         `json:"c"` // "spot@public.miniTicker.v3.api@BTCUSDT"
	Data    spotTickerData `json:"d"`
}

type spotTickerData struct {
	Symbol  string `json:"s"`
	Bid     string `json:"b"`  // BestBid
	Ask     string `json:"a"`  // BestAsk
	QVolume string `json:"qv"` // Quote volume
}

// MEXC Futures ticker
type futuresResponse struct {
	Channel string            `json:"channel"`
	Data    futuresTickerData `json:"data"`
}

type futuresTickerData struct {
	Symbol string  `json:"symbol"`
	Bid1   float64 `json:"bid1"`
	Ask1   float64 `json:"ask1"`
	Volume float64 `json:"volume24"` // Quote volume
}

type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	go a.listenSpot(ctx, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	go a.listenFutures(ctx, out)
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
			log.Printf("⚠️  MEXC Spot WS: %v — reconnecting", err)
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
			log.Printf("⚠️  MEXC Futures WS: %v — reconnecting", err)
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
	defer conn.Close()

	log.Printf("✅ MEXC Spot connected")

	// MEXC: подписка на все mini-tickers
	sub := subscribeMsg{
		Method: "SUBSCRIPTION",
		Params: []string{"spot@public.miniTickers.v3.api"},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)
	_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))
	go mexcKeepAlive(connCtx, conn)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		// MEXC шлёт массив тикеров в поле data
		var raw struct {
			Data []spotTickerData `json:"d"`
		}
		if err := sonic.Unmarshal(msg, &raw); err != nil {
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
			}
		}
	}
}

func (a *Adapter) connectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	conn, _, err := dialer.DialContext(ctx, futuresWS, nil)
	if err != nil {
		return fmt.Errorf("dial futures: %w", err)
	}
	defer conn.Close()

	log.Printf("✅ MEXC Futures connected")

	sub := subscribeMsg{
		Method: "sub.ticker",
		Params: []string{"all"},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)
	_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))
	go mexcKeepAlive(connCtx, conn)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read futures: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

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
		}
	}
}

func spotToTick(d *spotTickerData, ts time.Time) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(d.Bid)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}
	ask, err := decimal.NewFromString(d.Ask)
	if err != nil || ask.IsZero() {
		return domain.MarketTick{}, false
	}
	qVol, _ := decimal.NewFromString(d.QVolume)

	return domain.MarketTick{
		Exchange:    "MEXC",
		Symbol:      d.Symbol,
		MarketType:  domain.MarketTypeSpot,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		Timestamp:   ts,
	}, true
}

func futuresToTick(d *futuresTickerData) (domain.MarketTick, bool) {
	if d.Bid1 == 0 || d.Ask1 == 0 {
		return domain.MarketTick{}, false
	}

	return domain.MarketTick{
		Exchange:    "MEXC",
		Symbol:      d.Symbol,
		MarketType:  domain.MarketTypeFutures,
		BestBid:     decimal.NewFromFloat(d.Bid1),
		BestAsk:     decimal.NewFromFloat(d.Ask1),
		QuoteVolume: decimal.NewFromFloat(d.Volume),
		Timestamp:   time.Now(),
	}, true
}

func mexcKeepAlive(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	ping, _ := sonic.Marshal(map[string]string{"method": "PING"})

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := conn.WriteMessage(websocket.TextMessage, ping); err != nil {
				log.Printf("⚠️  MEXC ping error: %v", err)
				return
			}
		}
	}
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}
