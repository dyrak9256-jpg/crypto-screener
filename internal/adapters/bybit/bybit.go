package bybit

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
	spotWS    = "wss://stream.bybit.com/v5/public/spot"
	futuresWS = "wss://stream.bybit.com/v5/public/linear" // USDT Perpetual

	handshakeTimeout = 10 * time.Second
	pingInterval     = 20 * time.Second // Bybit требует ping каждые 20 сек
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var dialer = websocket.Dialer{HandshakeTimeout: handshakeTimeout}

// Bybit требует явной подписки на топики
// tickers.BTCUSDT — но нам нужны ВСЕ символы
// Bybit не имеет all-tickers stream, используем REST snapshot + WS updates
// Однако есть tickers топик без символа — получаем все через wildcard

type subscribeMsg struct {
	Op   string   `json:"op"`
	Args []string `json:"args"`
}

// Bybit V5 ticker response
type wsResponse struct {
	Topic string        `json:"topic"`
	Type  string        `json:"type"` // "snapshot" | "delta"
	Data  tickerPayload `json:"data"`
}

type tickerPayload struct {
	Symbol   string `json:"symbol"`
	Bid1     string `json:"bid1Price"`   // BestBid
	Ask1     string `json:"ask1Price"`   // BestAsk
	Volume   string `json:"volume24h"`   // Base volume
	TurnOver string `json:"turnover24h"` // Quote volume
}

// Bybit шлёт ping/pong в виде JSON
type pingMsg struct {
	Op string `json:"op"`
}

type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	go a.listen(ctx, spotWS, domain.MarketTypeSpot, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	go a.listen(ctx, futuresWS, domain.MarketTypeFutures, out)
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
	defer conn.Close()

	log.Printf("✅ Bybit %s connected", mType)

	// Подписываемся на все тикеры через wildcard
	// Bybit поддерживает: "tickers.*"
	sub := subscribeMsg{
		Op:   "subscribe",
		Args: []string{"tickers.*"},
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

	// Bybit использует JSON ping {"op":"ping"}, не WS ping frames
	go bybitKeepAlive(connCtx, conn)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		var resp wsResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil {
			continue
		}

		// Пропускаем служебные сообщения (op responses, pong)
		if resp.Topic == "" {
			continue
		}

		tick, ok := toMarketTick(&resp.Data, mType)
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

func toMarketTick(p *tickerPayload, mType domain.MarketType) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(p.Bid1)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(p.Ask1)
	if err != nil || ask.IsZero() {
		return domain.MarketTick{}, false
	}

	qVol, _ := decimal.NewFromString(p.TurnOver)

	return domain.MarketTick{
		Exchange:    "BYBIT",
		Symbol:      p.Symbol,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		Timestamp:   time.Now(),
	}, true
}

// Bybit ожидает JSON ping, не WS control frame
func bybitKeepAlive(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	ping, _ := sonic.Marshal(pingMsg{Op: "ping"})

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := conn.WriteMessage(websocket.TextMessage, ping); err != nil {
				log.Printf("⚠️  Bybit ping error: %v", err)
				return
			}
		}
	}
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}
