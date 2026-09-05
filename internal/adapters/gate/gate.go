package gateio

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
	spotWS    = "wss://api.gateio.ws/ws/v4/"
	futuresWS = "wss://fx-ws.gateio.ws/v4/ws/usdt"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 10 * time.Second // Gate.io требует ping каждые 10 сек
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var dialer = websocket.Dialer{HandshakeTimeout: handshakeTimeout}

// Gate.io использует channel/event модель
type wsRequest struct {
	Time    int64    `json:"time"`
	Channel string   `json:"channel"`
	Event   string   `json:"event"`
	Payload []string `json:"payload"`
}

type wsResponse struct {
	Channel string     `json:"channel"`
	Event   string     `json:"event"` // "update"
	Result  tickerData `json:"result"`
}

// Spot ticker
type tickerData struct {
	CurrencyPair string `json:"currency_pair"` // "BTC_USDT"
	HighestBid   string `json:"highest_bid"`
	LowestAsk    string `json:"lowest_ask"`
	QuoteVolume  string `json:"quote_volume"`
}

// Futures ticker (другая структура)
type futuresTickerData struct {
	Contract string `json:"contract"` // "BTC_USDT"
	Bid1     string `json:"highest_bid"`
	Ask1     string `json:"lowest_ask"`
	VolUSDT  string `json:"volume_24h_quote"`
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
	defer conn.Close()

	log.Printf("✅ Gate.io Spot connected")

	// Gate.io подписка на все тикеры: payload ["!all"]
	sub := wsRequest{
		Time:    time.Now().Unix(),
		Channel: "spot.tickers",
		Event:   "subscribe",
		Payload: []string{"!all"},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)
	_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))
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

		var resp wsResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil {
			continue
		}

		if resp.Event != "update" {
			continue
		}

		tick, ok := spotTickerToTick(&resp.Result)
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

func (a *Adapter) connectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	conn, _, err := dialer.DialContext(ctx, futuresWS, nil)
	if err != nil {
		return fmt.Errorf("dial futures: %w", err)
	}
	defer conn.Close()

	log.Printf("✅ Gate.io Futures connected")

	sub := wsRequest{
		Time:    time.Now().Unix(),
		Channel: "futures.tickers",
		Event:   "subscribe",
		Payload: []string{"!all"},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe futures: %w", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)
	_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))
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

		// Futures тикер приходит как массив
		var raw struct {
			Channel string              `json:"channel"`
			Event   string              `json:"event"`
			Result  []futuresTickerData `json:"result"`
		}
		if err := sonic.Unmarshal(msg, &raw); err != nil {
			continue
		}

		if raw.Event != "update" {
			continue
		}

		now := time.Now()
		for i := range raw.Result {
			tick, ok := futuresTickerToTick(&raw.Result[i], now)
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

func spotTickerToTick(d *tickerData) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(d.HighestBid)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}
	ask, err := decimal.NewFromString(d.LowestAsk)
	if err != nil || ask.IsZero() {
		return domain.MarketTick{}, false
	}
	qVol, _ := decimal.NewFromString(d.QuoteVolume)

	// "BTC_USDT" → "BTCUSDT"
	symbol := normalizeSymbol(d.CurrencyPair)

	return domain.MarketTick{
		Exchange:    "GATEIO",
		Symbol:      symbol,
		MarketType:  domain.MarketTypeSpot,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		Timestamp:   time.Now(),
	}, true
}

func futuresTickerToTick(d *futuresTickerData, ts time.Time) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(d.Bid1)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}
	ask, err := decimal.NewFromString(d.Ask1)
	if err != nil || ask.IsZero() {
		return domain.MarketTick{}, false
	}
	qVol, _ := decimal.NewFromString(d.VolUSDT)
	symbol := normalizeSymbol(d.Contract)

	return domain.MarketTick{
		Exchange:    "GATEIO",
		Symbol:      symbol,
		MarketType:  domain.MarketTypeFutures,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		Timestamp:   ts,
	}, true
}

// "BTC_USDT" → "BTCUSDT"
func normalizeSymbol(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '_' {
			result = append(result, s[i])
		}
	}
	return string(result)
}

// Gate.io ping это JSON с channel
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
				log.Printf("⚠️  Gate.io ping error: %v", err)
				return
			}
		}
	}
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}
