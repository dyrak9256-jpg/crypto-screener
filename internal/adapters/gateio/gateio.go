package gateio

import (
	"context"
	"crypto-screener/internal/domain"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	spotWS    = "wss://api.gateio.ws/ws/v4/"
	futuresWS = "wss://fx-ws.gateio.ws/v4/ws/usdt"

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
	droppedTicks atomic.Uint64
}

func NewAdapter() *Adapter {
	return &Adapter{}
}

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	return a.connectSpot(ctx, out)
}

// ConnectFunding — GATEIO не предоставляет поток ставок финансирования через
// этот коннектор. Блокируем до завершения контекста, чтобы supervisor-горутина
// (runWithReconnect) не зациклилась на переподключениях.
// Отсутствие данных о funding означает, что фильтр по funding остаётся
// пермиссивным (сигнал считается прибыльным).
func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	<-ctx.Done()
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	return a.connectFutures(ctx, out)
}

func (a *Adapter) connectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	conn, _, err := dialer.DialContext(ctx, spotWS, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	log.Printf("✅ Gate.io Spot connected")

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
	if err != nil || ask.IsZero() {
		return domain.MarketTick{}, false
	}

	qVol, _ := decimal.NewFromString(d.QuoteVolume)

	return domain.MarketTick{
		Exchange:    "GATEIO",
		Symbol:      symbolReplacer.Replace(d.CurrencyPair),
		MarketType:  domain.MarketTypeSpot,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		Timestamp:   ts,
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

	qVol, _ := decimal.NewFromString(d.Volume)

	return domain.MarketTick{
		Exchange:    "GATEIO",
		Symbol:      symbolReplacer.Replace(d.Contract),
		MarketType:  domain.MarketTypeFutures,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
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
