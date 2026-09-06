package mexc

import (
	"context"
	"fmt"
	"log"
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
	spotWS    = "wss://wbs.mexc.com/ws"
	futuresWS = "wss://contract.mexc.com/edge"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 15 * time.Second
	pongWait         = 20 * time.Second
	reconnectDelay   = 3 * time.Second
)

var (
	dialer         = websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	spotPingMsg    = []byte(`{"method":"PING"}`)
	futuresPingMsg = []byte(`{"method":"ping"}`)
	futuresPongMsg = []byte(`{"method":"pong"}`)
)

type subscribeMsg struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
}

type spotTickerData struct {
	Symbol  string `json:"s"`
	Bid     string `json:"b"`
	Ask     string `json:"a"`
	QVolume string `json:"qv"`
}

type futuresResponse struct {
	Channel string            `json:"channel"`
	Data    futuresTickerData `json:"data"`
}

// futuresTickerData: bid1/ask1 приходят как float64
// Используем float64 + конвертацию через strconv для точности
type futuresTickerData struct {
	Symbol string  `json:"symbol"`
	Bid1   float64 `json:"bid1"`
	Ask1   float64 `json:"ask1"`
	Volume float64 `json:"volume24"`
}

type Adapter struct {
	droppedTicks atomic.Uint64
}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	return a.connectSpot(ctx, out)
}

// ConnectFunding — MEXC не предоставляет поток ставок финансирования через
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
		return fmt.Errorf("dial spot: %w", err)
	}
	defer conn.Close()

	log.Printf("✅ MEXC Spot connected")

	sub := subscribeMsg{
		Method: "SUBSCRIPTION",
		Params: []string{"spot@public.miniTickers.v3.api"},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe spot: %w", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	go spotKeepAlive(connCtx, conn)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read spot: %w", err)
		}

		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		// MEXC Spot шлёт PONG как TextMessage — не Control Frame
		// Просто обновляем дедлайн (уже сделано выше) и пропускаем
		if strings.Contains(string(msg), "PONG") {
			continue
		}

		var raw struct {
			Data []spotTickerData `json:"d"`
		}
		if err := sonic.Unmarshal(msg, &raw); err != nil || len(raw.Data) == 0 {
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
				if n := a.droppedTicks.Add(1); n%1000 == 0 {
					log.Printf("⚠️  MEXC Spot: dropped %d ticks (channel full)", n)
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

	log.Printf("✅ MEXC Futures connected")

	sub := subscribeMsg{
		Method: "sub.ticker",
		Params: []string{"all"},
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

	go futuresKeepAlive(connCtx, conn)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read futures: %w", err)
		}

		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		// ✅ Отвечаем на серверный ping
		// Используем conn.WriteMessage — WriteTextMessage не существует в gorilla
		if strings.Contains(string(msg), `"ping"`) {
			if err := conn.WriteMessage(websocket.TextMessage, futuresPongMsg); err != nil {
				return fmt.Errorf("write pong: %w", err)
			}
			continue
		}

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
			if n := a.droppedTicks.Add(1); n%1000 == 0 {
				log.Printf("⚠️  MEXC Futures: dropped %d ticks (channel full)", n)
			}
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
		Symbol:      d.Symbol, // Spot уже в формате BTCUSDT
		MarketType:  domain.MarketTypeSpot,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		Timestamp:   ts,
	}, true
}

func futuresToTick(d *futuresTickerData) (domain.MarketTick, bool) {
	if d.Bid1 <= 0 || d.Ask1 <= 0 {
		return domain.MarketTick{}, false
	}

	// ✅ float64 → string → decimal для максимальной точности
	// strconv.FormatFloat с 'f' и -1 даёт минимальное представление без потерь
	bid, err := decimal.NewFromString(strconv.FormatFloat(d.Bid1, 'f', -1, 64))
	if err != nil {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(strconv.FormatFloat(d.Ask1, 'f', -1, 64))
	if err != nil {
		return domain.MarketTick{}, false
	}

	vol, _ := decimal.NewFromString(strconv.FormatFloat(d.Volume, 'f', -1, 64))

	// ✅ Нормализация: "BTC_USDT" → "BTCUSDT"
	symbol := strings.ReplaceAll(d.Symbol, "_", "")

	return domain.MarketTick{
		Exchange:    "MEXC",
		Symbol:      symbol,
		MarketType:  domain.MarketTypeFutures,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: vol,
		Timestamp:   time.Now(),
	}, true
}

func spotKeepAlive(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := conn.WriteMessage(websocket.TextMessage, spotPingMsg); err != nil {
				log.Printf("⚠️  MEXC Spot ping error: %v", err)
				return
			}
		}
	}
}

func futuresKeepAlive(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := conn.WriteMessage(websocket.TextMessage, futuresPingMsg); err != nil {
				log.Printf("⚠️  MEXC Futures ping error: %v", err)
				return
			}
		}
	}
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}
