package kucoin

import (
	"context"
	"crypto-screener/internal/domain"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

// KuCoin особенный: WS URL получается через REST API
const (
	bulletPublicURL = "https://api.kucoin.com/api/v1/bullet-public"

	handshakeTimeout = 10 * time.Second
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var dialer = websocket.Dialer{HandshakeTimeout: handshakeTimeout}

// REST ответ для получения WS endpoint
type bulletResponse struct {
	Data struct {
		Token           string `json:"token"`
		InstanceServers []struct {
			Endpoint     string `json:"endpoint"`
			PingInterval int    `json:"pingInterval"` // миллисекунды
			PingTimeout  int    `json:"pingTimeout"`
		} `json:"instanceServers"`
	} `json:"data"`
}

type subscribeMsg struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	Topic          string `json:"topic"`
	PrivateChannel bool   `json:"privateChannel"`
	Response       bool   `json:"response"`
}

type wsMessage struct {
	Type    string     `json:"type"` // "message" | "pong" | "ack"
	Topic   string     `json:"topic"`
	Subject string     `json:"subject"`
	Data    tickerData `json:"data"`
}

type tickerData struct {
	Symbol  string `json:"symbol"` // "BTC-USDT"
	BestBid string `json:"bestBid"`
	BestAsk string `json:"bestAsk"`
	// KuCoin не даёт volume в ticker топике — используем 0
}

type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	go a.listen(ctx, domain.MarketTypeSpot, out)
	return nil
}

// KuCoin Futures использует отдельный API
func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	go a.listenFutures(ctx, out)
	return nil
}

func (a *Adapter) listen(ctx context.Context, mType domain.MarketType, out chan<- domain.MarketTick) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := a.connectAndRead(ctx, mType, out); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  KuCoin %s WS: %v — reconnecting in %s", mType, err, reconnectDelay)
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
			log.Printf("⚠️  KuCoin Futures WS: %v — reconnecting in %s", err, reconnectDelay)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectDelay):
		}
	}
}

// getWSEndpoint получает временный WS URL через REST
func getWSEndpoint(ctx context.Context, restURL string) (endpoint, token string, pingMs int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, restURL, nil)
	if err != nil {
		return "", "", 0, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", 0, err
	}

	var bullet bulletResponse
	if err := sonic.Unmarshal(body, &bullet); err != nil {
		return "", "", 0, err
	}

	if len(bullet.Data.InstanceServers) == 0 {
		return "", "", 0, fmt.Errorf("no instance servers returned")
	}

	srv := bullet.Data.InstanceServers[0]
	return srv.Endpoint, bullet.Data.Token, srv.PingInterval, nil
}

func (a *Adapter) connectAndRead(ctx context.Context, mType domain.MarketType, out chan<- domain.MarketTick) error {
	endpoint, token, pingMs, err := getWSEndpoint(ctx, bulletPublicURL)
	if err != nil {
		return fmt.Errorf("get ws endpoint: %w", err)
	}

	// URL: endpoint?token=xxx&connectId=yyy
	wsURL := fmt.Sprintf("%s?token=%s&connectId=spot-%d", endpoint, token, time.Now().UnixNano())
	pingInterval := time.Duration(pingMs) * time.Millisecond

	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	log.Printf("✅ KuCoin Spot connected")

	// Подписка на все тикеры
	sub := subscribeMsg{
		ID:       fmt.Sprintf("%d", time.Now().UnixNano()),
		Type:     "subscribe",
		Topic:    "/market/ticker:all",
		Response: true,
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)
	_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))
	go kucoinKeepAlive(connCtx, conn, pingInterval)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		var wsMsg wsMessage
		if err := sonic.Unmarshal(msg, &wsMsg); err != nil {
			continue
		}

		if wsMsg.Type != "message" {
			continue
		}

		tick, ok := toMarketTick(&wsMsg.Data, mType)
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
	// KuCoin Futures отдельный bullet endpoint
	endpoint, token, pingMs, err := getWSEndpoint(ctx, "https://api-futures.kucoin.com/api/v1/bullet-public")
	if err != nil {
		return fmt.Errorf("get futures ws endpoint: %w", err)
	}

	wsURL := fmt.Sprintf("%s?token=%s&connectId=futures-%d", endpoint, token, time.Now().UnixNano())
	pingInterval := time.Duration(pingMs) * time.Millisecond

	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial futures: %w", err)
	}
	defer conn.Close()

	log.Printf("✅ KuCoin Futures connected")

	sub := subscribeMsg{
		ID:       fmt.Sprintf("%d", time.Now().UnixNano()),
		Type:     "subscribe",
		Topic:    "/contractMarket/ticker:all",
		Response: true,
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe futures: %w", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)
	_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))
	go kucoinKeepAlive(connCtx, conn, pingInterval)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read futures: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		var wsMsg wsMessage
		if err := sonic.Unmarshal(msg, &wsMsg); err != nil {
			continue
		}

		if wsMsg.Type != "message" {
			continue
		}

		tick, ok := toMarketTick(&wsMsg.Data, domain.MarketTypeFutures)
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

func toMarketTick(d *tickerData, mType domain.MarketType) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(d.BestBid)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}
	ask, err := decimal.NewFromString(d.BestAsk)
	if err != nil || ask.IsZero() {
		return domain.MarketTick{}, false
	}

	// "BTC-USDT" → "BTCUSDT"
	symbol := normalizeSymbol(d.Symbol)

	return domain.MarketTick{
		Exchange:    "KUCOIN",
		Symbol:      symbol,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: decimal.Zero, // KuCoin ticker не даёт volume
		Timestamp:   time.Now(),
	}, true
}

func normalizeSymbol(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '-' {
			result = append(result, s[i])
		}
	}
	return string(result)
}

// KuCoin ping: JSON {"id":"...","type":"ping"}
func kucoinKeepAlive(ctx context.Context, conn *websocket.Conn, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ping := subscribeMsg{
				ID:   fmt.Sprintf("%d", time.Now().UnixNano()),
				Type: "ping",
			}
			if err := conn.WriteJSON(ping); err != nil {
				log.Printf("⚠️  KuCoin ping error: %v", err)
				return
			}
		}
	}
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}
