package kucoin

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"crypto-screener/internal/domain"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	bulletSpotURL    = "https://api.kucoin.com/api/v1/bullet-public"
	bulletFuturesURL = "https://api-futures.kucoin.com/api/v1/bullet-public"

	handshakeTimeout    = 10 * time.Second
	pongWait            = 10 * time.Second
	reconnectDelay      = 3 * time.Second
	defaultPingInterval = 18 * time.Second // ✅ fallback если API вернул 0
)

var (
	dialer     = websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	httpClient = &http.Client{Timeout: 10 * time.Second}
)

type bulletResponse struct {
	Data struct {
		Token           string `json:"token"`
		InstanceServers []struct {
			Endpoint     string `json:"endpoint"`
			PingInterval int    `json:"pingInterval"` // миллисекунды
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

// tickerData покрывает оба формата
// Spot:    BestBid / BestAsk
// Futures: BestBidPrice / BestAskPrice
type tickerData struct {
	Symbol       string `json:"symbol"`
	BestBid      string `json:"bestBid"`
	BestAsk      string `json:"bestAsk"`
	BestBidPrice string `json:"bestBidPrice"`
	BestAskPrice string `json:"bestAskPrice"`
}

type Adapter struct {
	droppedTicks atomic.Uint64
}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	return a.connectAndRead(ctx, bulletSpotURL, "/market/ticker:all", domain.MarketTypeSpot, out)
}

// ConnectFunding — KUCOIN не предоставляет поток ставок финансирования через
// этот коннектор. Блокируем до завершения контекста, чтобы supervisor-горутина
// (runWithReconnect) не зациклилась на переподключениях.
// Отсутствие данных о funding означает, что фильтр по funding остаётся
// пермиссивным (сигнал считается прибыльным).
func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	<-ctx.Done()
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	return a.connectAndRead(ctx, bulletFuturesURL, "/contractMarket/ticker:all", domain.MarketTypeFutures, out)
}

func (a *Adapter) connectAndRead(
	ctx context.Context,
	bulletURL string,
	topic string,
	mType domain.MarketType,
	out chan<- domain.MarketTick,
) error {
	endpoint, token, pingInterval, err := getWSEndpoint(ctx, bulletURL)
	if err != nil {
		return fmt.Errorf("get ws endpoint: %w", err)
	}

	wsURL := fmt.Sprintf("%s?token=%s&connectId=%s-%d",
		endpoint, token, mType, time.Now().UnixNano())

	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	log.Printf("✅ KuCoin %s connected (ping every %s)", mType, pingInterval)

	sub := subscribeMsg{
		ID:       fmt.Sprintf("%d", time.Now().UnixNano()),
		Type:     "subscribe",
		Topic:    topic,
		Response: true,
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
			if n := a.droppedTicks.Add(1); n%1000 == 0 {
				log.Printf("⚠️  KuCoin %s: dropped %d ticks (channel full)", mType, n)
			}
		}
	}
}

func toMarketTick(d *tickerData, mType domain.MarketType) (domain.MarketTick, bool) {
	bidStr := d.BestBid
	askStr := d.BestAsk

	if mType == domain.MarketTypeFutures {
		if bidStr == "" || bidStr == "0" {
			bidStr = d.BestBidPrice
		}
		if askStr == "" || askStr == "0" {
			askStr = d.BestAskPrice
		}
	}

	bid, err := decimal.NewFromString(bidStr)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(askStr)
	if err != nil || ask.IsZero() {
		return domain.MarketTick{}, false
	}

	symbol, ok := normalizeSymbol(d.Symbol, mType)
	if !ok {
		return domain.MarketTick{}, false
	}

	return domain.MarketTick{
		Exchange:    "KUCOIN",
		Symbol:      symbol,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: decimal.Zero,
		Timestamp:   time.Now(),
	}, true
}

// normalizeSymbol приводит символ к формату BTCUSDT
//
// Spot:    "BTC-USDT"  → "BTCUSDT"
// Futures: "XBTUSDTM"  → "BTCUSDT"  (perpetual, XBT→BTC)
//
//	"ETHUSDTM"  → "ETHUSDT"  (perpetual)
//	"XBTMM24"   → ("", false) (квартальный — пропускаем)
func normalizeSymbol(s string, mType domain.MarketType) (string, bool) {
	if mType == domain.MarketTypeSpot {
		return strings.ReplaceAll(s, "-", ""), true
	}

	// Futures: только perpetual контракты
	// Признак perpetual: суффикс USDTM или USDM
	if !strings.HasSuffix(s, "USDTM") && !strings.HasSuffix(s, "USDM") {
		return "", false
	}

	// Убираем суффикс M: XBTUSDTM → XBTUSDT
	s = strings.TrimSuffix(s, "M")

	// XBT → BTC: KuCoin использует старое обозначение Bitcoin
	// "BTC" + s[3:] — компилятор эффективно оптимизирует короткую конкатенацию
	if strings.HasPrefix(s, "XBT") {
		return "BTC" + s[3:], true
	}

	return s, true
}

// getWSEndpoint получает временный WS URL и pingInterval через REST
func getWSEndpoint(ctx context.Context, restURL string) (endpoint, token string, pingInterval time.Duration, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, restURL, nil)
	if err != nil {
		return "", "", 0, err
	}

	resp, err := httpClient.Do(req)
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
		return "", "", 0, fmt.Errorf("parse bullet: %w", err)
	}

	if len(bullet.Data.InstanceServers) == 0 {
		return "", "", 0, fmt.Errorf("no instance servers in response")
	}

	srv := bullet.Data.InstanceServers[0]

	// ✅ Защита от паники: time.NewTicker(0) → panic
	pingMs := srv.PingInterval
	if pingMs <= 0 {
		log.Printf("⚠️  KuCoin returned invalid pingInterval=%d, using default %s",
			pingMs, defaultPingInterval)
		return srv.Endpoint, bullet.Data.Token, defaultPingInterval, nil
	}

	return srv.Endpoint, bullet.Data.Token, time.Duration(pingMs) * time.Millisecond, nil
}

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
