package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	VolValue     string `json:"volValue"`
	Turnover     string `json:"turnover"`
	Turnover24h  string `json:"turnover24h"`
}

type Adapter struct {
	droppedTicks atomic.Uint64
}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, domain.MarketTypeSpot, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, domain.MarketTypeFutures, out)
	return nil
}

func (a *Adapter) listen(ctx context.Context, mType domain.MarketType, out chan<- domain.MarketTick) {
	bulletURL := bulletSpotURL
	topic := "/market/ticker:all"

	if mType == domain.MarketTypeFutures {
		bulletURL = bulletFuturesURL
		topic = "/contractMarket/ticker:all"
	}

	for {
		if ctx.Err() != nil {
			return
		}

		if err := a.connectAndRead(ctx, bulletURL, topic, mType, out); err != nil {
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
	conn.SetReadLimit(1 << 20)
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

	qVolStr := d.VolValue
	if qVolStr == "" {
		qVolStr = d.Turnover24h
	}
	if qVolStr == "" {
		qVolStr = d.Turnover
	}
	qVol := decimal.Zero
	if qVolStr != "" {
		qVol, err = decimal.NewFromString(qVolStr)
		if err != nil {
			return domain.MarketTick{}, false
		}
	}
	if bid.GreaterThan(ask) {
		return domain.MarketTick{}, false
	}
	now := time.Now()
	return domain.MarketTick{
		Exchange: "KUCOIN", Symbol: symbol, MarketType: mType,
		BestBid: bid, BestAsk: ask, QuoteVolume: qVol,
		EventTime: now, ReceivedAt: now,
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
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", 0, fmt.Errorf("kucoin REST status %s", resp.Status)
	}

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

func (a *Adapter) ConnectCandles(ctx context.Context, sink domain.CandleSink) error {
	go a.runCandleFeed(ctx, sink, domain.MarketTypeSpot)
	go a.runCandleFeed(ctx, sink, domain.MarketTypeFutures)
	<-ctx.Done()
	return nil
}

type kucoinCandleData struct {
	Symbol  string   `json:"symbol"`
	Candles []string `json:"candles"`
	Time    int64    `json:"time"`
}
type kucoinCandleMessage struct {
	Type  string           `json:"type"`
	Topic string           `json:"topic"`
	Data  kucoinCandleData `json:"data"`
}
type kucoinSymbolsResponse struct {
	Code string `json:"code"`
	Data []struct {
		Symbol        string `json:"symbol"`
		QuoteCurrency string `json:"quoteCurrency"`
		EnableTrading bool   `json:"enableTrading"`
	} `json:"data"`
}
type kucoinFuturesSymbolsResponse struct {
	Code string `json:"code"`
	Data []struct {
		Symbol        string `json:"symbol"`
		QuoteCurrency string `json:"quoteCurrency"`
	} `json:"data"`
}

func (a *Adapter) runCandleFeed(ctx context.Context, sink domain.CandleSink, market domain.MarketType) {
	for ctx.Err() == nil {
		symbols, err := kucoinCandleSymbols(ctx, market)
		if err != nil {
			log.Printf("⚠️ KuCoin %s candle symbols: %v", market, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}
		for i := 0; i < len(symbols); i += 100 {
			end := i + 100
			if end > len(symbols) {
				end = len(symbols)
			}
			part := append([]string(nil), symbols[i:end]...)
			if err := a.readCandleShard(ctx, sink, part, market); err != nil && ctx.Err() == nil {
				log.Printf("⚠️ KuCoin %s candle WS: %v", market, err)
			}
		}
	}
}

func kucoinCandleSymbols(ctx context.Context, market domain.MarketType) ([]string, error) {
	url := "https://api.kucoin.com/api/v2/symbols"
	if market == domain.MarketTypeFutures {
		url = "https://api-futures.kucoin.com/api/v1/contracts/active"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if market == domain.MarketTypeSpot {
		var x kucoinSymbolsResponse
		if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
			return nil, err
		}
		out := make([]string, 0, len(x.Data))
		for _, v := range x.Data {
			if v.EnableTrading && v.QuoteCurrency == "USDT" {
				out = append(out, v.Symbol)
			}
		}
		return out, nil
	}
	var x kucoinFuturesSymbolsResponse
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(x.Data))
	for _, v := range x.Data {
		if strings.Contains(v.Symbol, "USDT") {
			out = append(out, v.Symbol)
		}
	}
	return out, nil
}

func (a *Adapter) readCandleShard(ctx context.Context, sink domain.CandleSink, symbols []string, market domain.MarketType) error {
	bullet := bulletSpotURL
	if market == domain.MarketTypeFutures {
		bullet = bulletFuturesURL
	}
	endpoint, token, ping, err := getWSEndpoint(ctx, bullet)
	if err != nil {
		return err
	}
	wsURL := fmt.Sprintf("%s?token=%s&connectId=candle-%d", endpoint, token, time.Now().UnixNano())
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	go closeOnCtx(ctx, conn)
	go kucoinKeepAlive(ctx, conn, ping)
	for _, sym := range symbols {
		topic := "/market/candles:" + sym + "_1min"
		if market == domain.MarketTypeFutures {
			topic = "/contractMarket/limitCandle:" + sym + "_1min"
		}
		if err := conn.WriteJSON(subscribeMsg{ID: fmt.Sprintf("%d", time.Now().UnixNano()), Type: "subscribe", Topic: topic, Response: true}); err != nil {
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
		var r kucoinCandleMessage
		if sonic.Unmarshal(msg, &r) != nil || r.Type != "message" || len(r.Data.Candles) < 7 {
			continue
		}
		start, err := strconv.ParseInt(r.Data.Candles[0], 10, 64)
		if err != nil {
			continue
		}
		vol, err := decimal.NewFromString(r.Data.Candles[6])
		if err != nil || !vol.IsPositive() {
			continue
		}
		et := time.Now()
		if r.Data.Time > 0 {
			et = time.Unix(0, r.Data.Time*int64(time.Microsecond))
		}
		sym := strings.ReplaceAll(r.Data.Symbol, "-", "")
		sink.UpdateCandle(domain.MarketCandle{Exchange: "KUCOIN", Symbol: sym, MarketType: market, OpenTime: time.Unix(start, 0), CloseTime: time.Unix(start, 0).Add(time.Minute - time.Millisecond), QuoteVolume: vol, EventTime: et})
	}
}
