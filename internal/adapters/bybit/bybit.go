package bybit

import (
	"context"
	"crypto-screener/internal/domain"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	spotWS    = "wss://stream.bybit.com/v5/public/spot"
	futuresWS = "wss://stream.bybit.com/v5/public/linear"

	spotSymbolsURL    = "https://api.bybit.com/v5/market/instruments-info?category=spot&status=Trading"
	futuresSymbolsURL = "https://api.bybit.com/v5/market/instruments-info?category=linear&status=Trading"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 20 * time.Second
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
	subBatchSize     = 50 // Bybit принимает макс 50 топиков за раз
)

var dialer = websocket.Dialer{HandshakeTimeout: handshakeTimeout}

type subscribeMsg struct {
	Op   string   `json:"op"`
	Args []string `json:"args"`
}

type pingMsg struct {
	Op string `json:"op"`
}

// wsResponse: Data как RawMessage т.к. нужно накладывать delta
type wsResponse struct {
	Topic string          `json:"topic"`
	Type  string          `json:"type"` // "snapshot" | "delta"
	Data  json.RawMessage `json:"data"`
}

// pongResponse: для обработки JSON pong от Bybit
type pongResponse struct {
	Op string `json:"op"` // "pong"
}

type tickerPayload struct {
	Symbol   string `json:"symbol"`
	Bid1     string `json:"bid1Price"`
	Ask1     string `json:"ask1Price"`
	TurnOver string `json:"turnover24h"`
}

type Adapter struct {
	client *http.Client
}

func NewAdapter() *Adapter {
	return &Adapter{
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, spotWS, domain.MarketTypeSpot, out)
	return nil
}

// ConnectFunding — BYBIT не предоставляет поток ставок финансирования через
// этот коннектор. Блокируем до завершения контекста, чтобы supervisor-горутина
// (runWithReconnect) не зациклилась на переподключениях.
// Отсутствие данных о funding означает, что фильтр по funding остаётся
// пермиссивным (сигнал считается прибыльным).
func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	<-ctx.Done()
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, futuresWS, domain.MarketTypeFutures, out)
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

	// Получаем символы через REST для подписки
	restURL := futuresSymbolsURL
	if mType == domain.MarketTypeSpot {
		restURL = spotSymbolsURL
	}

	symbols, err := a.fetchSymbols(ctx, restURL)
	if err != nil {
		return fmt.Errorf("fetch symbols: %w", err)
	}

	log.Printf("📋 Bybit %s: subscribing to %d symbols", mType, len(symbols))

	// Формируем топики
	args := make([]string, 0, len(symbols))
	for _, s := range symbols {
		args = append(args, "tickers."+s)
	}

	// Отправляем подписку батчами по 50
	for i := 0; i < len(args); i += subBatchSize {
		end := i + subBatchSize
		if end > len(args) {
			end = len(args)
		}
		sub := subscribeMsg{Op: "subscribe", Args: args[i:end]}
		if err := conn.WriteJSON(sub); err != nil {
			return fmt.Errorf("subscribe batch [%d:%d]: %w", i, end, err)
		}
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	// ✅ НЕ ставим SetPongHandler — Bybit шлёт JSON pong через ReadMessage
	// Дедлайн сбрасываем при каждом сообщении в цикле
	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	go bybitKeepAlive(connCtx, conn)

	// ✅ Локальный кэш: накладываем delta на snapshot
	cache := make(map[string]*tickerPayload, len(symbols))

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}

		// Сбрасываем дедлайн при любом входящем сообщении (включая pong)
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		var resp wsResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil {
			continue
		}

		// Пропускаем служебные сообщения: pong, ack подписки
		if resp.Topic == "" {
			continue
		}

		var delta tickerPayload
		if err := sonic.Unmarshal(resp.Data, &delta); err != nil {
			continue
		}

		if delta.Symbol == "" {
			continue
		}

		// ✅ Merge delta в кэш
		current, exists := cache[delta.Symbol]
		if !exists {
			current = &tickerPayload{Symbol: delta.Symbol}
			cache[delta.Symbol] = current
		}

		// Обновляем только непустые поля (delta может содержать только часть)
		if delta.Bid1 != "" {
			current.Bid1 = delta.Bid1
		}
		if delta.Ask1 != "" {
			current.Ask1 = delta.Ask1
		}
		if delta.TurnOver != "" {
			current.TurnOver = delta.TurnOver
		}

		// После первого snapshot у нас есть оба значения
		tick, ok := toMarketTick(current, mType)
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

// fetchSymbols получает список торгующихся символов через REST
func (a *Adapter) fetchSymbols(ctx context.Context, url string) ([]string, error) {
	// Bybit пагинирует по 1000, но обычно всё влезает
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var parsed struct {
		RetCode int    `json:"retCode"`
		RetMsg  string `json:"retMsg"`
		Result  struct {
			List []struct {
				Symbol string `json:"symbol"`
				Status string `json:"status"`
			} `json:"list"`
		} `json:"result"`
	}

	if err := sonic.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	if parsed.RetCode != 0 {
		return nil, fmt.Errorf("bybit API error %d: %s", parsed.RetCode, parsed.RetMsg)
	}

	symbols := make([]string, 0, len(parsed.Result.List))
	for _, item := range parsed.Result.List {
		// Дополнительная фильтрация — только активные пары
		if item.Status == "Trading" {
			symbols = append(symbols, item.Symbol)
		}
	}

	return symbols, nil
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
