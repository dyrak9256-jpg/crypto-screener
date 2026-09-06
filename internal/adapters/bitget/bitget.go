package bitget

import (
	"context"
	"crypto-screener/internal/domain"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	wsURL             = "wss://ws.bitget.com/v2/ws/public"
	spotSymbolsURL    = "https://api.bitget.com/api/v2/spot/public/symbols"
	futuresSymbolsURL = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 25 * time.Second
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
	maxBatchSize     = 100
)

var (
	dialer         = websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	httpClient     = &http.Client{Timeout: 10 * time.Second}
	symbolReplacer = strings.NewReplacer("-", "", "_", "")
)

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
	Action string       `json:"action"`
	Arg    argItem      `json:"arg"`
	Data   []tickerData `json:"data"`
	Event  string       `json:"event"`
	Code   string       `json:"code"`
}

type tickerData struct {
	InstID   string `json:"instId"`
	BidPr    string `json:"bidPr"`
	AskPr    string `json:"askPr"`
	QuoteVol string `json:"quoteVolume"`
}

type Adapter struct {
	droppedTicks atomic.Uint64
}

func NewAdapter() *Adapter {
	return &Adapter{}
}

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, "SPOT", domain.MarketTypeSpot, out)
	return nil
}

// ConnectFunding — BITGET не предоставляет поток ставок финансирования через
// этот коннектор. Блокируем до завершения контекста, чтобы supervisor-горутина
// (runWithReconnect) не зациклилась на переподключениях.
// Отсутствие данных о funding означает, что фильтр по funding остаётся
// пермиссивным (сигнал считается прибыльным).
func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	<-ctx.Done()
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, "USDT-FUTURES", domain.MarketTypeFutures, out)
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
	symbols, err := fetchSymbols(ctx, instType)
	if err != nil {
		return fmt.Errorf("fetch symbols: %w", err)
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no active symbols for %s", instType)
	}

	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	log.Printf("✅ Bitget %s connected (%d symbols)", mType, len(symbols))

	for i := 0; i < len(symbols); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		args := make([]argItem, 0, end-i)
		for _, sym := range symbols[i:end] {
			args = append(args, argItem{
				InstType: instType,
				Channel:  "ticker",
				InstID:   sym,
			})
		}

		if err := conn.WriteJSON(subscribeMsg{Op: "subscribe", Args: args}); err != nil {
			return fmt.Errorf("subscribe batch [%d:%d]: %w", i, end, err)
		}
	}

	// connCtx: отменяется при выходе из функции → останавливает keepAlive и closeOnCtx
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// closeOnCtx разблокирует ReadMessage если контекст отменён раньше ошибки чтения
	go closeOnCtx(connCtx, conn)
	go keepAlive(connCtx, conn)

	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

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

		if resp.Event != "" {
			if resp.Event == "error" {
				log.Printf("⚠️  Bitget WS error: code=%s", resp.Code)
			}
			continue
		}

		if len(resp.Data) == 0 {
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
				// Канал полон: дропаем тик чтобы не блокировать WS горутину
				// Блокировка здесь → WS буфер не читается → биржа закроет соединение
				if n := a.droppedTicks.Add(1); n%1000 == 0 {
					log.Printf("⚠️  Bitget %s: dropped %d ticks (channel full)", mType, n)
				}
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
	symbol := symbolReplacer.Replace(d.InstID)

	return domain.MarketTick{
		Exchange:    "BITGET",
		Symbol:      symbol,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		Timestamp:   ts,
	}, true
}

func fetchSymbols(ctx context.Context, instType string) ([]string, error) {
	if instType == "USDT-FUTURES" {
		return fetchFuturesSymbols(ctx)
	}
	return fetchSpotSymbols(ctx)
}

func fetchSpotSymbols(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, spotSymbolsURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var parsed struct {
		Code string `json:"code"`
		Data []struct {
			Symbol string `json:"symbol"`
			Status string `json:"status"`
		} `json:"data"`
	}

	if err := sonic.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("parse spot symbols: %w", err)
	}
	if parsed.Code != "00000" {
		return nil, fmt.Errorf("bitget spot REST error: %s", parsed.Code)
	}

	symbols := make([]string, 0, len(parsed.Data))
	for _, item := range parsed.Data {
		if item.Status == "online" {
			symbols = append(symbols, item.Symbol)
		}
	}

	return symbols, nil
}

func fetchFuturesSymbols(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, futuresSymbolsURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var parsed struct {
		Code string `json:"code"`
		Data []struct {
			Symbol         string `json:"symbol"`
			ContractStatus string `json:"contractStatus"`
		} `json:"data"`
	}

	if err := sonic.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("parse futures symbols: %w", err)
	}
	if parsed.Code != "00000" {
		return nil, fmt.Errorf("bitget futures REST error: %s", parsed.Code)
	}

	symbols := make([]string, 0, len(parsed.Data))
	for _, item := range parsed.Data {
		if item.ContractStatus == "normal" {
			symbols = append(symbols, item.Symbol)
		}
	}

	return symbols, nil
}

func keepAlive(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
				return
			}
		}
	}
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}
