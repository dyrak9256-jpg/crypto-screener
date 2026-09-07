package bitget

import (
	"context"
	"crypto-screener/internal/domain"
	"crypto-screener/internal/ingress"
	"crypto-screener/internal/retry"
	"crypto-screener/internal/wsutil"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
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
	maxBatchSize     = 50
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
	Ts     int64        `json:"ts"`
}

type tickerData struct {
	InstID          string `json:"instId"`
	BidPr           string `json:"bidPr"`
	AskPr           string `json:"askPr"`
	QuoteVol        string `json:"quoteVolume"`
	Ts              int64  `json:"ts,string"`
	FundingRate     string `json:"fundingRate"`
	NextFundingTime int64  `json:"nextFundingTime,string"`
}

type Adapter struct {
	droppedTicks atomic.Uint64
	fundingSink  atomic.Value
	fundingReady chan struct{}
	fundingOnce  sync.Once
}

func NewAdapter() *Adapter {
	return &Adapter{fundingReady: make(chan struct{})}
}

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, "SPOT", domain.MarketTypeSpot, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	select {
	case <-a.fundingReady:
	case <-ctx.Done():
		return nil
	}
	a.listen(ctx, "USDT-FUTURES", domain.MarketTypeFutures, out)
	return nil
}
func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	if sink == nil {
		return fmt.Errorf("Bitget funding sink is nil")
	}
	a.fundingSink.Store(sink)
	a.fundingOnce.Do(func() { close(a.fundingReady) })
	<-ctx.Done()
	return nil
}

func (a *Adapter) listen(
	ctx context.Context,
	instType string,
	mType domain.MarketType,
	out chan<- domain.MarketTick,
) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for {
		startedAt := time.Now()
		if ctx.Err() != nil {
			return
		}

		if err := a.connectAndRead(ctx, instType, mType, out); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  Bitget %s WS: %v — reconnecting in %s", mType, err, reconnectDelay)
		}

		if time.Since(startedAt) >= 30*time.Second {
			backoff.Reset()
		}
		if !backoff.Wait(ctx.Done()) {
			return
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
	conn.SetReadLimit(1 << 20)
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

		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("refresh Bitget %s read deadline: %w", mType, err)
		}

		if string(msg) == "pong" {
			continue
		}

		var resp wsResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil {
			continue
		}

		if resp.Event != "" {
			if resp.Event == "error" || (resp.Code != "" && resp.Code != "0") {
				return fmt.Errorf("Bitget websocket subscription error: code=%q event=%q", resp.Code, resp.Event)
			}
			continue
		}

		if len(resp.Data) == 0 {
			continue
		}

		now := time.Now()
		for i := range resp.Data {
			eventTime := now
			if resp.Data[i].Ts > 0 {
				eventTime = time.UnixMilli(resp.Data[i].Ts)
			} else if resp.Ts > 0 {
				eventTime = time.UnixMilli(resp.Ts)
			}
			if mType == domain.MarketTypeFutures {
				if v := a.fundingSink.Load(); v != nil && resp.Data[i].FundingRate != "" {
					rate, err := decimal.NewFromString(resp.Data[i].FundingRate)
					if err == nil {
						if updateErr := v.(domain.FundingSink).UpdateFunding("BITGET", resp.Data[i].InstID, rate, time.UnixMilli(resp.Data[i].NextFundingTime), eventTime); updateErr != nil {
							return fmt.Errorf("update Bitget funding %s: %w", resp.Data[i].InstID, updateErr)
						}
					}
				}
			}
			tick, ok := toMarketTick(&resp.Data[i], mType, eventTime)
			if !ok {
				continue
			}

			ingress.Submit(out, tick)
		}
	}
}

func toMarketTick(d *tickerData, mType domain.MarketType, ts time.Time) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(d.BidPr)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(d.AskPr)
	if err != nil || ask.IsZero() || bid.GreaterThan(ask) {
		return domain.MarketTick{}, false
	}

	qVol, err := decimal.NewFromString(d.QuoteVol)
	if err != nil {
		return domain.MarketTick{}, false
	}
	symbol := symbolReplacer.Replace(d.InstID)

	return domain.MarketTick{
		Exchange:    "BITGET",
		Symbol:      symbol,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		EventTime:   ts,
		ReceivedAt:  time.Now(),
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
		return nil, fmt.Errorf("fetchSpotSymbols: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetchSpotSymbols: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("bitget REST status %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("fetchSpotSymbols: %w", err)
	}

	var parsed struct {
		Code string `json:"code"`
		Data []struct {
			Symbol    string `json:"symbol"`
			QuoteCoin string `json:"quoteCoin"`
			Status    string `json:"status"`
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
		if item.Status == "online" && item.QuoteCoin == "USDT" {
			symbols = append(symbols, item.Symbol)
		}
	}

	return symbols, nil
}

func fetchFuturesSymbols(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, futuresSymbolsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("fetchFuturesSymbols: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetchFuturesSymbols: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("bitget REST status %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("fetchFuturesSymbols: %w", err)
	}

	var parsed struct {
		Code string `json:"code"`
		Data []struct {
			Symbol       string `json:"symbol"`
			SymbolStatus string `json:"symbolStatus"`
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
		// Bitget переименовал contractStatus → symbolStatus (подтверждено живым
		// API 08.09.2026: 780 USDT-перпетуалов со статусом "normal").
		if item.SymbolStatus == "normal" {
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

func (a *Adapter) ConnectCandles(ctx context.Context, sink domain.CandleSink) error {
	go a.runCandleFeed(ctx, sink, domain.MarketTypeSpot)
	go a.runCandleFeed(ctx, sink, domain.MarketTypeFutures)
	<-ctx.Done()
	return nil
}

type bitgetCandleResponse struct {
	Action string `json:"action"`
	Event  string `json:"event"`
	Code   string `json:"code"`
	Msg    string `json:"msg"`
	Arg    struct {
		InstType string `json:"instType"`
		Channel  string `json:"channel"`
		InstID   string `json:"instId"`
	} `json:"arg"`
	Data [][]string `json:"data"`
	Ts   int64      `json:"ts"`
}

func (a *Adapter) runCandleFeed(ctx context.Context, sink domain.CandleSink, market domain.MarketType) {
	instType := "USDT-FUTURES"
	if market == domain.MarketTypeSpot {
		instType = "SPOT"
	}
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		symbols, err := fetchSymbols(ctx, instType)
		if err != nil {
			log.Printf("⚠️ Bitget %s candle symbols: %v", market, err)
			select {
			case <-ctx.Done():
				return
			case <-retry.After(reconnectDelay):
			}
			continue
		}
		if err := a.readCandleShard(ctx, sink, instType, symbols, market); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ Bitget %s candle WS: %v", market, err)
		}
		if ctx.Err() == nil && !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) readCandleShard(ctx context.Context, sink domain.CandleSink, instType string, symbols []string, market domain.MarketType) error {
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("readCandleShard: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := wsutil.StartHeartbeat(connCtx, conn, 20*time.Second, 60*time.Second); err != nil {
		return fmt.Errorf("start websocket heartbeat: %w", err)
	}
	go closeOnCtx(connCtx, conn)
	args := make([]argItem, 0, len(symbols))
	for _, sym := range symbols {
		args = append(args, argItem{InstType: instType, Channel: "candle1m", InstID: sym})
	}
	for i := 0; i < len(args); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(args) {
			end = len(args)
		}
		if err := conn.WriteJSON(subscribeMsg{Op: "subscribe", Args: args[i:end]}); err != nil {
			return fmt.Errorf("readCandleShard: %w", err)
		}
	}
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("readCandleShard: %w", err)
		}
		if err := wsutil.TouchReadDeadline(conn, 60*time.Second); err != nil {
			return fmt.Errorf("refresh websocket read deadline: %w", err)
		}
		var r bitgetCandleResponse
		if err := sonic.Unmarshal(msg, &r); err != nil {
			continue
		}
		if r.Event == "error" || (r.Code != "" && r.Code != "0") {
			return fmt.Errorf("Bitget candle subscription rejected: code=%q msg=%q", r.Code, r.Msg)
		}
		if len(r.Data) == 0 || r.Arg.InstID == "" {
			continue
		}
		for _, d := range r.Data {
			if len(d) < 8 {
				continue
			}
			start, _ := strconv.ParseInt(d[0], 10, 64)
			q := d[7]
			if q == "" {
				q = d[6]
			}
			vol, err := decimal.NewFromString(q)
			if err != nil || vol.IsNegative() {
				continue
			}
			et := time.UnixMilli(r.Ts)
			if err := sink.UpdateCandle(domain.MarketCandle{Exchange: "BITGET", Symbol: r.Arg.InstID, MarketType: market, OpenTime: time.UnixMilli(start), CloseTime: time.UnixMilli(start + 59999), QuoteVolume: vol, EventTime: et, Closed: false}); err != nil {
				return fmt.Errorf("readCandleShard: %w", err)
			}
		}
	}
}
