package gateio

import (
	"context"
	"crypto-screener/internal/domain"
	"crypto-screener/internal/ingress"
	"crypto-screener/internal/retry"
	"crypto-screener/internal/wsutil"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	spotWS            = "wss://api.gateio.ws/ws/v4/"
	futuresWS         = "wss://fx-ws.gateio.ws/v4/ws/usdt"
	spotSymbolsURL    = "https://api.gateio.ws/api/v4/spot/currency_pairs"
	futuresSymbolsURL = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
	symbolBatchSize   = 100

	handshakeTimeout = 10 * time.Second
	pingInterval     = 10 * time.Second
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var (
	dialer         = websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	httpClient     = &http.Client{Timeout: 10 * time.Second}
	symbolReplacer = strings.NewReplacer("_", "", "-", "")
)

type wsRequest struct {
	Time    int64    `json:"time"`
	Channel string   `json:"channel"`
	Event   string   `json:"event"`
	Payload []string `json:"payload,omitempty"`
}

// Живые форматы Gate.io WS v4 (подтверждены зондами 08.09.2026, аудит docs/07):
//
//	spot.tickers update     → result = ОДИНОЧНЫЙ объект (currency_pair, BBO, объём);
//	futures.book_ticker upd → result = одиночный объект {t,u,s,b,B,a,A} (BBO, без объёма);
//	futures.tickers update  → result = МАССИВ объектов (объём 24h, без BBO);
//	ack любой подписки      → result = объект {"status":"success"} или {"error":{…}}.
//
// Поэтому общий конверт несёт result как json.RawMessage и диспетчеризует по channel.
type gateWsEnvelope struct {
	Channel string          `json:"channel"`
	Event   string          `json:"event"`
	Result  json.RawMessage `json:"result"`
}

type gateAck struct {
	Status string `json:"status"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// Gate.io Spot ticker fields (spot.tickers, update)
type tickerData struct {
	CurrencyPair string `json:"currency_pair"` // "BTC_USDT"
	HighestBid   string `json:"highest_bid"`
	LowestAsk    string `json:"lowest_ask"`
	QuoteVolume  string `json:"quote_volume"`
}

// Gate.io Futures book_ticker (BBO, без объёма).
// B/A — размеры сторон: у фьючерсов Gate шлёт их числами, у спота строками;
// json.RawMessage принимает любой тип и не ломает декод (важно: без этих
// полей sonic кейс-инсенситивно матчит "B" в строковое поле "b" и падает).
type bookTickerData struct {
	Contract string          `json:"s"` // "BTC_USDT"
	Bid      string          `json:"b"`
	Ask      string          `json:"a"`
	BidSize  json.RawMessage `json:"B"`
	AskSize  json.RawMessage `json:"A"`
}

// Gate.io Futures tickers: только источник 24h quote-объёма (BBO отсутствует)
type futuresTickerData struct {
	Contract string `json:"contract"`         // "BTC_USDT"
	Volume   string `json:"volume_24h_quote"` // quote-currency turnover
}

type Adapter struct {
	droppedTicks    atomic.Uint64
	symbolsMu       sync.Mutex
	fundingSink     domain.FundingSink
	spotSymbols     []string
	futuresSymbols  []string
	symbolsLoadedAt time.Time
}

func NewAdapter() *Adapter { return &Adapter{} }
func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	if sink == nil {
		return fmt.Errorf("Gate funding sink is nil")
	}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		if err := a.pollFunding(ctx, sink); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ Gate funding poll: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

type gateFundingContract struct {
	Name             string `json:"name"`
	FundingRate      string `json:"funding_rate"`
	FundingNextApply int64  `json:"funding_next_apply"`
	Status           string `json:"status"`
}

func (a *Adapter) pollFunding(ctx context.Context, sink domain.FundingSink) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, futuresSymbolsURL, nil)
	if err != nil {
		return fmt.Errorf("pollFunding: %w", err)
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("pollFunding: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %s", r.Status)
	}
	var rows []gateFundingContract
	if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
		return fmt.Errorf("pollFunding: %w", err)
	}
	now := time.Now()
	for _, row := range rows {
		if row.Status != "trading" {
			continue
		}
		rate, err := decimal.NewFromString(row.FundingRate)
		if err != nil {
			continue
		}
		symbol := strings.ReplaceAll(row.Name, "_", "")
		if err := sink.UpdateFunding("GATEIO", symbol, rate, time.Unix(row.FundingNextApply, 0), now); err != nil {
			return fmt.Errorf("update GateIO funding %s: %w", symbol, err)
		}
	}
	return nil
}

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listenSpot(ctx, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listenFutures(ctx, out)
	return nil
}

func (a *Adapter) listenSpot(ctx context.Context, out chan<- domain.MarketTick) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
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
		if !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) listenFutures(ctx context.Context, out chan<- domain.MarketTick) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
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
		if !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) connectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	conn, _, err := dialer.DialContext(ctx, spotWS, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	defer conn.Close()

	log.Printf("✅ Gate.io Spot connected")

	symbols, err := a.getSymbols(ctx, true)
	if err != nil {
		return fmt.Errorf("fetch spot symbols: %w", err)
	}
	for i := 0; i < len(symbols); i += symbolBatchSize {
		end := i + symbolBatchSize
		if end > len(symbols) {
			end = len(symbols)
		}
		sub := wsRequest{Time: time.Now().Unix(), Channel: "spot.tickers", Event: "subscribe", Payload: symbols[i:end]}
		if err := conn.WriteJSON(sub); err != nil {
			return fmt.Errorf("subscribe spot batch: %w", err)
		}
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
		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("refresh GateIO read deadline: %w", err)
		}

		var env gateWsEnvelope
		if err := sonic.Unmarshal(msg, &env); err != nil {
			continue
		}

		// ✅ Фильтруем: нужны только update от ticker канала
		// Пропускаем: pong, subscribe-ack и другие служебные сообщения
		if env.Event != "update" || env.Channel != "spot.tickers" {
			continue
		}

		var t tickerData
		if err := sonic.Unmarshal(env.Result, &t); err != nil || t.CurrencyPair == "" {
			continue
		}

		now := time.Now()
		if tick, ok := spotTickerToTick(&t, now); ok {
			ingress.Submit(out, tick)
		}
	}
}

func (a *Adapter) connectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	conn, _, err := dialer.DialContext(ctx, futuresWS, nil)
	if err != nil {
		return fmt.Errorf("dial futures: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	defer conn.Close()

	// Мерж-состояние каналов: объём (futures.tickers) + BBO (futures.book_ticker).
	// Локально на соединение: при реконнекте снапшот tickers приходит первым.
	var volMu sync.RWMutex
	volByContract := make(map[string]decimal.Decimal)

	log.Printf("✅ Gate.io Futures connected")

	symbols, err := a.getSymbols(ctx, false)
	if err != nil {
		return fmt.Errorf("fetch futures symbols: %w", err)
	}
	// Два канала на одном соединении: book_ticker даёт BBO (без объёма),
	// tickers — 24h quote-объём (без BBO). Мержим по контракту в volByContract.
	for i := 0; i < len(symbols); i += symbolBatchSize {
		end := i + symbolBatchSize
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]
		if err := conn.WriteJSON(wsRequest{Time: time.Now().Unix(), Channel: "futures.book_ticker", Event: "subscribe", Payload: batch}); err != nil {
			return fmt.Errorf("subscribe futures book_ticker batch: %w", err)
		}
		if err := conn.WriteJSON(wsRequest{Time: time.Now().Unix(), Channel: "futures.tickers", Event: "subscribe", Payload: batch}); err != nil {
			return fmt.Errorf("subscribe futures tickers batch: %w", err)
		}
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
		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("refresh GateIO read deadline: %w", err)
		}

		var env gateWsEnvelope
		if err := sonic.Unmarshal(msg, &env); err != nil {
			continue
		}

		if env.Event != "update" {
			// ack-и и pong пропускаем, но ошибки подписки логируем.
			if env.Event == "subscribe" {
				var ack gateAck
				if sonic.Unmarshal(env.Result, &ack) == nil && ack.Error != nil {
					log.Printf("⚠️ Gate.io futures subscribe rejected: channel=%s code=%d msg=%s", env.Channel, ack.Error.Code, ack.Error.Message)
				}
			}
			continue
		}

		switch env.Channel {
		case "futures.tickers":
			// Обновляем 24h quote-объём по контрактам.
			var rows []futuresTickerData
			if err := sonic.Unmarshal(env.Result, &rows); err != nil {
				continue
			}
			for i := range rows {
				if rows[i].Contract == "" {
					continue
				}
				vol, err := decimal.NewFromString(rows[i].Volume)
				if err != nil || vol.IsNegative() {
					continue
				}
				volMu.Lock()
				volByContract[rows[i].Contract] = vol
				volMu.Unlock()
			}
			continue
		case "futures.book_ticker":
			// BBO-апдейт — источник эмиссии тика.
			var b bookTickerData
			if err := sonic.Unmarshal(env.Result, &b); err != nil || b.Contract == "" {
				continue
			}
			now := time.Now()
			if tick, ok := bookTickerToTick(&b, now); ok {
				volMu.RLock()
				vol := volByContract[b.Contract]
				volMu.RUnlock()
				if !vol.IsZero() {
					tick.QuoteVolume = vol
				}
				ingress.Submit(out, tick)
			}
			continue
		default:
			continue
		}
	}
}

func spotTickerToTick(d *tickerData, ts time.Time) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(d.HighestBid)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(d.LowestAsk)
	if err != nil || ask.IsZero() || bid.GreaterThan(ask) {
		return domain.MarketTick{}, false
	}

	qVol, err := decimal.NewFromString(d.QuoteVolume)
	if err != nil {
		return domain.MarketTick{}, false
	}

	return domain.MarketTick{
		Exchange:    "GATEIO",
		Symbol:      symbolReplacer.Replace(d.CurrencyPair),
		MarketType:  domain.MarketTypeSpot,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		EventTime:   ts,
		ReceivedAt:  time.Now(),
		Timestamp:   ts,
	}, true
}

// bookTickerToTick конвертирует BBO-апдейт futures.book_ticker в тик.
// QuoteVolume дозаполняется вызывающим кодом из futures.tickers-состояния.
func bookTickerToTick(d *bookTickerData, ts time.Time) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(d.Bid)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(d.Ask)
	if err != nil || ask.IsZero() || bid.GreaterThan(ask) {
		return domain.MarketTick{}, false
	}

	return domain.MarketTick{
		Exchange:    "GATEIO",
		Symbol:      symbolReplacer.Replace(d.Contract),
		MarketType:  domain.MarketTypeFutures,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: decimal.Zero, // дозаполняется из futures.tickers-состояния
		EventTime:   ts,
		ReceivedAt:  time.Now(),
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
				log.Printf("⚠️  %v", fmt.Errorf("GateIO %s keepalive: %w", pingChannel, err))
				return
			}
		}
	}
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}

func (a *Adapter) getSymbols(ctx context.Context, spot bool) ([]string, error) {
	a.symbolsMu.Lock()
	defer a.symbolsMu.Unlock()
	if time.Since(a.symbolsLoadedAt) < 5*time.Minute {
		if spot && len(a.spotSymbols) > 0 {
			return append([]string(nil), a.spotSymbols...), nil
		}
		if !spot && len(a.futuresSymbols) > 0 {
			return append([]string(nil), a.futuresSymbols...), nil
		}
	}
	url := futuresSymbolsURL
	if spot {
		url = spotSymbolsURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("getSymbols: %w", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("getSymbols: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("gate REST status %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("getSymbols: %w", err)
	}
	var symbols []string
	if spot {
		var rows []struct {
			ID          string `json:"id"`
			Quote       string `json:"quote"`
			TradeStatus string `json:"trade_status"`
		}
		if err := sonic.Unmarshal(body, &rows); err != nil {
			return nil, fmt.Errorf("getSymbols: %w", err)
		}
		for _, row := range rows {
			if row.Quote == "USDT" && (row.TradeStatus == "tradable" || row.TradeStatus == "trading" || row.TradeStatus == "") {
				symbols = append(symbols, row.ID)
			}
		}
	} else {
		var rows []struct {
			Name        string `json:"name"`
			Type        string `json:"type"`
			InDelisting bool   `json:"in_delisting"`
			Status      string `json:"status"`
		}
		if err := sonic.Unmarshal(body, &rows); err != nil {
			return nil, fmt.Errorf("getSymbols: %w", err)
		}
		for _, row := range rows {
			if row.Type == "direct" && !row.InDelisting && (row.Status == "open" || row.Status == "trading" || row.Status == "") {
				symbols = append(symbols, row.Name)
			}
		}
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("no USDT symbols returned")
	}
	if spot {
		a.spotSymbols = symbols
	} else {
		a.futuresSymbols = symbols
	}
	a.symbolsLoadedAt = time.Now()
	return append([]string(nil), symbols...), nil
}

func (a *Adapter) ConnectCandles(ctx context.Context, sink domain.CandleSink) error {
	go a.runCandleFeed(ctx, sink, true)
	go a.runCandleFeed(ctx, sink, false)
	<-ctx.Done()
	return nil
}

type gateCandleResponse struct {
	Channel string          `json:"channel"`
	Event   string          `json:"event"`
	Result  json.RawMessage `json:"result"`
	TimeMS  int64           `json:"time_ms"`
}
type gateSpotCandle struct {
	T int64  `json:"t"`
	N string `json:"n"`
	V string `json:"v"`
	W bool   `json:"w"`
}
type gateFuturesCandle struct {
	T int64  `json:"t"`
	N string `json:"n"`
	A string `json:"a"`
	V string `json:"v"`
}

func (a *Adapter) runCandleFeed(ctx context.Context, sink domain.CandleSink, spot bool) {
	url := spotWS
	channel := "spot.candlesticks"
	market := domain.MarketTypeSpot
	if !spot {
		url = futuresWS
		channel = "futures.candlesticks"
		market = domain.MarketTypeFutures
	}
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		symbols, err := a.getSymbols(ctx, spot)
		if err != nil {
			log.Printf("⚠️ Gate %s candle symbols: %v", market, err)
			select {
			case <-ctx.Done():
				return
			case <-retry.After(reconnectDelay):
			}
			continue
		}
		if err := a.readCandleShard(ctx, sink, url, channel, symbols, market); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ Gate %s candle WS: %v", market, err)
		}
		if ctx.Err() == nil && !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) readCandleShard(ctx context.Context, sink domain.CandleSink, url, channel string, symbols []string, market domain.MarketType) error {
	conn, _, err := dialer.DialContext(ctx, url, nil)
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
	for _, sym := range symbols {
		if err := conn.WriteJSON(wsRequest{Time: time.Now().Unix(), Channel: channel, Event: "subscribe", Payload: []string{"1m", sym}}); err != nil {
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
		var r gateCandleResponse
		if err := sonic.Unmarshal(msg, &r); err != nil {
			continue
		}
		if r.Event == "error" {
			return fmt.Errorf("GateIO candle subscription rejected on %s: %s", channel, string(r.Result))
		}
		if r.Event != "update" || r.Channel != channel {
			continue
		}
		var symbol string
		var t int64
		var quote string
		var closed bool
		if market == domain.MarketTypeSpot {
			var d gateSpotCandle
			if err := json.Unmarshal(r.Result, &d); err != nil {
				continue
			}
			symbol = d.N
			t = d.T
			quote = d.V
			closed = d.W
		} else {
			var ds []gateFuturesCandle
			if err := json.Unmarshal(r.Result, &ds); err != nil {
				continue
			}
			if len(ds) == 0 {
				continue
			}
			d := ds[0]
			symbol = d.N
			t = d.T
			quote = d.A
		}
		if symbol == "" || t == 0 {
			continue
		}
		v, err := decimal.NewFromString(quote)
		if err != nil || v.IsNegative() {
			continue
		}
		parts := strings.SplitN(symbol, "_", 2)
		if len(parts) != 2 {
			continue
		}
		sym := parts[1]
		et := time.Now()
		if r.TimeMS > 0 {
			et = time.UnixMilli(r.TimeMS)
		}
		if err := sink.UpdateCandle(domain.MarketCandle{Exchange: "GATEIO", Symbol: strings.ReplaceAll(sym, "_", ""), MarketType: market, OpenTime: time.Unix(t, 0), CloseTime: time.Unix(t, 0).Add(time.Minute - time.Millisecond), QuoteVolume: v, EventTime: et, Closed: closed}); err != nil {
			return fmt.Errorf("readCandleShard: %w", err)
		}
	}
}
