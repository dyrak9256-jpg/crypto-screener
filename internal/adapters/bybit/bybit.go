package bybit

import (
	"context"
	"crypto-screener/internal/domain"
	"crypto-screener/internal/ingress"
	"crypto-screener/internal/retry"
	"crypto-screener/internal/symbolscache"
	"crypto-screener/internal/wsutil"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
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
	spotWS    = "wss://stream.bybit.com/v5/public/spot"
	futuresWS = "wss://stream.bybit.com/v5/public/linear"

	spotSymbolsURL    = "https://api.bybit.com/v5/market/instruments-info?category=spot&status=Trading"
	futuresSymbolsURL = "https://api.bybit.com/v5/market/instruments-info?category=linear&status=Trading"

	handshakeTimeout    = 10 * time.Second
	pingInterval        = 20 * time.Second
	pongWait            = 10 * time.Second
	reconnectDelay      = 3 * time.Second
	spotSubBatchSize    = 10
	futuresSubBatchSize = 10

	// Bybit допускает не более 10 подписочных запросов/сек на соединение
	// (v5 public WS): батчи отправляются с паузой, иначе биржа молча
	// отклоняет всё после первой десятки (зонд 08.09.2026).
	bybitSubInterval = 110 * time.Millisecond

	// Эмпирические лимиты Bybit WS из дата-центровых сетей (зонды 08.09.2026):
	// 1) не более 10 аргументов на подписочный запрос (батч 20 отклоняется
	//    целиком — это документированный лимит спота);
	// 2) соединение принимает суммарно лишь ~10-15 подписок, остальные
	//    молча отклоняются (анти-абьюз CloudFront).
	// Поэтому: шард по 10 аргументам на соединение + лимит числа символов
	// (BYBIT_SYMBOL_LIMIT, по умолчанию 100 на рынок) — соединения остаются
	// в разумном числе, а обрезка идёт по ликвидности (seed упорядочен).
	bybitMaxArgsPerConn = 10
	bybitDefaultSymCap  = 100
)

// bybitSymbolLimit возвращает лимит числа символов Bybit на рынок.
func bybitSymbolLimit() int {
	if v := strings.TrimSpace(os.Getenv("BYBIT_SYMBOL_LIMIT")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return bybitDefaultSymCap
}

// writeSubscribeBatches отправляет батчи подписок с троттлингом
// bybitSubInterval (≤9 запросов/сек). Прерывается по ctx.
func writeSubscribeBatches(ctx context.Context, conn *websocket.Conn, args []string, batch int, opLabel string) error {
	for i := 0; i < len(args); i += batch {
		end := i + batch
		if end > len(args) {
			end = len(args)
		}
		if i > 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("%s: подписки прерваны (%d из %d)", opLabel, i, len(args))
			case <-time.After(bybitSubInterval):
			}
		}
		if err := conn.WriteJSON(subscribeMsg{Op: "subscribe", Args: args[i:end]}); err != nil {
			return fmt.Errorf("%s batch [%d:%d]: %w", opLabel, i, end, err)
		}
	}
	return nil
}

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
	Ts    int64           `json:"ts"`
	Data  json.RawMessage `json:"data"`
}

// pongResponse: для обработки JSON pong от Bybit
type pongResponse struct {
	Op string `json:"op"` // "pong"
}

type tickerPayload struct {
	Symbol          string `json:"symbol"`
	Bid1            string `json:"bid1Price"`
	Ask1            string `json:"ask1Price"`
	TurnOver        string `json:"turnover24h"`
	FundingRate     string `json:"fundingRate"`
	NextFundingTime string `json:"nextFundingTime"`
}

type Adapter struct {
	client       *http.Client
	fundingSink  atomic.Value
	fundingReady chan struct{}
	fundingOnce  sync.Once
}

func NewAdapter() *Adapter {
	return &Adapter{client: &http.Client{Timeout: 15 * time.Second}, fundingReady: make(chan struct{})}
}

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, spotWS, domain.MarketTypeSpot, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	select {
	case <-a.fundingReady:
	case <-ctx.Done():
		return nil
	}
	a.listen(ctx, futuresWS, domain.MarketTypeFutures, out)
	return nil
}
func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	if sink == nil {
		return fmt.Errorf("Bybit funding sink is nil")
	}
	a.fundingSink.Store(sink)
	a.fundingOnce.Do(func() { close(a.fundingReady) })
	<-ctx.Done()
	return nil
}

func (a *Adapter) listen(
	ctx context.Context,
	url string,
	mType domain.MarketType,
	out chan<- domain.MarketTick,
) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for {
		startedAt := time.Now()
		if ctx.Err() != nil {
			return
		}
		if err := a.connectAndRead(ctx, url, mType, out); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  Bybit %s WS: %v — reconnecting in %s", mType, err, reconnectDelay)
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
	wsURL string,
	mType domain.MarketType,
	out chan<- domain.MarketTick,
) error {
	restURL := futuresSymbolsURL
	if mType == domain.MarketTypeSpot {
		restURL = spotSymbolsURL
	}
	symbols := a.symbolsWithFallback(ctx, restURL, mType)
	args := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		args = append(args, "tickers."+symbol)
	}
	shards := shardArgsMax(args, 16000, bybitMaxArgsPerConn)
	if len(shards) == 0 {
		return fmt.Errorf("no %s ticker subscriptions", mType)
	}
	log.Printf("📋 Bybit %s: %d symbols across %d connections", mType, len(symbols), len(shards))

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, len(shards))
	var wg sync.WaitGroup
	for _, shard := range shards {
		shard := append([]string(nil), shard...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.readTickerShard(connCtx, wsURL, mType, shard, out); err != nil && connCtx.Err() == nil {
				errCh <- err
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if mType == domain.MarketTypeFutures {
			if v := a.fundingSink.Load(); v != nil {
				v.(domain.FundingSink).SetStreamHealth("BYBIT", false)
			}
		}
		return fmt.Errorf("connectAndRead: %w", err)
	}
	return nil
}

func shardArgs(args []string, maxChars int) [][]string {
	return shardArgsMax(args, maxChars, 0)
}

// shardArgsMax режет аргументы на группы не только по объёму JSON, но и по
// числу: Bybit-соединение принимает ~100 аргументов подписки суммарно
// (эмпирика 08.09.2026: всё после первой сотни молча отклоняется).
func shardArgsMax(args []string, maxChars, maxArgs int) [][]string {
	if maxChars <= 0 {
		maxChars = 16000
	}
	var shards [][]string
	current := make([]string, 0, 64)
	currentSize := 0
	for _, arg := range args {
		extra := len(arg) + 3 // quotes, comma and JSON framing overhead
		if len(current) > 0 && (currentSize+extra > maxChars || (maxArgs > 0 && len(current) >= maxArgs)) {
			shards = append(shards, current)
			current = make([]string, 0, 64)
			currentSize = 0
		}
		current = append(current, arg)
		currentSize += extra
	}
	if len(current) > 0 {
		shards = append(shards, current)
	}
	return shards
}

func (a *Adapter) readTickerShard(ctx context.Context, wsURL string, mType domain.MarketType, args []string, out chan<- domain.MarketTick) error {
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial ticker shard: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)

	batchSize := futuresSubBatchSize
	if mType == domain.MarketTypeSpot {
		batchSize = spotSubBatchSize
	}
	if err := writeSubscribeBatches(ctx, conn, args, batchSize, "subscribe ticker"); err != nil {
		return err
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go closeOnCtx(connCtx, conn)
	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set ticker read deadline: %w", err)
	}
	go bybitKeepAlive(connCtx, conn)

	cache := make(map[string]*tickerPayload, len(args))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read ticker shard: %w", err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
			return fmt.Errorf("refresh ticker read deadline: %w", err)
		}

		var resp wsResponse
		if err := sonic.Unmarshal(msg, &resp); err != nil {
			continue
		}
		if resp.Topic == "" {
			continue
		}
		var delta tickerPayload
		if err := sonic.Unmarshal(resp.Data, &delta); err != nil || delta.Symbol == "" {
			continue
		}
		current := cache[delta.Symbol]
		if current == nil {
			current = &tickerPayload{Symbol: delta.Symbol}
			cache[delta.Symbol] = current
		}
		if delta.Bid1 != "" {
			current.Bid1 = delta.Bid1
		}
		if delta.Ask1 != "" {
			current.Ask1 = delta.Ask1
		}
		if delta.TurnOver != "" {
			current.TurnOver = delta.TurnOver
		}
		if delta.FundingRate != "" {
			current.FundingRate = delta.FundingRate
		}
		if delta.NextFundingTime != "" {
			current.NextFundingTime = delta.NextFundingTime
		}

		eventTime := time.Now()
		if resp.Ts > 0 {
			eventTime = time.UnixMilli(resp.Ts)
		}
		if mType == domain.MarketTypeFutures && current.FundingRate != "" {
			if rate, parseErr := decimal.NewFromString(current.FundingRate); parseErr == nil {
				var next time.Time
				if ms, parseErr := strconv.ParseInt(current.NextFundingTime, 10, 64); parseErr == nil && ms > 0 {
					next = time.UnixMilli(ms)
				}
				if v := a.fundingSink.Load(); v != nil {
					if err := v.(domain.FundingSink).UpdateFunding("BYBIT", current.Symbol, rate, next, eventTime); err != nil {
						return fmt.Errorf("update Bybit funding for %s: %w", current.Symbol, err)
					}
				}
			}
		}
		tick, ok := toMarketTick(current, mType, eventTime)
		if !ok {
			continue
		}
		ingress.Submit(out, tick)
	}
}

// fetchSymbols получает список торгующихся символов через REST
func (a *Adapter) fetchSymbols(ctx context.Context, endpoint string) ([]string, error) {
	var symbols []string
	cursor := ""
	for {
		reqURL := endpoint
		if cursor != "" {
			reqURL += "&cursor=" + url.QueryEscape(cursor)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("fetchSymbols: %w", err)
		}
		resp, err := a.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetchSymbols: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("bybit REST status %s", resp.Status)
		}
		var parsed struct {
			RetCode int    `json:"retCode"`
			RetMsg  string `json:"retMsg"`
			Result  struct {
				List []struct {
					Symbol string `json:"symbol"`
					Status string `json:"status"`
				} `json:"list"`
				NextPageCursor string `json:"nextPageCursor"`
			} `json:"result"`
		}
		if err := sonic.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("parse response: %w", err)
		}
		if parsed.RetCode != 0 {
			return nil, fmt.Errorf("bybit API error %d: %s", parsed.RetCode, parsed.RetMsg)
		}
		for _, item := range parsed.Result.List {
			if item.Status != "Trading" {
				continue
			}
			if strings.Contains(endpoint, "category=spot") || strings.Contains(endpoint, "category=linear") {
				if !strings.HasSuffix(item.Symbol, "USDT") {
					continue
				}
			}
			symbols = append(symbols, item.Symbol)
		}
		if parsed.Result.NextPageCursor == "" || parsed.Result.NextPageCursor == cursor {
			break
		}
		cursor = parsed.Result.NextPageCursor
	}
	return symbols, nil
}

// symbolsWithFallback: REST (api.bybit.com → резервный api.bytick.com) →
// дисковый кэш → встроенный seed. Bybit гео-блокирует REST (CloudFront 403)
// в ряде регионов, при этом публичные WS-потоки доступны: список символов —
// единственное, что требует REST, поэтому фолбэк полностью оживляет биржу.
// Успешный REST-запрос обновляет кэш на диске (SYMBOLS_CACHE_DIR).
func (a *Adapter) symbolsWithFallback(ctx context.Context, restURL string, mType domain.MarketType) []string {
	cacheName, seed := "bybit-linear", seedLinearSymbols
	if mType == domain.MarketTypeSpot {
		cacheName, seed = "bybit-spot", seedSpotSymbols
	}
	symbols, restErr := a.fetchSymbols(ctx, restURL)
	if restErr != nil {
		// Резервный хост API (тот же CloudFront-контур, иная гео-политика).
		if alt := strings.Replace(restURL, "https://api.bybit.com", "https://api.bytick.com", 1); alt != restURL {
			symbols, restErr = a.fetchSymbols(ctx, alt)
		}
	}
	if restErr == nil {
		if err := symbolscache.Save(cacheName, symbols); err != nil {
			log.Printf("⚠️  Bybit %s: кэш символов не записан: %v", mType, err)
		}
		return capBybitSymbols(symbols, mType, "REST")
	}
	if cached, err := symbolscache.Load(cacheName); err == nil {
		log.Printf("📋 Bybit %s: REST недоступен (%v) — %d символов из дискового кэша", mType, restErr, len(cached))
		return capBybitSymbols(cached, mType, "кэш")
	}
	log.Printf("📋 Bybit %s: REST недоступен (%v), кэша нет — %d символов из встроенного seed", mType, restErr, len(seed))
	return capBybitSymbols(seed, mType, "seed")
}

// capBybitSymbols обрезает список до BYBIT_SYMBOL_LIMIT: в дата-центровых
// сетях Bybit принимает лишь ~10-15 подписок на соединение, поэтому сотни
// символов всё равно не подписались бы. Порядок списка — по ликвидности.
func capBybitSymbols(symbols []string, mType domain.MarketType, source string) []string {
	limit := bybitSymbolLimit()
	if len(symbols) <= limit {
		return symbols
	}
	capped := append([]string(nil), symbols[:limit]...)
	log.Printf("📋 Bybit %s: %d → %d символов (лимит BYBIT_SYMBOL_LIMIT, источник %s; порядок — по ликвидности)", mType, len(symbols), limit, source)
	return capped
}

// ConnectCandles subscribes only to 1-minute klines. Higher timeframes are
// calculated locally from these minute buckets, so the exchange sends no
// duplicate 5m/15m/30m streams.
func bybitKeepAlive(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := conn.WriteJSON(pingMsg{Op: "ping"}); err != nil {
				log.Printf("⚠️  %v", fmt.Errorf("Bybit keepalive: %w", err))
				return
			}
		}
	}
}

func (a *Adapter) ConnectCandles(ctx context.Context, sink domain.CandleSink) error {
	for _, market := range []domain.MarketType{domain.MarketTypeSpot, domain.MarketTypeFutures} {
		go a.runCandleFeed(ctx, sink, market)
	}
	<-ctx.Done()
	return nil
}

type bybitCandleResponse struct {
	Op      string `json:"op"`
	Success bool   `json:"success"`
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Topic   string `json:"topic"`
	Ts      int64  `json:"ts"`
	Data    []struct {
		Start     int64  `json:"start"`
		End       int64  `json:"end"`
		Turnover  string `json:"turnover"`
		Confirm   bool   `json:"confirm"`
		Timestamp int64  `json:"timestamp"`
	} `json:"data"`
}

func (a *Adapter) runCandleFeed(ctx context.Context, sink domain.CandleSink, market domain.MarketType) {
	url := futuresWS
	rest := futuresSymbolsURL
	batch := futuresSubBatchSize
	if market == domain.MarketTypeSpot {
		url = spotWS
		rest = spotSymbolsURL
		batch = spotSubBatchSize
	}
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		// Фолбэк REST → кэш → seed (см. symbolsWithFallback): гео-блок REST
		// не должен останавливать и свечной фид.
		symbols := a.symbolsWithFallback(ctx, rest, market)
		if err := a.readCandleShard(ctx, sink, url, symbols, market, batch); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ Bybit %s candle WS: %v", market, err)
		}
		if ctx.Err() == nil && !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) readCandleShard(ctx context.Context, sink domain.CandleSink, url string, symbols []string, market domain.MarketType, batch int) error {
	args := make([]string, 0, len(symbols))
	for _, s := range symbols {
		args = append(args, "kline.1."+s)
	}
	shards := shardArgsMax(args, 16000, bybitMaxArgsPerConn)
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, len(shards))
	var wg sync.WaitGroup
	for _, shard := range shards {
		shard := append([]string(nil), shard...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.readCandleConnection(connCtx, sink, url, shard, market, batch); err != nil && connCtx.Err() == nil {
				errCh <- err
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return fmt.Errorf("readCandleShard: %w", err)
	}
	return nil
}

func (a *Adapter) readCandleConnection(ctx context.Context, sink domain.CandleSink, url string, args []string, market domain.MarketType, batch int) error {
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial candle shard: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := wsutil.StartHeartbeat(connCtx, conn, 20*time.Second, 60*time.Second); err != nil {
		return fmt.Errorf("start websocket heartbeat: %w", err)
	}
	go closeOnCtx(connCtx, conn)
	if err := writeSubscribeBatches(connCtx, conn, args, batch, "subscribe candle"); err != nil {
		return err
	}
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read candle shard: %w", err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
			return fmt.Errorf("refresh candle read deadline: %w", err)
		}
		var r bybitCandleResponse
		if err := sonic.Unmarshal(msg, &r); err != nil {
			continue
		}
		if r.Op == "subscribe" {
			if r.RetCode != 0 || !r.Success {
				// Точечный отказ (например, kline-топик по символу, которого нет
				// на площадке: seed-список/кэш шире реального листинга). Валидные
				// подписки батча продолжают стримить — соединение не рвём,
				// иначе одна невалидная пара убивала бы весь свечной фид.
				log.Printf("⚠️ Bybit %s candle: часть подписок отклонена (code=%d msg=%q)", market, r.RetCode, r.RetMsg)
			}
			continue
		}
		if r.RetCode != 0 {
			return fmt.Errorf("Bybit candle websocket error: code=%d msg=%q", r.RetCode, r.RetMsg)
		}
		if len(r.Data) == 0 || r.Topic == "" {
			continue
		}
		parts := strings.Split(r.Topic, ".")
		if len(parts) != 3 {
			continue
		}
		symbol := parts[2]
		for _, d := range r.Data {
			q, err := decimal.NewFromString(d.Turnover)
			if err != nil || q.IsNegative() {
				continue
			}
			et := time.UnixMilli(d.Timestamp)
			if d.Timestamp == 0 {
				et = time.UnixMilli(r.Ts)
			}
			if err := sink.UpdateCandle(domain.MarketCandle{Exchange: "BYBIT", Symbol: symbol, MarketType: market, OpenTime: time.UnixMilli(d.Start), CloseTime: time.UnixMilli(d.End), QuoteVolume: q, EventTime: et, Closed: d.Confirm}); err != nil {
				return fmt.Errorf("update Bybit candle %s: %w", symbol, err)
			}
		}
	}
}

func toMarketTick(p *tickerPayload, mType domain.MarketType, ts time.Time) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(p.Bid1)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(p.Ask1)
	if err != nil || ask.IsZero() || bid.GreaterThan(ask) {
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
		EventTime:   ts,
		ReceivedAt:  time.Now(),
		Timestamp:   ts,
	}, true
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}
