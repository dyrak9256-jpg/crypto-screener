package bingx

import (
	"bytes"
	"compress/gzip"
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
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	spotWS    = "wss://open-api-ws.bingx.com/market"
	futuresWS = "wss://open-api-swap.bingx.com/swap-market"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 5 * time.Second
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var (
	dialer     = websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	httpClient = &http.Client{Timeout: 10 * time.Second}
)

// ✅ Pool для переиспользования gzip.Reader
// gzip.NewReader аллоцирует буферы при каждом вызове
// При сотнях сообщений/сек это создаёт серьёзное давление на GC
var gzipReaderPool = sync.Pool{
	New: func() any {
		// Создаём reader с пустым источником
		// Reset() будет вызван перед каждым использованием
		return new(gzip.Reader)
	},
}

type subscribeMsg struct {
	ID       string `json:"id"`
	ReqType  string `json:"reqType"`
	DataType string `json:"dataType"`
}

// @bookTicker: BBO без объёма (одинаковый формат на споте и свопе).
// b/a — строки у свопа, у спота встречаются числа; decimal парсит оба
// варианта. B/A (размеры заявок) объявлены явно: без них sonic
// кейс-инсенситивно матчит "B" в поле "b" и падает на числе.
type bingxBookTicker struct {
	Event     string          `json:"e"` // "bookTicker"
	EventTime int64           `json:"E"`
	Symbol    string          `json:"s"`
	Bid       decimal.Decimal `json:"b"`
	Ask       decimal.Decimal `json:"a"`
	BidSize   json.RawMessage `json:"B"`
	AskSize   json.RawMessage `json:"A"`
}

// @ticker: 24hTicker с quote-объёмом (поле q), без BBO — объём мержится
// с BBO из @bookTicker.
type bingxDayTicker struct {
	Event       string          `json:"e"` // "24hTicker"
	EventTime   int64           `json:"E"`
	Symbol      string          `json:"s"`
	QuoteVolume decimal.Decimal `json:"q"`
}

type Adapter struct {
	writeMu sync.Mutex
}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	if sink == nil {
		return fmt.Errorf("BingX funding sink is nil")
	}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		if err := a.pollFunding(ctx, sink); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ BingX funding poll: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
func (a *Adapter) pollFunding(ctx context.Context, sink domain.FundingSink) error {
	// Эндпоинт fundingRate без параметра symbol перестал отвечать данными
	// (код 109400). PremiumIndex возвращает текущую ставку и время следующего
	// сеттла сразу по всем контрактам — подтверждено живым API 08.09.2026.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex", nil)
	if err != nil {
		return fmt.Errorf("pollFunding: %w", err)
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("pollFunding: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s", r.Status)
	}
	var x struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return fmt.Errorf("pollFunding: %w", err)
	}
	if x.Code != 0 {
		return fmt.Errorf("API code %d", x.Code)
	}
	type row struct {
		Symbol          string `json:"symbol"`
		LastFundingRate string `json:"lastFundingRate"`
		NextFundingTime int64  `json:"nextFundingTime"`
	}
	var rows []row
	trim := bytes.TrimSpace(x.Data)
	if len(trim) > 0 && trim[0] == '[' {
		if err := json.Unmarshal(trim, &rows); err != nil {
			return fmt.Errorf("pollFunding: %w", err)
		}
	} else {
		var one row
		if err := json.Unmarshal(trim, &one); err != nil {
			return fmt.Errorf("pollFunding: %w", err)
		}
		rows = []row{one}
	}
	now := time.Now()
	for _, v := range rows {
		rate, err := decimal.NewFromString(v.LastFundingRate)
		if err != nil {
			continue
		}
		sym := strings.ReplaceAll(v.Symbol, "-", "")
		if err := sink.UpdateFunding("BINGX", sym, rate, time.UnixMilli(v.NextFundingTime), now); err != nil {
			return fmt.Errorf("update BingX funding %s: %w", sym, err)
		}
	}
	return nil
}

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.runTickerFeed(ctx, out, true)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	a.runTickerFeed(ctx, out, false)
	return nil
}

// bingxShardSymbols — символов на одно WS-соединение. Мульти-символьные
// dataType (через пробел) не поддерживаются, старые каналы spot.tickers /
// swap.tickers мертвы (зонды 08.09.2026): подписка строго по одному символу
// на сообщение, поэтому соединения шардированы.
const bingxShardSymbols = 100

// runTickerFeed загружает список символов и раскладывает по шардам; каждый
// шард — своё соединение со своим reconnect-циклом (по образцу свечей).
func (a *Adapter) runTickerFeed(ctx context.Context, out chan<- domain.MarketTick, spot bool) {
	mType := domain.MarketTypeFutures
	if spot {
		mType = domain.MarketTypeSpot
	}
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		symbols, err := bingxTickerSymbols(ctx, spot)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("⚠️  BingX %s ticker symbols: %v", mType, err)
			}
			if !backoff.Wait(ctx.Done()) {
				return
			}
			continue
		}
		shards := (len(symbols) + bingxShardSymbols - 1) / bingxShardSymbols
		log.Printf("✅ BingX %s: %d symbols → %d WS shards", mType, len(symbols), shards)
		var wg sync.WaitGroup
		for i := 0; i < len(symbols); i += bingxShardSymbols {
			end := min(i+bingxShardSymbols, len(symbols))
			part := append([]string(nil), symbols[i:end]...)
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.runTickerShard(ctx, out, mType, part)
			}()
		}
		wg.Wait()
		if ctx.Err() != nil {
			return
		}
		// Все шарды закрылись при живом контексте — пересобираем с backoff.
		if !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) runTickerShard(ctx context.Context, out chan<- domain.MarketTick, mType domain.MarketType, symbols []string) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		startedAt := time.Now()
		if err := a.readTickerShard(ctx, out, mType, symbols); err != nil && ctx.Err() == nil {
			log.Printf("⚠️  BingX %s ticker WS: %v — reconnecting in %s", mType, err, reconnectDelay)
		}
		if time.Since(startedAt) >= 30*time.Second {
			backoff.Reset()
		}
		if !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) readTickerShard(
	ctx context.Context,
	out chan<- domain.MarketTick,
	mType domain.MarketType,
	symbols []string,
) error {
	url := spotWS
	if mType == domain.MarketTypeFutures {
		url = futuresWS
	}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	defer conn.Close()

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go closeOnCtx(connCtx, conn)

	// Подписка: один символ на сообщение, оба канала — @bookTicker (BBO)
	// и @ticker (24h quote-объём).
	id := time.Now().UnixNano()
	for _, sym := range symbols {
		for _, channel := range []string{"bookTicker", "ticker"} {
			sub := subscribeMsg{
				ID:       fmt.Sprintf("sub-%d", id),
				ReqType:  "sub",
				DataType: sym + "@" + channel,
			}
			id++
			if err := a.writeJSON(conn, sub); err != nil {
				return fmt.Errorf("subscribe %s: %w", sub.DataType, err)
			}
		}
	}
	log.Printf("✅ BingX %s ticker shard: %d symbols", mType, len(symbols))

	// Мерж объёма: @ticker (q) держим по символу, @bookTicker эмиссирует
	// тик с этим объёмом (до первого @ticker объём нулевой).
	var volMu sync.RWMutex
	volBySymbol := make(map[string]decimal.Decimal, len(symbols))

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
			return fmt.Errorf("refresh BingX read deadline: %w", err)
		}

		data, err := decompressGzip(msg)
		if err != nil {
			data = msg // не gzip — служебный фрейм
		}

		strData := string(data)

		// BingX сам инициирует Ping и рвёт соединение без Pong.
		if strData == "Ping" || strings.Contains(strData, `"ping"`) {
			if err := a.writeMessage(conn, websocket.TextMessage, []byte("Pong")); err != nil {
				return fmt.Errorf("write pong: %w", err)
			}
			continue
		}

		var raw struct {
			DataType string          `json:"dataType"`
			Data     json.RawMessage `json:"data"`
		}
		if err := sonic.Unmarshal(data, &raw); err != nil || raw.DataType == "" {
			continue
		}

		now := time.Now()
		switch {
		case strings.HasSuffix(raw.DataType, "@bookTicker"):
			var b bingxBookTicker
			if err := sonic.Unmarshal(raw.Data, &b); err != nil {
				continue
			}
			volMu.RLock()
			vol := volBySymbol[b.Symbol]
			volMu.RUnlock()
			if tick, ok := bboToTick(&b, mType, now, vol); ok {
				ingress.Submit(out, tick)
			}
		case strings.HasSuffix(raw.DataType, "@ticker"):
			var d bingxDayTicker
			if err := sonic.Unmarshal(raw.Data, &d); err != nil || d.Symbol == "" {
				continue
			}
			volMu.Lock()
			volBySymbol[d.Symbol] = d.QuoteVolume
			volMu.Unlock()
		}
	}
}

func bboToTick(b *bingxBookTicker, mType domain.MarketType, ts time.Time, vol decimal.Decimal) (domain.MarketTick, bool) {
	if b.Symbol == "" || b.Bid.IsZero() || b.Ask.IsZero() || b.Bid.GreaterThan(b.Ask) {
		return domain.MarketTick{}, false
	}

	// Нормализация: "BTC-USDT" → "BTCUSDT"
	symbol := strings.ReplaceAll(b.Symbol, "-", "")
	if !strings.HasSuffix(strings.ToUpper(symbol), "USDT") {
		return domain.MarketTick{}, false
	}

	return domain.MarketTick{
		Exchange:    "BINGX",
		Symbol:      symbol,
		MarketType:  mType,
		BestBid:     b.Bid,
		BestAsk:     b.Ask,
		QuoteVolume: vol,
		EventTime:   ts,
		ReceivedAt:  time.Now(),
		Timestamp:   ts,
	}, true
}

// ✅ Исправленный порядок: Close → Put (не Put → Close)
func decompressGzip(data []byte) ([]byte, error) {
	gr := gzipReaderPool.Get().(*gzip.Reader)

	if err := gr.Reset(bytes.NewReader(data)); err != nil {
		gzipReaderPool.Put(gr)
		return nil, fmt.Errorf("decompressGzip: %w", err)
	}

	result, err := io.ReadAll(io.LimitReader(gr, 4<<20))

	// ✅ Сначала Close, потом Put обратно в пул
	// Иначе: объект уже в пуле, но defer gr.Close() его модифицирует
	gr.Close()
	gzipReaderPool.Put(gr)

	return result, err
}

func (a *Adapter) writeMessage(conn *websocket.Conn, messageType int, data []byte) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if err := conn.WriteMessage(messageType, data); err != nil {
		return fmt.Errorf("write websocket message: %w", err)
	}
	return nil
}

func (a *Adapter) writeJSON(conn *websocket.Conn, v any) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if err := conn.WriteJSON(v); err != nil {
		return fmt.Errorf("write websocket JSON: %w", err)
	}
	return nil
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}

func (a *Adapter) ConnectCandles(ctx context.Context, sink domain.CandleSink) error {
	go a.runCandleFeed(ctx, sink, true)
	go a.runCandleFeed(ctx, sink, false)
	<-ctx.Done()
	return nil
}

type bingxCandleMessage struct {
	Code     int    `json:"code"`
	Msg      string `json:"msg"`
	DataType string `json:"dataType"`
	Data     struct {
		EventTime int64  `json:"E"`
		Symbol    string `json:"s"`
		K         struct {
			Start int64  `json:"t"`
			End   int64  `json:"T"`
			Quote string `json:"q"`
		} `json:"K"`
	} `json:"data"`
}

// BingX сменил формат /spot/v1/common/symbols (зонд 08.09.2026): список
// вложен в data.symbols, а status — число (1 = активна, 0/10/25 — нет).
type bingxSpotSymbolsResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		Symbols []bingxSpotSymbol `json:"symbols"`
	} `json:"data"`
}

type bingxSpotSymbol struct {
	Symbol string `json:"symbol"`
	Status int    `json:"status"`
}
type bingxSwapContractsResponse struct {
	Code int `json:"code"`
	Data []struct {
		Symbol string `json:"symbol"`
		Status int    `json:"status"`
	} `json:"data"`
}

func (a *Adapter) runCandleFeed(ctx context.Context, sink domain.CandleSink, spot bool) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		symbols, err := bingxCandleSymbols(ctx, spot)
		if err != nil {
			log.Printf("⚠️ BingX candle symbols: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-retry.After(reconnectDelay):
			}
			continue
		}
		var wg sync.WaitGroup
		for i := 0; i < len(symbols); i += 50 {
			end := i + 50
			if end > len(symbols) {
				end = len(symbols)
			}
			part := append([]string(nil), symbols[i:end]...)
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.runBingXCandleShard(ctx, sink, part, spot)
			}()
		}
		wg.Wait()
		if ctx.Err() == nil && !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func bingxCandleSymbols(ctx context.Context, spot bool) ([]string, error) {
	url := "https://open-api.bingx.com/openApi/spot/v1/common/symbols"
	if !spot {
		url = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("bingxCandleSymbols: %w", err)
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bingxCandleSymbols: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return nil, fmt.Errorf("bingx candle symbols HTTP %s", r.Status)
	}
	if spot {
		var x bingxSpotSymbolsResponse
		if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
			return nil, fmt.Errorf("bingxCandleSymbols: %w", err)
		}
		if x.Code != 0 {
			return nil, fmt.Errorf("bingx candle symbols API code %d", x.Code)
		}
		out := make([]string, 0, len(x.Data.Symbols))
		for _, v := range x.Data.Symbols {
			if v.Status == 1 {
				out = append(out, v.Symbol)
			}
		}
		return out, nil
	}
	var x bingxSwapContractsResponse
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return nil, fmt.Errorf("bingxCandleSymbols: %w", err)
	}
	if x.Code != 0 {
		return nil, fmt.Errorf("bingx candle symbols API code %d", x.Code)
	}
	out := make([]string, 0, len(x.Data))
	for _, v := range x.Data {
		if v.Status == 1 {
			out = append(out, v.Symbol)
		}
	}
	return out, nil
}

// bingxTickerSymbols возвращает активные USDT-пары для тикерных подписок:
// спот — data.symbols (status==1, суффикс -USDT), фьючерсы — swap contracts.
func bingxTickerSymbols(ctx context.Context, spot bool) ([]string, error) {
	if !spot {
		all, err := bingxCandleSymbols(ctx, false)
		if err != nil {
			return nil, fmt.Errorf("bingxTickerSymbols: %w", err)
		}
		out := make([]string, 0, len(all))
		for _, s := range all {
			if strings.HasSuffix(s, "-USDT") {
				out = append(out, s)
			}
		}
		return out, nil
	}
	url := "https://open-api.bingx.com/openApi/spot/v1/common/symbols"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("bingxTickerSymbols: %w", err)
	}
	r, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bingxTickerSymbols: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return nil, fmt.Errorf("bingx ticker symbols HTTP %s", r.Status)
	}
	var x bingxSpotSymbolsResponse
	if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
		return nil, fmt.Errorf("bingxTickerSymbols: %w", err)
	}
	if x.Code != 0 {
		return nil, fmt.Errorf("bingx ticker symbols API code %d", x.Code)
	}
	out := make([]string, 0, len(x.Data.Symbols))
	for _, v := range x.Data.Symbols {
		if v.Status == 1 && strings.HasSuffix(v.Symbol, "-USDT") {
			out = append(out, v.Symbol)
		}
	}
	return out, nil
}

func (a *Adapter) runBingXCandleShard(ctx context.Context, sink domain.CandleSink, symbols []string, spot bool) {
	backoff := retry.New(reconnectDelay, 30*time.Second)
	for ctx.Err() == nil {
		if err := a.readCandleShard(ctx, sink, symbols, spot); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ BingX candle WS: %v", err)
		}
		if ctx.Err() == nil && !backoff.Wait(ctx.Done()) {
			return
		}
	}
}

func (a *Adapter) readCandleShard(ctx context.Context, sink domain.CandleSink, symbols []string, spot bool) error {
	url := spotWS
	if !spot {
		url = futuresWS
	}
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
		interval := "1min"
		if !spot {
			interval = "1m"
		}
		if err := a.writeJSON(conn, subscribeMsg{ID: fmt.Sprintf("candle-%d", time.Now().UnixNano()), ReqType: "sub", DataType: sym + "@kline_" + interval}); err != nil {
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
		raw, err := decompressGzip(msg)
		if err != nil {
			raw = msg
		}
		var r bingxCandleMessage
		if err := sonic.Unmarshal(raw, &r); err != nil {
			continue
		}
		if r.Code != 0 {
			return fmt.Errorf("BingX candle subscription rejected: code=%d msg=%q", r.Code, r.Msg)
		}
		if r.Data.Symbol == "" {
			continue
		}
		q, err := decimal.NewFromString(r.Data.K.Quote)
		if err != nil || q.IsNegative() {
			continue
		}
		et := time.UnixMilli(r.Data.EventTime)
		if err := sink.UpdateCandle(domain.MarketCandle{Exchange: "BINGX", Symbol: strings.ReplaceAll(r.Data.Symbol, "-", ""), MarketType: func() domain.MarketType {
			if spot {
				return domain.MarketTypeSpot
			}
			return domain.MarketTypeFutures
		}(), OpenTime: time.UnixMilli(r.Data.K.Start), CloseTime: time.UnixMilli(r.Data.K.End), QuoteVolume: q, EventTime: et}); err != nil {
			return fmt.Errorf("readCandleShard: %w", err)
		}
	}
}
