package bingx

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto-screener/internal/domain"
	"fmt"
	"io"
	"log"
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

var dialer = websocket.Dialer{HandshakeTimeout: handshakeTimeout}

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

// BingX Spot и Futures используют разные имена полей
type tickerData struct {
	Symbol     string `json:"s"`          // "BTC-USDT" (Spot)
	BidPr      string `json:"b"`          // BestBid (Spot)
	AskPr      string `json:"a"`          // BestAsk (Spot)
	BidPrice   string `json:"bidPrice"`   // BestBid (Futures)
	AskPrice   string `json:"askPrice"`   // BestAsk (Futures)
	TradePrice string `json:"tradePrice"` // Last price (Futures fallback)
	QVolume    string `json:"q"`          // Quote volume
}

type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, spotWS, domain.MarketTypeSpot, out)
	return nil
}

// ConnectFunding — BINGX не предоставляет поток ставок финансирования через
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
			log.Printf("⚠️  BingX %s WS: %v — reconnecting in %s", mType, err, reconnectDelay)
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

	log.Printf("✅ BingX %s connected", mType)

	dataType := "spot.tickers"
	if mType == domain.MarketTypeFutures {
		dataType = "swap.tickers"
	}

	sub := subscribeMsg{
		ID:       fmt.Sprintf("sub-%d", time.Now().UnixNano()),
		ReqType:  "sub",
		DataType: dataType,
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	// Дедлайн: если сервер молчит дольше чем pingInterval+pongWait
	// значит что-то пошло не так (соединение зависло)
	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	// ✅ Нет keepAlive горутины — BingX сам инициирует Ping
	// Нам нужно только отвечать на серверный Ping

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}

		// Сбрасываем дедлайн при каждом сообщении
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		// Декомпрессия gzip
		data, err := decompressGzip(msg)
		if err != nil {
			// Не gzip — используем как есть (служебные фреймы)
			data = msg
		}

		strData := string(data)

		// ✅ Критично: отвечаем на серверный Ping
		// BingX разрывает соединение если не получает Pong
		if strData == "Ping" || strings.Contains(strData, `"ping"`) {
			if err := conn.WriteMessage(websocket.TextMessage, []byte("Pong")); err != nil {
				return fmt.Errorf("write pong: %w", err)
			}
			continue
		}

		var raw struct {
			DataType string       `json:"dataType"`
			Data     []tickerData `json:"data"`
		}
		if err := sonic.Unmarshal(data, &raw); err != nil {
			continue
		}

		if len(raw.Data) == 0 {
			continue
		}

		now := time.Now()
		for i := range raw.Data {
			tick, ok := toMarketTick(&raw.Data[i], mType, now)
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
}

func toMarketTick(d *tickerData, mType domain.MarketType, ts time.Time) (domain.MarketTick, bool) {
	// ✅ Fallback цепочка для bid/ask
	// Spot:    b / a
	// Futures: bidPrice / askPrice
	// Fallback: tradePrice (bid == ask == last для скринера)
	bidStr := firstNonEmpty(d.BidPr, d.BidPrice, d.TradePrice)
	askStr := firstNonEmpty(d.AskPr, d.AskPrice, d.TradePrice)

	bid, err := decimal.NewFromString(bidStr)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(askStr)
	if err != nil || ask.IsZero() {
		return domain.MarketTick{}, false
	}

	qVol, _ := decimal.NewFromString(d.QVolume)

	// ✅ Нормализация: "BTC-USDT" → "BTCUSDT"
	symbol := strings.ReplaceAll(d.Symbol, "-", "")

	return domain.MarketTick{
		Exchange:    "BINGX",
		Symbol:      symbol,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		Timestamp:   ts,
	}, true
}

// firstNonEmpty возвращает первую непустую строку из списка
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// ✅ Исправленный порядок: Close → Put (не Put → Close)
func decompressGzip(data []byte) ([]byte, error) {
	gr := gzipReaderPool.Get().(*gzip.Reader)

	if err := gr.Reset(bytes.NewReader(data)); err != nil {
		gzipReaderPool.Put(gr)
		return nil, err
	}

	result, err := io.ReadAll(gr)

	// ✅ Сначала Close, потом Put обратно в пул
	// Иначе: объект уже в пуле, но defer gr.Close() его модифицирует
	gr.Close()
	gzipReaderPool.Put(gr)

	return result, err
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}
