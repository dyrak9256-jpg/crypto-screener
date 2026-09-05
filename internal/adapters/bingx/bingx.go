package bingx

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto-screener/internal/domain"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	spotWS    = "wss://open-api-ws.bingx.com/market"
	futuresWS = "wss://open-api-swap.bingx.com/swap-market"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 5 * time.Second // BingX требует частый ping
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var dialer = websocket.Dialer{HandshakeTimeout: handshakeTimeout}

type subscribeMsg struct {
	ID       string `json:"id"`
	ReqType  string `json:"reqType"`
	DataType string `json:"dataType"`
}

// BingX сжимает данные gzip
type wsResponse struct {
	DataType string     `json:"dataType"`
	Data     tickerData `json:"data"`
}

type tickerData struct {
	Symbol  string `json:"s"` // Symbol
	BidPr   string `json:"b"` // BestBid
	AskPr   string `json:"a"` // BestAsk
	QVolume string `json:"q"` // Quote volume
}

type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	go a.listen(ctx, spotWS, domain.MarketTypeSpot, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	go a.listen(ctx, futuresWS, domain.MarketTypeFutures, out)
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

	// BingX: подписка на all tickers
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
	_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))
	go keepAlive(connCtx, conn)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		// BingX сжимает ответы gzip
		data, err := decompressGzip(msg)
		if err != nil {
			// Не сжатое сообщение (например Pong)
			data = msg
		}

		// BingX pong приходит как текст "Pong"
		if string(data) == "Pong" {
			continue
		}

		// BingX шлёт массив тикеров
		var raw struct {
			DataType string       `json:"dataType"`
			Data     []tickerData `json:"data"`
		}
		if err := sonic.Unmarshal(data, &raw); err != nil {
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
	bid, err := decimal.NewFromString(d.BidPr)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}
	ask, err := decimal.NewFromString(d.AskPr)
	if err != nil || ask.IsZero() {
		return domain.MarketTick{}, false
	}
	qVol, _ := decimal.NewFromString(d.QVolume)

	return domain.MarketTick{
		Exchange:    "BINGX",
		Symbol:      d.Symbol,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		Timestamp:   ts,
	}, true
}

func decompressGzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// BingX ping: текстовое "Ping"
func keepAlive(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := conn.WriteMessage(websocket.TextMessage, []byte("Ping")); err != nil {
				log.Printf("⚠️  BingX ping error: %v", err)
				return
			}
		}
	}
}

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}
