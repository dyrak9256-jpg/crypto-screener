package binance

import (
	"context"
	"crypto-screener/internal/domain"
	"fmt"
	"log"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	spotWS    = "wss://stream.binance.com:9443/ws/!ticker@arr"
	futuresWS = "wss://fstream.binance.com/ws/!ticker@arr"
	fundingWS = "wss://fstream.binance.com/ws/!markPrice@arr"

	handshakeTimeout = 10 * time.Second
	pingInterval     = 3 * time.Minute
	pongWait         = 10 * time.Second
	reconnectDelay   = 3 * time.Second
)

var dialer = websocket.Dialer{
	HandshakeTimeout: handshakeTimeout,
}

type tickerPayload struct {
	Symbol  string `json:"s"`
	BestBid string `json:"b"`
	BestAsk string `json:"a"`
	QVolume string `json:"q"`
}

type fundingPayload struct {
	Symbol          string `json:"s"`
	FundingRate     string `json:"r"`
	NextFundingTime int64  `json:"T"`
}

type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ConnectSpot(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, spotWS, domain.MarketTypeSpot, out)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, out chan<- domain.MarketTick) error {
	a.listen(ctx, futuresWS, domain.MarketTypeFutures, out)
	return nil
}

func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	a.listenFunding(ctx, sink)
	return nil
}

// --- Ticker ---

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
			log.Printf("⚠️  Binance %s WS: %v — reconnecting in %s", mType, err, reconnectDelay)
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

	log.Printf("✅ Binance %s connected", mType)

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))
	})

	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	go keepAlive(connCtx, conn, string(mType))

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}

		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		var payloads []tickerPayload
		if err := sonic.Unmarshal(msg, &payloads); err != nil {
			continue
		}

		now := time.Now()
		for i := range payloads {
			tick, ok := toMarketTick(&payloads[i], mType, now)
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

func toMarketTick(p *tickerPayload, mType domain.MarketType, ts time.Time) (domain.MarketTick, bool) {
	bid, err := decimal.NewFromString(p.BestBid)
	if err != nil || bid.IsZero() {
		return domain.MarketTick{}, false
	}

	ask, err := decimal.NewFromString(p.BestAsk)
	if err != nil || ask.IsZero() {
		return domain.MarketTick{}, false
	}

	qVol, _ := decimal.NewFromString(p.QVolume)

	return domain.MarketTick{
		Exchange:    "BINANCE",
		Symbol:      p.Symbol,
		MarketType:  mType,
		BestBid:     bid,
		BestAsk:     ask,
		QuoteVolume: qVol,
		Timestamp:   ts,
	}, true
}

// --- Funding ---

func (a *Adapter) listenFunding(ctx context.Context, sink domain.FundingSink) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := a.connectAndReadFunding(ctx, sink); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  Binance Funding WS: %v — reconnecting in %s", err, reconnectDelay)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectDelay):
		}
	}
}

func (a *Adapter) connectAndReadFunding(ctx context.Context, sink domain.FundingSink) error {
	conn, _, err := dialer.DialContext(ctx, fundingWS, nil)
	if err != nil {
		return fmt.Errorf("dial funding: %w", err)
	}
	defer conn.Close()

	log.Printf("✅ Binance Funding connected")

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go closeOnCtx(connCtx, conn)

	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))
	})

	if err := conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	go keepAlive(connCtx, conn, "funding")

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read funding: %w", err)
		}

		_ = conn.SetReadDeadline(time.Now().Add(pingInterval + pongWait))

		var payloads []fundingPayload
		if err := sonic.Unmarshal(msg, &payloads); err != nil {
			continue
		}

		for _, p := range payloads {
			rate, _ := decimal.NewFromString(p.FundingRate)
			sink.UpdateFunding("BINANCE", p.Symbol, rate, time.UnixMilli(p.NextFundingTime))
		}
	}
}

// --- Helpers ---

func closeOnCtx(ctx context.Context, conn *websocket.Conn) {
	<-ctx.Done()
	conn.Close()
}

func keepAlive(ctx context.Context, conn *websocket.Conn, label string) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := conn.WriteControl(
				websocket.PingMessage,
				nil,
				time.Now().Add(5*time.Second),
			); err != nil {
				log.Printf("⚠️  Binance ping error [%s]: %v", label, err)
				return
			}
		}
	}
}
