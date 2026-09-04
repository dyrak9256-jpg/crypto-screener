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
	spotWS    = "wss://stream.binance.com:9443/ws/!ticker"
	futuresWS = "wss://fstream.binance.com/ws/!ticker"
	fundingWS = "wss://fstream.binance.com/ws/!markPrice@arr"
)

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

func (a *Adapter) ConnectSpot(ctx context.Context, outChan chan<- domain.MarketTick) error {
	go a.listen(ctx, spotWS, domain.MarketTypeSpot, outChan)
	return nil
}

func (a *Adapter) ConnectFutures(ctx context.Context, outChan chan<- domain.MarketTick) error {
	go a.listen(ctx, futuresWS, domain.MarketTypeFutures, outChan)
	return nil
}

func (a *Adapter) ConnectFunding(ctx context.Context, sink domain.FundingSink) error {
	go a.listenFunding(ctx, sink)
	return nil
}

func (a *Adapter) listen(ctx context.Context, url string, mType domain.MarketType, outChan chan<- domain.MarketTick) {
	for {
		if ctx.Err() != nil {
			return
		}
		err := a.connectAndRead(ctx, url, mType, outChan)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("⚠️ Binance %s WS error: %v. Reconnecting...", mType, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}
}

func (a *Adapter) connectAndRead(ctx context.Context, url string, mType domain.MarketType, outChan chan<- domain.MarketTick) error {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial error: %w", err)
	}
	defer conn.Close()

	log.Printf("✅ Connected to Binance %s", mType)
	go func() { <-ctx.Done(); conn.Close() }()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		var p tickerPayload
		if err := sonic.Unmarshal(msg, &p); err != nil {
			continue
		}

		bid, _ := decimal.NewFromString(p.BestBid)
		ask, _ := decimal.NewFromString(p.BestAsk)
		qVol, _ := decimal.NewFromString(p.QVolume)

		if bid.IsZero() || ask.IsZero() {
			continue
		}

		tick := domain.MarketTick{
			Exchange: "BINANCE", Symbol: p.Symbol, MarketType: mType,
			BestBid: bid, BestAsk: ask, QuoteVolume: qVol, Timestamp: time.Now(),
		}

		select {
		case outChan <- tick:
		default: // Drop if channel full to prevent WS blocking
		}
	}
}

func (a *Adapter) listenFunding(ctx context.Context, sink domain.FundingSink) {
	for {
		if ctx.Err() != nil {
			return
		}
		err := a.connectAndReadFunding(ctx, sink)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("⚠️ Binance Funding WS error: %v. Reconnecting...", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}
}

func (a *Adapter) connectAndReadFunding(ctx context.Context, sink domain.FundingSink) error {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, fundingWS, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	log.Printf("✅ Connected to Binance Funding Rates")
	go func() { <-ctx.Done(); conn.Close() }()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		var payloads []fundingPayload
		if err := sonic.Unmarshal(msg, &payloads); err != nil {
			continue
		}

		for _, p := range payloads {
			rate, _ := decimal.NewFromString(p.FundingRate)
			nextTime := time.UnixMilli(p.NextFundingTime)
			sink.UpdateFunding(p.Symbol, rate, nextTime)
		}
	}
}
