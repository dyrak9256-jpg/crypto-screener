package ingress

import (
	"crypto-screener/internal/domain"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestSubmitCoalescesAndDoesNotStrandLatestValue(t *testing.T) {
	out := make(chan domain.MarketTick, 1)
	out <- domain.MarketTick{Symbol: "SENTINEL"}
	t1 := domain.MarketTick{Exchange: "BINANCE", Symbol: "BTCUSDT", MarketType: domain.MarketTypeSpot, BestBid: decimal.NewFromInt(100), BestAsk: decimal.NewFromInt(101), EventTime: time.Now(), ReceivedAt: time.Now()}
	t2 := t1
	t2.BestBid = decimal.NewFromInt(102)

	Submit(out, t1)
	Submit(out, t2)

	<-out // release the backpressure slot; the coalescer must now deliver t2.
	select {
	case got := <-out:
		if !got.BestBid.Equal(decimal.NewFromInt(102)) {
			t.Fatalf("expected latest bid 102, got %s", got.BestBid)
		}
	case <-time.After(time.Second):
		t.Fatal("latest value was not delivered")
	}

	DrainAndStop(out)
}

func TestDrainAndStopDoesNotBlockOnBackpressure(t *testing.T) {
	out := make(chan domain.MarketTick)
	Submit(out, domain.MarketTick{Exchange: "BINANCE", Symbol: "BTCUSDT", MarketType: domain.MarketTypeSpot})
	done := make(chan struct{})
	go func() {
		DrainAndStop(out)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("DrainAndStop blocked on downstream backpressure")
	}
}
