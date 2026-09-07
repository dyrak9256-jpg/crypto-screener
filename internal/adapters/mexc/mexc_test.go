package mexc

import (
	"encoding/json"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestFuturesToTickConvertsContractVolumeToQuoteNotional(t *testing.T) {
	a := NewAdapter()
	a.contractSize["BTC_USDT"] = decimal.RequireFromString("0.0001")
	raw := futuresTickerData{
		Symbol:    "BTC_USDT",
		LastPrice: json.RawMessage(`50000`),
		Bid1:      json.RawMessage(`49999`),
		Ask1:      json.RawMessage(`50001`),
		Volume:    json.RawMessage(`100000`),
	}
	tick, ok := a.futuresToTick(&raw)
	require.True(t, ok)
	require.True(t, tick.QuoteVolume.Equal(decimal.NewFromInt(500000)))
}

func TestFuturesToTickFailsClosedWithoutContractSize(t *testing.T) {
	a := NewAdapter()
	raw := futuresTickerData{
		Symbol:    "BTC_USDT",
		LastPrice: json.RawMessage(`50000`),
		Bid1:      json.RawMessage(`49999`),
		Ask1:      json.RawMessage(`50001`),
		Volume:    json.RawMessage(`100000`),
	}
	_, ok := a.futuresToTick(&raw)
	require.False(t, ok)
}
