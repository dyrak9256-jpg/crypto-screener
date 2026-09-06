package adapters

import (
	"testing"

	"crypto-screener/internal/adapters/binance"
	"crypto-screener/internal/adapters/bingx"
	"crypto-screener/internal/adapters/bitget"
	"crypto-screener/internal/adapters/bybit"
	"crypto-screener/internal/adapters/gateio"
	"crypto-screener/internal/adapters/kucoin"
	"crypto-screener/internal/adapters/mexc"
	"crypto-screener/internal/adapters/okx"
	"crypto-screener/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Компиляционная проверка: все биржевые адаптеры реализуют порт ExchangeConnector.
var (
	_ domain.ExchangeConnector = binance.NewAdapter()
	_ domain.ExchangeConnector = bitget.NewAdapter()
	_ domain.ExchangeConnector = bingx.NewAdapter()
	_ domain.ExchangeConnector = bybit.NewAdapter()
	_ domain.ExchangeConnector = gateio.NewAdapter()
	_ domain.ExchangeConnector = kucoin.NewAdapter()
	_ domain.ExchangeConnector = mexc.NewAdapter()
	_ domain.ExchangeConnector = okx.NewAdapter()
)

func TestSupported_ContainsAllExchanges(t *testing.T) {
	exchanges := Supported()
	require.Len(t, exchanges, 8)

	names := make(map[string]bool)
	for _, ex := range exchanges {
		names[ex.Name] = true
		require.NotNil(t, ex.New, "each exchange must have a factory")
		// Фабрика должна возвращать корректный коннектор.
		require.NotNil(t, ex.New(), "factory must produce a connector")
	}

	for _, want := range []string{"binance", "bitget", "bingx", "bybit", "gateio", "kucoin", "mexc", "okx"} {
		assert.True(t, names[want], "missing exchange %q in Supported()", want)
	}
}

func TestNewByName_AllSupported(t *testing.T) {
	for _, ex := range Supported() {
		conn, err := NewByName(ex.Name)
		require.NoError(t, err, "NewByName(%q) should succeed", ex.Name)
		require.NotNil(t, conn)
	}
}

func TestNewByName_CaseInsensitive(t *testing.T) {
	for _, name := range []string{"BINANCE", "binance", "BinAnCe", "ByBit", "KUCOIN", "mexc"} {
		conn, err := NewByName(name)
		require.NoError(t, err, "NewByName(%q) should be case-insensitive", name)
		require.NotNil(t, conn)
	}
}

func TestNewByName_UnknownReturnsError(t *testing.T) {
	conn, err := NewByName("DEX_NOT_SUPPORTED")
	assert.Nil(t, conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}
