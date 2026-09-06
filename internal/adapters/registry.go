package adapters

import (
	"fmt"
	"strings"

	"crypto-screener/internal/adapters/binance"
	"crypto-screener/internal/adapters/bingx"
	"crypto-screener/internal/adapters/bitget"
	"crypto-screener/internal/adapters/bybit"
	"crypto-screener/internal/adapters/gateio"
	"crypto-screener/internal/adapters/kucoin"
	"crypto-screener/internal/adapters/mexc"
	"crypto-screener/internal/adapters/okx"
	"crypto-screener/internal/domain"
)

// Exchange описывает подключённую биржу и фабрику её коннектора.
type Exchange struct {
	Name string
	// New создаёт свежий экземпляр коннектора биржи.
	// Каждый вызов возвращает независимый Adapter, чтобы hot-swap был корректным.
	New func() domain.ExchangeConnector
}

// Supported возвращает список всех бирж, подключённых к системе.
func Supported() []Exchange {
	return []Exchange{
		{"binance", func() domain.ExchangeConnector { return binance.NewAdapter() }},
		{"bitget", func() domain.ExchangeConnector { return bitget.NewAdapter() }},
		{"bingx", func() domain.ExchangeConnector { return bingx.NewAdapter() }},
		{"bybit", func() domain.ExchangeConnector { return bybit.NewAdapter() }},
		{"gateio", func() domain.ExchangeConnector { return gateio.NewAdapter() }},
		{"kucoin", func() domain.ExchangeConnector { return kucoin.NewAdapter() }},
		{"mexc", func() domain.ExchangeConnector { return mexc.NewAdapter() }},
		{"okx", func() domain.ExchangeConnector { return okx.NewAdapter() }},
	}
}

// NewByName создаёт коннектор биржи по имени (без учёта регистра).
func NewByName(name string) (domain.ExchangeConnector, error) {
	for _, ex := range Supported() {
		if strings.EqualFold(ex.Name, name) {
			return ex.New(), nil
		}
	}
	return nil, fmt.Errorf("exchange %q not supported", name)
}
