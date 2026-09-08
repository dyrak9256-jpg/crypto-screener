package mexc

import (
	"context"
	"testing"
	"time"

	"crypto-screener/internal/domain"

	"github.com/shopspring/decimal"
)

type nopFundingSink struct{}

func (nopFundingSink) UpdateFunding(string, string, decimal.Decimal, time.Time, time.Time) error {
	return nil
}
func (nopFundingSink) SetStreamHealth(string, bool) {}

// Раньше единичный сбой REST при старте (список символов / размеры
// контрактов) завершал funding- и futures-потоки MEXC навсегда —
// connector goroutine умирал, менеджер его не перезапускал. Теперь
// загрузка ретраится: предотменённый контекст должен давать быстрый
// выход из retry-цикла (без сети и без зависаний).
func TestMEXC_ConnectMethods_ReturnPromptlyOnCanceledContext(t *testing.T) {
	adapter := NewAdapter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tickChan := make(chan domain.MarketTick, 1)

	done := make(chan error, 3)
	go func() { done <- adapter.ConnectFutures(ctx, tickChan) }()
	go func() { done <- adapter.ConnectFunding(ctx, nopFundingSink{}) }()
	go func() { done <- adapter.ConnectSpot(ctx, tickChan) }()

	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("Connect* did not return promptly on cancelled context")
		}
	}
}
