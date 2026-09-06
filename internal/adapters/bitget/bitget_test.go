package bitget

import (
	"context"
	"testing"
	"time"

	"crypto-screener/internal/domain"

	"github.com/stretchr/testify/assert"
)

// Connect* методы после H1-фикса БЛОКИРУЮТСЯ на время жизни соединения.
// Предотменённый контекст должен заставить их вернуться сразу (без сети и без зависания).
func TestBitget_ConnectMethods_ReturnPromptlyOnCanceledContext(t *testing.T) {
	adapter := NewAdapter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменён до вызова => процессы должны сразу выйти

	tickChan := make(chan domain.MarketTick, 1)

	done := make(chan error, 3)
	go func() { done <- adapter.ConnectSpot(ctx, tickChan) }()
	go func() { done <- adapter.ConnectFutures(ctx, tickChan) }()
	go func() { done <- adapter.ConnectFunding(ctx, nil) }()

	for i := 0; i < 3; i++ {
		select {
		case err := <-done:
			_ = err // prompt return is the contract; a cancelled-ctx dial error is expected
		case <-time.After(2 * time.Second):
			t.Fatalf("Connect* did not return promptly on cancelled context")
		}
	}
}

func TestBitget_ImplementsExchangeConnector(t *testing.T) {
	var _ domain.ExchangeConnector = NewAdapter()
	assert.NotNil(t, NewAdapter())
}
