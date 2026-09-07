package telegram

import (
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/stretchr/testify/require"
)

func TestBot_NewBot_EmptyToken(t *testing.T) {
	bot, err := NewBot(0, "", nil)
	require.Error(t, err)
	require.Nil(t, bot)
}
func TestBot_SendPrivateMessageAndBroadcast(t *testing.T) {
	ch := make(chan tgbotapi.Chattable, 10)
	b := &Bot{sendChan: ch}
	b.SendPrivateMessage(1, "hello")
	b.Broadcast("alert", []int64{2, 3})
	require.Len(t, ch, 3)
	b.Close()
	select {
	case _, ok := <-ch:
		require.False(t, ok)
	default:
		require.Fail(t, "channel should be closed and drained")
	}
}
func TestBot_SendPrivateMessageDoesNotBlockWhenFull(t *testing.T) {
	ch := make(chan tgbotapi.Chattable, 1)
	ch <- tgbotapi.NewMessage(1, "x")
	b := &Bot{sendChan: ch}
	done := make(chan struct{})
	go func() { b.SendPrivateMessage(2, "y"); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SendPrivateMessage blocked")
	}
	b.Close()
}
