package telegram

import (
	"testing"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/stretchr/testify/require"
)

func newTestBot(id int64) *Bot {
	return &Bot{id: id, sendChan: make(chan tgbotapi.Chattable, 16)}
}

func TestBotPool_RoutesToUserBot(t *testing.T) {
	b0 := newTestBot(0)
	b1 := newTestBot(1)
	pool := NewBotPool([]*Bot{b0, b1}, func(chatID int64) int64 {
		if chatID == 100 {
			return 1
		}
		return 0
	})
	require.Equal(t, 2, pool.Len())

	// Личные сообщения идут через бот, закреплённый за чатом.
	pool.SendPrivateMessage(100, "hi") // → b1
	pool.SendPrivateMessage(200, "hi") // → b0
	require.Len(t, b1.sendChan, 1)
	require.Len(t, b0.sendChan, 1)

	// Broadcast разбивает чаты по ботам.
	pool.Broadcast("alert", []int64{100, 200, 300})
	require.Len(t, b1.sendChan, 2)
	require.Len(t, b0.sendChan, 3)
}

func TestBotPool_Fallbacks(t *testing.T) {
	b0 := newTestBot(0)
	b1 := newTestBot(1)
	// Неизвестный bot_id маршрута → первый бот пула.
	pool := NewBotPool([]*Bot{b0, b1}, func(int64) int64 { return 42 })
	pool.SendPrivateMessage(1, "x")
	require.Len(t, b0.sendChan, 1)
	require.Len(t, b1.sendChan, 0)
	// nil-маршрут → первый бот.
	pool2 := NewBotPool([]*Bot{b0, b1}, nil)
	pool2.Broadcast("y", []int64{1, 2})
	require.Len(t, b0.sendChan, 3)
	require.Len(t, b1.sendChan, 0)
	// Пустой пул — безопасный no-op.
	pool3 := NewBotPool(nil, nil)
	pool3.SendPrivateMessage(1, "z")
	pool3.Broadcast("z", []int64{1})
	pool3.Close()
	pool3.StartPolling(nil) // не должен паниковать
}
