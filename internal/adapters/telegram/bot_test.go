package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"crypto-screener/internal/domain/mocks"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBot_NewBot_InvalidToken(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHandler := mocks.NewMockCommandHandler(ctrl)
	bot, err := NewBot("invalid_token_12345", mockHandler)
	assert.Error(t, err, "NewBot with invalid token should return error")
	assert.Nil(t, bot)
}

func TestBot_SendPrivateMessage_And_Broadcast(t *testing.T) {
	t.Parallel()

	sendChan := make(chan tgbotapi.Chattable, 10)
	bot := &Bot{
		sendChan: sendChan,
		done:     make(chan struct{}),
	}

	// 1. SendPrivateMessage
	bot.SendPrivateMessage(12345, "Hello Trader")
	require.Len(t, sendChan, 1)

	msg := (<-sendChan).(tgbotapi.MessageConfig)
	assert.Equal(t, int64(12345), msg.ChatID)
	assert.Equal(t, "Hello Trader", msg.Text)
	assert.Equal(t, "Markdown", msg.ParseMode)

	// 2. Broadcast
	chatIDs := []int64{101, 102, 103}
	bot.Broadcast("🚨 Arbitrage Alert", chatIDs)
	require.Len(t, sendChan, 3)

	for _, expectedID := range chatIDs {
		bMsg := (<-sendChan).(tgbotapi.MessageConfig)
		assert.Equal(t, expectedID, bMsg.ChatID)
		assert.Equal(t, "🚨 Arbitrage Alert", bMsg.Text)
		assert.Equal(t, "Markdown", bMsg.ParseMode)
	}

	// 3. Close: идемпотентно; sendChan НЕ закрывается (нет send-on-closed-channel).
	bot.Close()
	bot.Close() // идемпотентно
	// После Close() отправки — no-op, не паника и не добавление в очередь.
	bot.SendPrivateMessage(999, "after close")
	bot.Broadcast("after close", []int64{9, 10})
	require.Len(t, sendChan, 0, "no messages may be enqueued after Close()")
	assert.NotPanics(t, func() { bot.Close() })
}

func TestBot_NonBlockingDropWhenFull(t *testing.T) {
	t.Parallel()

	// Small buffer of size 2
	sendChan := make(chan tgbotapi.Chattable, 2)
	bot := &Bot{
		sendChan: sendChan,
		done:     make(chan struct{}),
	}

	// Fill buffer completely
	sendChan <- tgbotapi.NewMessage(1, "msg1")
	sendChan <- tgbotapi.NewMessage(2, "msg2")
	require.Len(t, sendChan, 2)

	// SendPrivateMessage should not block even when buffer is full
	done := make(chan struct{})
	go func() {
		bot.SendPrivateMessage(3, "msg3_dropped")
		close(done)
	}()

	select {
	case <-done:
		// Succeeded without blocking
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("SendPrivateMessage blocked when sendChan was full")
	}

	// Broadcast should also not block
	doneBroadcast := make(chan struct{})
	go func() {
		bot.Broadcast("broadcast_dropped", []int64{4, 5})
		close(doneBroadcast)
	}()

	select {
	case <-doneBroadcast:
		// Succeeded without blocking
	case <-doneBroadcast:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Broadcast blocked when sendChan was full")
	}

	// Length should still be 2 (overflow messages dropped)
	assert.Len(t, sendChan, 2)
}

func TestBot_SendWorker_And_StartPolling(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var sentMessages []string
	var mu sync.Mutex
	updateSent := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path == "/botTEST_TOKEN/getMe" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": true,
				"result": map[string]any{
					"id":         123456,
					"is_bot":     true,
					"first_name": "TestBot",
					"username":   "test_bot",
				},
			})
			return
		}

		if r.URL.Path == "/botTEST_TOKEN/sendMessage" {
			mu.Lock()
			sentMessages = append(sentMessages, r.FormValue("text"))
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": true,
				"result": map[string]any{
					"message_id": 1,
					"date":       1725500000,
					"chat":       map[string]any{"id": 12345, "type": "private"},
					"text":       r.FormValue("text"),
				},
			})
			return
		}

		if r.URL.Path == "/botTEST_TOKEN/getUpdates" {
			mu.Lock()
			alreadySent := updateSent
			updateSent = true
			mu.Unlock()

			if !alreadySent {
				// Return an update containing a command with arguments
				updates := []map[string]any{
					{
						"update_id": 101,
						"message": map[string]any{
							"message_id": 1,
							"date":       1725500000,
							"chat":       map[string]any{"id": int64(12345), "type": "private", "username": "chat_user"},
							"from":       map[string]any{"id": int64(12345), "is_bot": false, "first_name": "Alice", "username": "alice"},
							"text":       "/setcross 2.0",
							"entities": []map[string]any{
								{"type": "bot_command", "offset": 0, "length": 9},
							},
						},
					},
					{
						// Message without command (should be ignored by command handler)
						"update_id": 102,
						"message": map[string]any{
							"message_id": 2,
							"date":       1725500000,
							"chat":       map[string]any{"id": int64(12345), "type": "private", "username": "chat_user"},
							"from":       map[string]any{"id": int64(12345), "is_bot": false, "first_name": "Alice", "username": "alice"},
							"text":       "just regular text",
						},
					},
					{
						// Update without message (e.g. channel post or callback query)
						"update_id": 103,
					},
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ok":     true,
					"result": updates,
				})
			} else {
				// No more updates
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ok":     true,
					"result": []any{},
				})
			}
			return
		}

		http.NotFound(w, r)
	}))
	defer server.Close()

	api, err := tgbotapi.NewBotAPIWithClient("TEST_TOKEN", server.URL+"/bot%s/%s", http.DefaultClient)
	require.NoError(t, err)

	mockHandler := mocks.NewMockCommandHandler(ctrl)
	mockHandler.EXPECT().
		HandleCommand(int64(12345), "alice", "setcross", []string{"2.0"}).
		Return("✅ Минимальный спред установлен на 2.00%").
		Times(1)

	bot := &Bot{
		api:        api,
		cmdHandler: mockHandler,
		sendChan:   make(chan tgbotapi.Chattable, 10),
		done:       make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start sendWorker in background.
	// В NewBot() существуют b.wg.Add(1) перед запуском; здесь Bot создан
	// напрямую, поэтому регистрируем горутину вручную, иначе defer
	// b.wg.Done() вызовет negative WaitGroup counter.
	bot.wg.Add(1)
	go bot.sendWorker()

	// 2. Start Polling in background
	go bot.StartPolling(ctx)

	// Wait for command processing and response message to be sent
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(sentMessages) >= 1
	}, 3*time.Second, 50*time.Millisecond)

	mu.Lock()
	assert.Contains(t, sentMessages[0], "Минимальный спред установлен на 2.00%")
	mu.Unlock()

	// Cancel context to cleanly stop polling loop
	cancel()
	bot.Close()
}
