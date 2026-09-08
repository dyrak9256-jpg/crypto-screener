package telegram

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/observability"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const (
	sendChanBuffer     = 5000
	sendInterval       = 40 * time.Millisecond
	retryAfterDefault  = 5 * time.Second
	commandQueueBuffer = 1000
	commandWorkers     = 8
)

type commandRequest struct {
	chatID            int64
	username, command string
	args              []string
}

type Bot struct {
	// id идентифицирует бота внутри пула (индекс токена в TELEGRAM_TOKENS).
	id         int64
	api        *tgbotapi.BotAPI
	sendChan   chan tgbotapi.Chattable
	cmdHandler domain.CommandHandler
	commandWg  sync.WaitGroup
	sendWg     sync.WaitGroup
	closeOnce  sync.Once
	closed     atomic.Bool
	commands   chan commandRequest
	sendStop   chan struct{}
}

func NewBot(id int64, token string, handler domain.CommandHandler) (*Bot, error) {
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("telegram token is empty")
	}
	api, err := tgbotapi.NewBotAPIWithClient(token, tgbotapi.APIEndpoint, &http.Client{Timeout: 15 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("init telegram bot: %w", err)
	}
	b := &Bot{id: id, api: api, cmdHandler: handler, sendChan: make(chan tgbotapi.Chattable, sendChanBuffer), commands: make(chan commandRequest, commandQueueBuffer), sendStop: make(chan struct{})}
	b.sendWg.Add(1)
	go b.sendWorker()
	for i := 0; i < commandWorkers; i++ {
		b.commandWg.Add(1)
		go b.commandWorker()
	}
	return b, nil
}
func (b *Bot) commandWorker() {
	defer b.commandWg.Done()
	for {
		select {
		case <-b.sendStop:
			return
		case req := <-b.commands:
			if b.cmdHandler == nil {
				continue
			}
			response := b.cmdHandler.HandleCommand(b.id, req.chatID, req.username, req.command, req.args)
			b.SendPrivateMessage(req.chatID, response)
		}
	}
}
func (b *Bot) sendWorker() {
	defer b.sendWg.Done()
	ticker := time.NewTicker(sendInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.sendStop:
			return
		default:
		}
		select {
		case <-b.sendStop:
			return
		case msg, ok := <-b.sendChan:
			if !ok {
				return
			}
			select {
			case <-b.sendStop:
				return
			case <-ticker.C:
			}
			if _, err := b.api.Send(msg); err != nil {
				if retry, ok := telegramRetryAfter(err); ok {
					timer := time.NewTimer(retry)
					select {
					case <-b.sendStop:
						if !timer.Stop() {
							select {
							case <-timer.C:
							default:
							}
						}
						return
					case <-timer.C:
					}
					if _, retryErr := b.api.Send(msg); retryErr != nil {
						observability.TelegramDropped()
						slog.Warn("telegram retry send failed", "bot_id", b.id, "error", retryErr)
					} else {
						observability.TelegramSent()
					}
				} else {
					observability.TelegramDropped()
					slog.Warn("telegram send failed", "bot_id", b.id, "error", err)
				}
			} else {
				observability.TelegramSent()
			}
		}
	}
}
func telegramRetryAfter(err error) (time.Duration, bool) {
	var tgErr *tgbotapi.Error
	if !errorsAs(err, &tgErr) || tgErr.Code != 429 {
		return 0, false
	}
	retry := retryAfterDefault
	if tgErr.RetryAfter > 0 {
		retry = time.Duration(tgErr.RetryAfter) * time.Second
	}
	return retry, true
}

// Small local wrapper keeps this file independent of fmt/errors aliasing details in tests.
func errorsAs(err error, target any) bool { return errors.As(err, target) }
func (b *Bot) SendPrivateMessage(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	if b.closed.Load() {
		return
	}
	select {
	case b.sendChan <- msg:
	default:
		observability.TelegramDropped()
		slog.Warn("telegram send queue full, message dropped", "bot_id", b.id, "chat_id", chatID)
	}
}

func (b *Bot) Broadcast(text string, chatIDs []int64) {
	for _, id := range chatIDs {
		msg := tgbotapi.NewMessage(id, text)
		if b.closed.Load() {
			return
		}
		select {
		case b.sendChan <- msg:
		case <-b.sendStop:
			return
		default:
			observability.TelegramDropped()
			slog.Warn("telegram send queue full, broadcast message dropped", "bot_id", b.id, "chat_id", id)
		}
	}
}

func (b *Bot) Close() {
	b.closeOnce.Do(func() {
		// commands and sendChan are intentionally never closed. Producers may be
		// unwinding concurrently; sendStop is the sole shutdown signal, which
		// eliminates send-on-closed-channel races.
		b.closed.Store(true)
		if b.sendStop != nil {
			close(b.sendStop)
		}
		b.commandWg.Wait()
		b.sendWg.Wait()
	})
}

func (b *Bot) StartPolling(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 10
	updates := b.api.GetUpdatesChan(u)
	defer b.api.StopReceivingUpdates()
	for {
		select {
		case <-ctx.Done():
			return nil
		case update, ok := <-updates:
			if !ok {
				return nil
			}
			if update.Message == nil || !update.Message.IsCommand() {
				continue
			}
			username := update.Message.From.UserName
			if username == "" {
				username = update.Message.Chat.UserName
			}
			req := commandRequest{chatID: update.Message.Chat.ID, username: username, command: update.Message.Command(), args: strings.Fields(update.Message.CommandArguments())}
			select {
			case <-b.sendStop:
				return nil
			case b.commands <- req:
			case <-ctx.Done():
				return nil
			default:
				slog.Warn("telegram command queue full, command dropped", "bot_id", b.id, "command", req.command, "chat_id", req.chatID)
			}
		}
	}
}

func EscapeMarkdownV2(text string) string { return text }
