package telegram

import (
	"context"
	"log"
	"strings"

	"crypto-screener/internal/domain"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type Bot struct {
	api        *tgbotapi.BotAPI
	sendChan   chan tgbotapi.Chattable
	cmdHandler domain.CommandHandler
}

func NewBot(token string, handler domain.CommandHandler) (*Bot, error) {
	api, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, err
	}

	b := &Bot{
		api:        api,
		cmdHandler: handler,
		sendChan:   make(chan tgbotapi.Chattable, 10000), // Увеличили буфер для рассылок
	}
	go b.sendWorker()
	return b, nil
}

func (b *Bot) sendWorker() {
	for msg := range b.sendChan {
		if _, err := b.api.Send(msg); err != nil {
			log.Printf("⚠️ Telegram send error: %v", err)
		}
	}
}

func (b *Bot) SendPrivateMessage(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	select {
	case b.sendChan <- msg:
	default:
	}
}

func (b *Bot) Broadcast(text string, chatIDs []int64) {
	for _, id := range chatIDs {
		msg := tgbotapi.NewMessage(id, text)
		msg.ParseMode = "Markdown"
		select {
		case b.sendChan <- msg:
		default:
			// Если канал переполнен, пропускаем, чтобы не блокировать роутер
		}
	}
}

func (b *Bot) Close() {
	close(b.sendChan)
}

func (b *Bot) StartPolling(ctx context.Context) {
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := b.api.GetUpdatesChan(u)

	for {
		select {
		case <-ctx.Done():
			return
		case update, ok := <-updates:
			if !ok {
				return
			}
			if update.Message == nil {
				continue
			}

			chatID := update.Message.Chat.ID
			username := update.Message.From.UserName
			if username == "" {
				username = update.Message.Chat.UserName
			}

			if update.Message.IsCommand() {
				cmd := update.Message.Command()
				args := []string{}
				if update.Message.CommandArguments() != "" {
					args = strings.Fields(update.Message.CommandArguments())
				}

				response := b.cmdHandler.HandleCommand(chatID, username, cmd, args)
				b.SendPrivateMessage(chatID, response)
			}
		}
	}
}
