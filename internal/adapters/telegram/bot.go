package telegram

import (
	"context"
	"crypto-screener/internal/domain"
	"log"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type Bot struct {
	api        *tgbotapi.BotAPI
	chatID     int64
	sendChan   chan string
	cmdHandler domain.CommandHandler
}

func NewBot(token string, chatID int64, handler domain.CommandHandler) (*Bot, error) {
	api, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, err
	}

	b := &Bot{
		api: api, chatID: chatID, cmdHandler: handler,
		sendChan: make(chan string, 5000),
	}
	go b.sendWorker()
	return b, nil
}

func (b *Bot) sendWorker() {
	for text := range b.sendChan {
		msg := tgbotapi.NewMessage(b.chatID, text)
		msg.ParseMode = "Markdown"
		if _, err := b.api.Send(msg); err != nil {
			log.Printf("⚠️ Telegram send error: %v", err)
		}
	}
}

func (b *Bot) SendMessage(text string) {
	select {
	case b.sendChan <- text:
	default:
	}
}

func (b *Bot) Close() { close(b.sendChan) }

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
			if update.Message == nil || !update.Message.IsCommand() {
				continue
			}

			cmd := update.Message.Command()
			args := []string{}
			if update.Message.CommandArguments() != "" {
				args = strings.Fields(update.Message.CommandArguments())
			}

			response := b.cmdHandler.HandleCommand(cmd, args)
			b.SendMessage(response)
		}
	}
}
