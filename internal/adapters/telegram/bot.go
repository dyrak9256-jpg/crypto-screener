package telegram

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"crypto-screener/internal/domain"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const (
	sendChanBuffer = 5000
	// Безопасный порог: 25 msg/sec (лимит Telegram — 30 msg/sec)
	sendInterval = 40 * time.Millisecond
	// Пауза при получении 429 Too Many Requests
	retryAfterDefault = 5 * time.Second
)

// ✅ Глобальный replacer — создаётся один раз, используется многократно
// Аллокация при каждом вызове EscapeMarkdownV2 недопустима при высокой нагрузке
var mdV2Replacer = strings.NewReplacer(
	"_", "\\_",
	"*", "\\*",
	"[", "\\[",
	"]", "\\]",
	"(", "\\(",
	")", "\\)",
	"~", "\\~",
	"`", "\\`",
	">", "\\>",
	"#", "\\#",
	"+", "\\+",
	"-", "\\-",
	"=", "\\=",
	"|", "\\|",
	"{", "\\{",
	"}", "\\}",
	".", "\\.",
	"!", "\\!",
)

type Bot struct {
	api        *tgbotapi.BotAPI
	sendChan   chan tgbotapi.Chattable
	cmdHandler domain.CommandHandler
	wg         sync.WaitGroup
}

func NewBot(token string, handler domain.CommandHandler) (*Bot, error) {
	api, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, fmt.Errorf("init telegram bot: %w", err)
	}

	b := &Bot{
		api:        api,
		cmdHandler: handler,
		sendChan:   make(chan tgbotapi.Chattable, sendChanBuffer),
	}

	b.wg.Add(1)
	go b.sendWorker()

	return b, nil
}

// sendWorker обрабатывает очередь сообщений с учётом rate limits Telegram
// Лимиты: 30 msg/sec глобально, 1 msg/sec в один чат
func (b *Bot) sendWorker() {
	defer b.wg.Done()

	ticker := time.NewTicker(sendInterval)
	defer ticker.Stop()

	for msg := range b.sendChan {
		<-ticker.C

		if _, err := b.api.Send(msg); err != nil {
			// ✅ Обработка 429 Too Many Requests
			// Telegram возвращает retry_after в секундах
			var tgErr *tgbotapi.Error
			if errors.As(err, &tgErr) && tgErr.Code == 429 {
				retryAfter := retryAfterDefault
				if tgErr.RetryAfter > 0 {
					retryAfter = time.Duration(tgErr.RetryAfter) * time.Second
				}
				log.Printf("⚠️  Telegram 429: retry after %s", retryAfter)
				time.Sleep(retryAfter)

				// Повторяем отправку после паузы
				if _, retryErr := b.api.Send(msg); retryErr != nil {
					log.Printf("⚠️  Telegram retry failed: %v", retryErr)
				}
				continue
			}

			log.Printf("⚠️  Telegram send error: %v", err)
		}
	}
}

// SendPrivateMessage отправляет сообщение конкретному пользователю
// Блокирующая отправка для личных ответов — не должны теряться
func (b *Bot) SendPrivateMessage(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, EscapeMarkdownV2(text))
	msg.ParseMode = tgbotapi.ModeMarkdownV2

	// ✅ Блокирующая отправка для личных ответов на команды
	// Если канал полон — используем select с таймаутом
	// чтобы не заблокировать polling горутину навсегда
	select {
	case b.sendChan <- msg:
	case <-time.After(5 * time.Second):
		log.Printf("⚠️  Telegram: send timeout for chat_id %d, message dropped", chatID)
	}
}

// Broadcast рассылает сообщение всем пользователям
// Использует non-blocking send — сигналы менее критичны чем личные ответы
func (b *Bot) Broadcast(text string, chatIDs []int64) {
	if len(chatIDs) == 0 {
		return
	}

	formattedText := EscapeMarkdownV2(text)

	var dropped int
	for _, id := range chatIDs {
		msg := tgbotapi.NewMessage(id, formattedText)
		msg.ParseMode = tgbotapi.ModeMarkdownV2

		select {
		case b.sendChan <- msg:
		default:
			dropped++
		}
	}

	if dropped > 0 {
		log.Printf("⚠️  Telegram Broadcast: dropped %d/%d messages (buffer full)",
			dropped, len(chatIDs))
	}
}

// Close корректно завершает работу:
// 1. Закрывает канал — sendWorker получит сигнал завершения
// 2. Ждёт пока sendWorker отправит все оставшиеся сообщения из буфера
func (b *Bot) Close() {
	close(b.sendChan)
	b.wg.Wait()
}

// StartPolling запускает получение обновлений от Telegram
func (b *Bot) StartPolling(ctx context.Context) {
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := b.api.GetUpdatesChan(u)

	for {
		select {
		case <-ctx.Done():
			b.api.StopReceivingUpdates()
			return

		case update, ok := <-updates:
			if !ok {
				return
			}
			if update.Message == nil || !update.Message.IsCommand() {
				continue
			}

			chatID := update.Message.Chat.ID

			// Получаем username: сначала из From, потом из Chat
			username := update.Message.From.UserName
			if username == "" {
				username = update.Message.Chat.UserName
			}

			cmd := update.Message.Command()

			var args []string
			if argStr := update.Message.CommandArguments(); argStr != "" {
				args = strings.Fields(argStr)
			}

			// Обрабатываем команду в отдельной горутине
			// чтобы медленный handler не блокировал получение следующих обновлений
			go func(cid int64, uname, command string, arguments []string) {
				response := b.cmdHandler.HandleCommand(cid, uname, command, arguments)
				b.SendPrivateMessage(cid, response)
			}(chatID, username, cmd, args)
		}
	}
}

// EscapeMarkdownV2 экранирует все спецсимволы Telegram MarkdownV2
// Использует глобальный replacer — zero allocation per call
func EscapeMarkdownV2(text string) string {
	return mdV2Replacer.Replace(text)
}
