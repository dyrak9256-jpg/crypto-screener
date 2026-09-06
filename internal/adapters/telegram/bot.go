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

// ✅ Глобальный replacer — создаётся один раз, используется многократно.
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
	// done закрывается в Close(). sendChan НЕ закрывается: это исключает
	// «send on closed channel» при гонке продюсера (polling-команда) с Close().
	done      chan struct{}
	closeOnce sync.Once
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
		done:       make(chan struct{}),
	}

	b.wg.Add(1)
	go b.sendWorker()

	return b, nil
}

// sendWorker обрабатывает очередь сообщений с учётом rate limits Telegram.
// Завершается по закрытию done, предварительно дочитав оставшиеся сообщения.
func (b *Bot) sendWorker() {
	defer b.wg.Done()

	ticker := time.NewTicker(sendInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.done:
			// Дочитываем и отправляем всё, что осталось в очереди, затем выходим.
			b.drain(ticker)
			return
		case msg := <-b.sendChan:
			<-ticker.C
			b.doSend(msg)
		}
	}
}

// drain отправляет оставшиеся в очереди сообщения без блокировки.
func (b *Bot) drain(ticker *time.Ticker) {
	for {
		select {
		case msg := <-b.sendChan:
			<-ticker.C
			b.doSend(msg)
		default:
			return
		}
	}
}

func (b *Bot) doSend(msg tgbotapi.Chattable) {
	if _, err := b.api.Send(msg); err != nil {
		var tgErr *tgbotapi.Error
		if errors.As(err, &tgErr) && tgErr.Code == 429 {
			retryAfter := retryAfterDefault
			if tgErr.RetryAfter > 0 {
				retryAfter = time.Duration(tgErr.RetryAfter) * time.Second
			}
			log.Printf("⚠️  Telegram 429: retry after %s", retryAfter)
			time.Sleep(retryAfter)
			if _, retryErr := b.api.Send(msg); retryErr != nil {
				log.Printf("⚠️  Telegram retry failed: %v", retryErr)
			}
			return
		}
		log.Printf("⚠️  Telegram send error: %v", err)
	}
}

// trySend отправляет сообщение неблокирующе. После Close() (done закрыт)
// возвращает false и ничего не отправляет — это защищает от «send on closed
// channel» и не блокирует вызывающего.
func (b *Bot) trySend(msg tgbotapi.Chattable) bool {
	select {
	case <-b.done:
		return false
	default:
	}
	select {
	case b.sendChan <- msg:
		return true
	case <-b.done:
		return false
	default:
		return false
	}
}

// SendPrivateMessage отправляет сообщение конкретному пользователю.
// Неблокирующая отправка: при заполненном буфере сообщение отбрасывается.
func (b *Bot) SendPrivateMessage(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = tgbotapi.ModeMarkdown
	if !b.trySend(msg) {
		log.Printf("⚠️  Telegram: sendChan full or bot closed, reply to chat_id %d dropped", chatID)
	}
}

// Broadcast рассылает сообщение всем пользователям. Неблокирующе.
func (b *Bot) Broadcast(text string, chatIDs []int64) {
	if len(chatIDs) == 0 {
		return
	}

	var dropped int
	for _, id := range chatIDs {
		msg := tgbotapi.NewMessage(id, text)
		msg.ParseMode = tgbotapi.ModeMarkdown
		if !b.trySend(msg) {
			dropped++
		}
	}

	if dropped > 0 {
		log.Printf("⚠️  Telegram Broadcast: dropped %d/%d messages", dropped, len(chatIDs))
	}
}

// Close корректно и ИДЕМПОТЕНТНО завершает работу: сигналит sendWorker о
// необходимости дочитать очередь и остановиться, затем ждёт его завершения.
// sendChan при этом не закрывается — нет «send on closed channel».
func (b *Bot) Close() {
	if b.done != nil {
		b.closeOnce.Do(func() { close(b.done) })
	}
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

			username := update.Message.From.UserName
			if username == "" {
				username = update.Message.Chat.UserName
			}

			cmd := update.Message.Command()

			var args []string
			if argStr := update.Message.CommandArguments(); argStr != "" {
				args = strings.Fields(argStr)
			}

			// Обрабатываем команду в отдельной горутине, чтобы медленный handler
			// не блокировал получение следующих обновлений.
			go func(cid int64, uname, command string, arguments []string) {
				response := b.cmdHandler.HandleCommand(cid, uname, command, arguments)
				b.SendPrivateMessage(cid, response)
			}(chatID, username, cmd, args)
		}
	}
}

// EscapeMarkdownV2 экранирует все спецсимволы Telegram MarkdownV2
func EscapeMarkdownV2(text string) string {
	return mdV2Replacer.Replace(text)
}
