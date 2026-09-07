package telegram

import (
	"context"
	"log/slog"
	"sync"

	"crypto-screener/internal/domain"
)

// BotPool маршрутизирует сообщения между несколькими Telegram-ботами
// (TELEGRAM_TOKENS). Каждый пользователь закреплён за ботом, через которого
// подписался (users.bot_id): лимиты Telegram считаются на бота, поэтому
// распределение подписчиков по ботам поднимает суммарный throughput доставки.
type BotPool struct {
	mu    sync.RWMutex
	bots  []*Bot
	route func(chatID int64) int64
}

// NewBotPool создаёт пул. route(chatID) возвращает id бота для чата
// (может быть nil — тогда все сообщения идут через первого бота).
func NewBotPool(bots []*Bot, route func(chatID int64) int64) *BotPool {
	return &BotPool{bots: append([]*Bot(nil), bots...), route: route}
}

// botFor выбирает бота для чата: по маршруту, иначе — первый бот пула.
func (p *BotPool) botFor(chatID int64) *Bot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.bots) == 0 {
		return nil
	}
	if p.route != nil {
		id := p.route(chatID)
		for _, b := range p.bots {
			if b.id == id {
				return b
			}
		}
	}
	return p.bots[0]
}

// SendPrivateMessage доставляет личное сообщение через бот пользователя.
func (p *BotPool) SendPrivateMessage(chatID int64, text string) {
	if b := p.botFor(chatID); b != nil {
		b.SendPrivateMessage(chatID, text)
	}
}

// Broadcast рассылает сообщение группе чатов, разбивая её по ботам.
func (p *BotPool) Broadcast(text string, chatIDs []int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.bots) == 0 {
		return
	}
	byBot := make(map[*Bot][]int64)
	for _, id := range chatIDs {
		b := p.bots[0]
		if p.route != nil {
			botID := p.route(id)
			for _, cand := range p.bots {
				if cand.id == botID {
					b = cand
					break
				}
			}
		}
		byBot[b] = append(byBot[b], id)
	}
	for b, ids := range byBot {
		b.Broadcast(text, ids)
	}
}

// StartPolling запускает getUpdates-циклы всех ботов и ждёт их завершения.
// Ошибка любого бота логируется, но не останавливает остальных.
func (p *BotPool) StartPolling(ctx context.Context) error {
	p.mu.RLock()
	bots := append([]*Bot(nil), p.bots...)
	p.mu.RUnlock()
	var wg sync.WaitGroup
	for _, b := range bots {
		wg.Add(1)
		go func(b *Bot) {
			defer wg.Done()
			if err := b.StartPolling(ctx); err != nil {
				slog.Error("telegram bot polling failed", "bot_id", b.id, "error", err)
			}
		}(b)
	}
	wg.Wait()
	return nil
}

// Close останавливает всех ботов пула.
func (p *BotPool) Close() {
	p.mu.RLock()
	bots := append([]*Bot(nil), p.bots...)
	p.mu.RUnlock()
	for _, b := range bots {
		b.Close()
	}
}

// Len возвращает количество ботов в пуле.
func (p *BotPool) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.bots)
}

var _ domain.TelegramSender = (*BotPool)(nil)
