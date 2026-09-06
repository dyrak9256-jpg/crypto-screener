package app

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"crypto-screener/internal/adapters/binance"
	"crypto-screener/internal/domain"

	"github.com/shopspring/decimal"
)

type Application struct {
	connManager *ConnectorManager
	aggregator  *ShardedAggregator
	tracker     *Tracker
	fundingMgr  *FundingManager
	config      *domain.ScreenerConfig
	adminIDs    []int64

	userMgr  *domain.UserManager
	userRepo domain.UserRepository
	router   *NotificationRouter

	tickChan    chan domain.MarketTick
	trackerChan chan domain.SpreadEvent
	dbChan      chan *domain.ArbitrageSignal

	// ✅ atomic.Pointer вместо RWMutex + context.Context
	// Паттерн: write-once в Run(), read-many в HandleCommand()
	// Преимущество: нет блокировок при чтении — lock-free доступ
	ctx atomic.Pointer[context.Context]

	// Две независимые WaitGroup для детерминированного shutdown:
	// workerWg  — ingestion + tracker воркеры
	// persistWg — PersistenceWorker (завершается последним)
	workerWg  sync.WaitGroup
	persistWg sync.WaitGroup
}

func NewApplication(
	cfg *domain.ScreenerConfig,
	repo domain.SignalRepository,
	userRepo domain.UserRepository,
) *Application {
	tickChan := make(chan domain.MarketTick, 200_000)
	trackerChan := make(chan domain.SpreadEvent, 50_000)
	dbChan := make(chan *domain.ArbitrageSignal, 10_000)

	userMgr := domain.NewUserManager()
	fundingMgr := NewFundingManager(cfg)
	aggregator := NewShardedAggregator(trackerChan, fundingMgr, cfg)
	router := NewNotificationRouter(userMgr, aggregator, nil)
	tracker := NewTracker(cfg, dbChan, router)
	connMgr := NewConnectorManager(tickChan, fundingMgr)

	return &Application{
		connManager: connMgr,
		aggregator:  aggregator,
		tracker:     tracker,
		fundingMgr:  fundingMgr,
		config:      cfg,
		userMgr:     userMgr,
		userRepo:    userRepo,
		router:      router,
		tickChan:    tickChan,
		trackerChan: trackerChan,
		dbChan:      dbChan,
	}
}

func (a *Application) SetTelegramSender(tg domain.TelegramSender) {
	a.router.telegram = tg
}

func (a *Application) SetAdminIDs(ids []int64) {
	a.adminIDs = ids
}

func (a *Application) GetConnectorManager() *ConnectorManager {
	return a.connManager
}

func (a *Application) isAdmin(chatID int64) bool {
	if len(a.adminIDs) == 0 {
		return true
	}
	for _, id := range a.adminIDs {
		if chatID == id {
			return true
		}
	}
	return false
}

// getContext возвращает контекст приложения без блокировок
// atomic.Load — O(1), lock-free, безопасно для конкурентного доступа
// Возвращает context.Background() если Run() ещё не был вызван
func (a *Application) getContext() context.Context {
	if ptr := a.ctx.Load(); ptr != nil {
		return *ptr
	}
	return context.Background()
}

func (a *Application) Run(ctx context.Context, repo domain.SignalRepository) error {
	// Сохраняем контекст атомарно до старта горутин
	// Store выполняется один раз — гарантия happens-before для всех последующих Load()
	a.ctx.Store(&ctx)

	log.Println("🚀 Starting High-Performance Arbitrage Engine...")

	// Загружаем пользователей из БД в кэш
	users, err := a.userRepo.GetAllUsers(ctx)
	if err != nil {
		log.Printf("⚠️  Failed to load users from DB: %v", err)
	} else {
		for _, u := range users {
			a.userMgr.SetUser(u)
		}
		log.Printf("✅ Loaded %d users from DB", len(users))
	}

	// PersistenceWorker стартует первым в отдельной WaitGroup
	// Завершится последним — после закрытия dbChan
	a.persistWg.Add(1)
	go NewPersistenceWorker(a.dbChan, repo).Start(ctx, &a.persistWg)

	// Ingestion воркеры: по 2 на каждый CPU, минимум 8
	workerCount := runtime.NumCPU() * 2
	if workerCount < 8 {
		workerCount = 8
	}
	for i := 0; i < workerCount; i++ {
		a.workerWg.Add(1)
		go a.ingestionWorker(ctx)
	}

	// Tracker воркеры
	for i := 0; i < 4; i++ {
		a.workerWg.Add(1)
		go a.trackerWorker(ctx)
	}

	log.Printf("✅ Engine started: %d ingestion workers, 4 tracker workers", workerCount)

	<-ctx.Done()
	log.Println("🛑 Graceful shutdown initiated...")

	// ═══════════════════════════════════════════════════════
	// Детерминированная цепочка завершения:
	//
	// [ingestion/tracker workers] ──done──▶ close(dbChan)
	//                                              │
	//                                        [persistence
	//                                          worker]
	//                                              │
	//                                           ──done──▶ return nil
	// ═══════════════════════════════════════════════════════

	// Шаг 1: Ждём завершения ingestion и tracker воркеров
	a.workerWg.Wait()
	log.Println("   ↳ [1/3] ingestion & tracker workers stopped")

	// Шаг 2: Закрываем dbChan — теперь безопасно
	// После workerWg.Wait() гарантировано: никто больше не пишет в dbChan
	close(a.dbChan)
	log.Println("   ↳ [2/3] dbChan closed, draining persistence queue...")

	// Шаг 3: Ждём PersistenceWorker — все сигналы записаны в БД
	a.persistWg.Wait()
	log.Println("   ↳ [3/3] persistence worker stopped")

	log.Println("✅ Shutdown complete. All signals persisted.")
	return nil
}

func (a *Application) ingestionWorker(ctx context.Context) {
	defer a.workerWg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case tick, ok := <-a.tickChan:
			if !ok {
				return
			}
			a.aggregator.ProcessTick(tick)
		}
	}
}

func (a *Application) trackerWorker(ctx context.Context) {
	defer a.workerWg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-a.trackerChan:
			if !ok {
				return
			}
			a.tracker.HandleEvent(event)
		}
	}
}

func (a *Application) HandleCommand(chatID int64, username, cmd string, args []string) string {
	appCtx := a.getContext() // lock-free atomic.Load

	// Проверка прав администратора
	adminOnly := map[string]bool{"addex": true, "rmex": true}
	if adminOnly[cmd] && !a.isAdmin(chatID) {
		return "⛔ Access Denied."
	}

	user, exists := a.userMgr.GetUser(chatID)
	if !exists && cmd != "start" {
		return "⚠️ Вы не подписаны. Отправьте /start для начала работы."
	}

	switch cmd {

	case "start":
		if exists {
			return "✅ Вы уже подписаны! /help — список команд."
		}
		newUser := &domain.User{
			ChatID:    chatID,
			Username:  username,
			MinSpread: a.config.GetHardMinSpread(),
			MinVolume: a.config.GetHardMinVolume(),
			Timeframe: domain.TF_15m,
		}
		a.userMgr.SetUser(newUser)

		// ✅ Копия по значению — горутина изолирована от будущих изменений
		go func(u domain.User) {
			if err := a.userRepo.SaveUser(appCtx, &u); err != nil {
				log.Printf("⚠️  SaveUser %d: %v", u.ChatID, err)
			}
		}(*newUser)

		return fmt.Sprintf("🎉 Добро пожаловать, @%s!\nВы подписаны на сигналы.\n/help — список команд.", username)

	case "stop":
		a.userMgr.RemoveUser(chatID)

		// int64 передаётся по значению — никакого race
		go func(cid int64) {
			if err := a.userRepo.DeleteUser(appCtx, cid); err != nil {
				log.Printf("⚠️  DeleteUser %d: %v", cid, err)
			}
		}(chatID)

		return "👋 Вы отписались. /start — чтобы вернуться."

	case "setcross":
		if len(args) < 1 {
			return "Usage: /setcross <percent>"
		}

		// ✅ decimal.NewFromString — точное представление без потерь float64
		val, err := decimal.NewFromString(args[0])
		if err != nil || val.IsNegative() {
			return "❌ Некорректное число. Пример: /setcross 1.5"
		}

		spread := val.Div(decimal.NewFromInt(100))
		if spread.LessThan(a.config.GetHardMinSpread()) {
			spread = a.config.GetHardMinSpread()
		}

		updated := *user
		updated.MinSpread = spread
		a.userMgr.SetUser(&updated)

		go func(u domain.User) {
			if err := a.userRepo.SaveUser(appCtx, &u); err != nil {
				log.Printf("⚠️  SaveUser (spread) %d: %v", u.ChatID, err)
			}
		}(updated)

		return fmt.Sprintf("✅ Минимальный спред: %s%%",
			spread.Mul(decimal.NewFromInt(100)).StringFixed(2))

	case "setvol":
		if len(args) < 1 {
			return "Usage: /setvol <usdt_amount>"
		}

		vol, err := decimal.NewFromString(args[0])
		if err != nil || vol.IsNegative() {
			return "❌ Некорректный объём. Пример: /setvol 500000"
		}

		if vol.LessThan(a.config.GetHardMinVolume()) {
			vol = a.config.GetHardMinVolume()
		}

		updated := *user
		updated.MinVolume = vol
		a.userMgr.SetUser(&updated)

		go func(u domain.User) {
			if err := a.userRepo.SaveUser(appCtx, &u); err != nil {
				log.Printf("⚠️  SaveUser (volume) %d: %v", u.ChatID, err)
			}
		}(updated)

		return fmt.Sprintf("✅ Минимальный объём: $%s", vol.StringFixed(0))

	case "settimeframe":
		if len(args) < 1 {
			return "Usage: /settimeframe <1m|5m|15m|30m|1h|4h|24h>"
		}

		tf := domain.Timeframe(strings.ToLower(args[0]))
		valid := map[domain.Timeframe]bool{
			domain.TF_1m:  true,
			domain.TF_5m:  true,
			domain.TF_15m: true,
			domain.TF_30m: true,
			domain.TF_1h:  true,
			domain.TF_4h:  true,
			domain.TF_24h: true,
		}
		if !valid[tf] {
			return "❌ Неверный таймфрейм. Доступны: 1m, 5m, 15m, 30m, 1h, 4h, 24h"
		}

		updated := *user
		updated.Timeframe = tf
		a.userMgr.SetUser(&updated)

		go func(u domain.User) {
			if err := a.userRepo.SaveUser(appCtx, &u); err != nil {
				log.Printf("⚠️  SaveUser (timeframe) %d: %v", u.ChatID, err)
			}
		}(updated)

		return fmt.Sprintf("✅ Таймфрейм: %s", tf)

	case "help":
		return "📋 *Доступные команды:*\n" +
			"/start — Подписаться на сигналы\n" +
			"/stop — Отписаться\n" +
			"/setcross <%%> — Мин. спред (пример: 1.5)\n" +
			"/setvol <USDT> — Мин. объём (пример: 500000)\n" +
			"/settimeframe <tf> — Таймфрейм (1m, 5m, 15m, 30m, 1h, 4h, 24h)"

	case "addex":
		if len(args) < 1 {
			return "Usage: /addex <exchange>"
		}
		name := strings.ToUpper(args[0])
		var conn domain.ExchangeConnector
		switch name {
		case "BINANCE":
			conn = binance.NewAdapter()
		default:
			return fmt.Sprintf("❌ Exchange %s not supported", name)
		}
		a.connManager.AddConnector(name, conn, appCtx)
		return fmt.Sprintf("✅ Hot-swapped IN: %s", name)

	case "rmex":
		if len(args) < 1 {
			return "Usage: /rmex <exchange>"
		}
		name := strings.ToUpper(args[0])
		a.connManager.RemoveConnector(name)
		return fmt.Sprintf("🛑 Hot-swapped OUT: %s", name)

	default:
		return "❓ Неизвестная команда. /help — список команд."
	}
}
