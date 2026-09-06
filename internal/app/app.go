package app

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"crypto-screener/internal/adapters"
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

	// Независимые WaitGroup для детерминированного shutdown:
	// ingestWg  — приём тиков (останавливается первым; единственный продюсер trackerChan)
	// trackerWg — трекер событий (единственный продюсер dbChan)
	// routerWg  — горутины уведомлений (Broadcast)
	// persistWg — PersistenceWorker (завершается последним)
	ingestWg  sync.WaitGroup
	trackerWg sync.WaitGroup
	routerWg  sync.WaitGroup
	persistWg sync.WaitGroup
}

func NewApplication(
	cfg *domain.ScreenerConfig,
	userRepo domain.UserRepository,
) *Application {
	tickChan := make(chan domain.MarketTick, 200_000)
	trackerChan := make(chan domain.SpreadEvent, 50_000)
	dbChan := make(chan *domain.ArbitrageSignal, 10_000)

	userMgr := domain.NewUserManager()
	fundingMgr := NewFundingManager(cfg)
	aggregator := NewShardedAggregator(trackerChan, fundingMgr, cfg)
	router := NewNotificationRouter(userMgr, aggregator, nil)
	connMgr := NewConnectorManager(tickChan, fundingMgr)

	a := &Application{
		connManager: connMgr,
		aggregator:  aggregator,

		fundingMgr:  fundingMgr,
		config:      cfg,
		userMgr:     userMgr,
		userRepo:    userRepo,
		router:      router,
		tickChan:    tickChan,
		trackerChan: trackerChan,
		dbChan:      dbChan,
	}
	a.tracker = NewTracker(cfg, dbChan, router, &a.routerWg)
	return a
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
	// Если список админов не задан (ADMIN_CHAT_IDS пуст) — доступ к
	// административным командам (/addex, /rmex) запрещён всем.
	// Раньше пустой список означал «все админы» — это позволяло любому
	// пользователю менять активные биржевые коннекторы на живом сервисе.
	if len(a.adminIDs) == 0 {
		return false
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
		a.ingestWg.Add(1)
		go a.ingestionWorker(ctx)
	}

	// ОДИН tracker-воркер: гарантирует ПОСЛЕДОВАТЕЛЬНУЮ обработку событий
	// одного ключа (требование строгого рефакторинга).
	a.trackerWg.Add(1)
	go a.trackerWorker()

	log.Printf("✅ Engine started: %d ingestion workers, 1 tracker worker", workerCount)

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

	// Шаг 1: Останавливаем приём тиков. После этого НЕТ продюсеров, пишущих в trackerChan.
	a.ingestWg.Wait()
	log.Println("   ↳ [1/5] ingestion workers stopped")

	// Шаг 2: Закрываем trackerChan — tracker-воркер дочитает и обработает ВСЕ события.
	close(a.trackerChan)
	a.trackerWg.Wait()
	log.Println("   ↳ [2/5] tracker workers stopped (all signals emitted)")

	// Шаг 3: Ждём горутины уведомлений (Telegram Broadcast).
	a.routerWg.Wait()
	log.Println("   ↳ [3/5] notification workers stopped")

	// Шаг 4: Закрываем dbChan — теперь безопасно (никто больше не пишет).
	close(a.dbChan)
	log.Println("   ↳ [4/5] dbChan closed, draining persistence queue...")

	// Шаг 5: Ждём PersistenceWorker — все сигналы записаны в БД.
	a.persistWg.Wait()
	log.Println("   ↳ [5/5] persistence worker stopped")

	log.Println("✅ Shutdown complete. All signals persisted.")
	return nil
}

func (a *Application) ingestionWorker(ctx context.Context) {
	defer a.ingestWg.Done()
	for {
		select {
		case <-ctx.Done():
			// Выходим; текущий in-flight тик уже обрабатывается (не прерываем его).
			// Буфер tickChan содержит только сырые тики — они эфемерны.
			return
		case tick, ok := <-a.tickChan:
			if !ok {
				return
			}
			a.aggregator.ProcessTick(tick)
		}
	}
}

// trackerWorker читает trackerChan до его закрытия. Это гарантирует, что при
// graceful shutdown Run() закрывает trackerChan ПОСЛЕ остановки всех продюсеров
// (ingestWg.Wait()), поэтому воркер дообрабатывает все события и не выходит
// преждевременно — сигналы не теряются.
func (a *Application) trackerWorker() {
	defer a.trackerWg.Done()
	for event := range a.trackerChan {
		a.tracker.HandleEvent(event)
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
		conn, err := adapters.NewByName(name)
		if err != nil {
			return fmt.Sprintf("❌ %v", err)
		}
		if err := a.connManager.AddConnector(name, conn, appCtx); err != nil {
			return fmt.Sprintf("❌ Failed to add %s: %v", name, err)
		}
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
