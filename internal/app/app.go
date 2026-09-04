package app

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"

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

	userMgr  *domain.UserManager
	userRepo domain.UserRepository
	router   *NotificationRouter

	tickChan    chan domain.MarketTick
	trackerChan chan domain.SpreadEvent
	dbChan      chan *domain.ArbitrageSignal

	ctx context.Context
	wg  sync.WaitGroup
}

func NewApplication(cfg *domain.ScreenerConfig, repo domain.SignalRepository, userRepo domain.UserRepository) *Application {
	tickChan := make(chan domain.MarketTick, 200000)
	trackerChan := make(chan domain.SpreadEvent, 50000)
	dbChan := make(chan *domain.ArbitrageSignal, 10000)

	userMgr := domain.NewUserManager()
	fundingMgr := NewFundingManager(cfg)

	// Aggregator будет выступать в роли VolumeProvider
	aggregator := NewShardedAggregator(trackerChan, fundingMgr, cfg)

	// Router пока без Telegram (инжектим позже)
	router := NewNotificationRouter(userMgr, aggregator, nil)

	tracker := NewTracker(cfg, dbChan, router)
	connManager := NewConnectorManager(tickChan, fundingMgr)

	return &Application{
		connManager: connManager, aggregator: aggregator, tracker: tracker,
		fundingMgr: fundingMgr, config: cfg, userMgr: userMgr, userRepo: userRepo,
		router: router, tickChan: tickChan, trackerChan: trackerChan, dbChan: dbChan,
	}
}

func (a *Application) SetTelegramSender(tg domain.TelegramSender) {
	// Инжектим Telegram в Роутер
	a.router.telegram = tg
}

func (a *Application) Run(ctx context.Context, repo domain.SignalRepository) error {
	a.ctx = ctx
	log.Println("🚀 Starting High-Performance Arbitrage Engine...")

	// Загружаем пользователей из БД в кэш
	users, err := a.userRepo.GetAllUsers(ctx)
	if err != nil {
		log.Printf("⚠️ Failed to load users from DB: %v", err)
	} else {
		for _, u := range users {
			a.userMgr.SetUser(u)
		}
		log.Printf("✅ Loaded %d users from DB", len(users))
	}

	workerCount := runtime.NumCPU() * 2
	if workerCount < 8 {
		workerCount = 8
	}
	for i := 0; i < workerCount; i++ {
		a.wg.Add(1)
		go a.ingestionWorker(ctx)
	}
	for i := 0; i < 4; i++ {
		a.wg.Add(1)
		go a.trackerWorker(ctx)
	}

	a.wg.Add(1)
	go NewPersistenceWorker(a.dbChan, repo).Start(ctx, &a.wg)

	<-ctx.Done()
	log.Println("🛑 Engine shutting down...")
	close(a.dbChan)
	a.wg.Wait()
	log.Println("✅ Shutdown complete.")
	return nil
}

func (a *Application) ingestionWorker(ctx context.Context) {
	defer a.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-a.tickChan:
			a.aggregator.ProcessTick(tick)
		}
	}
}

func (a *Application) trackerWorker(ctx context.Context) {
	defer a.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-a.trackerChan:
			a.tracker.HandleEvent(event)
		}
	}
}

func (a *Application) GetConnectorManager() *ConnectorManager { return a.connManager }

// HandleCommand обрабатывает команды с учетом конкретного пользователя
func (a *Application) HandleCommand(chatID int64, username string, cmd string, args []string) string {
	user, exists := a.userMgr.GetUser(chatID)

	// Если пользователь не найден, разрешаем только /start
	if !exists && cmd != "start" {
		return "⚠️ Вы не подписаны на сигналы. Отправьте /start для начала работы."
	}

	switch cmd {
	case "start":
		if exists {
			return "✅ Вы уже подписаны! Используйте /help для списка команд."
		}
		newUser := &domain.User{
			ChatID:    chatID,
			Username:  username,
			MinSpread: a.config.GetHardMinSpread(),
			MinVolume: a.config.GetHardMinVolume(),
			Timeframe: domain.TF_15m,
		}
		a.userMgr.SetUser(newUser)
		// Сохраняем в БД асинхронно, чтобы не блокировать бота
		go a.userRepo.SaveUser(context.Background(), newUser)
		return fmt.Sprintf("🎉 Добро пожаловать, @%s!\nВы подписаны на сигналы.\nИспользуйте /help для настройки фильтров.", username)

	case "stop":
		a.userMgr.RemoveUser(chatID)
		go a.userRepo.DeleteUser(context.Background(), chatID)
		return "👋 Вы отписались от сигналов. Чтобы вернуться, отправьте /start."

	case "setcross":
		if len(args) < 1 {
			return "Usage: /setcross <percent>"
		}
		val, err := strconv.ParseFloat(args[0], 64)
		if err != nil {
			return "❌ Invalid number."
		}
		decVal := decimal.NewFromFloat(val / 100.0)

		// Применяем жесткий лимит разработчика
		if decVal.LessThan(a.config.GetHardMinSpread()) {
			decVal = a.config.GetHardMinSpread()
		}

		user.MinSpread = decVal
		a.userMgr.SetUser(user)
		go a.userRepo.SaveUser(context.Background(), user)

		return fmt.Sprintf("✅ Минимальный спред установлен на %s%%", decVal.Mul(decimal.NewFromInt(100)).StringFixed(2))

	case "setvol":
		if len(args) < 1 {
			return "Usage: /setvol <usdt_amount>"
		}
		val, err := strconv.ParseFloat(args[0], 64)
		if err != nil {
			return "❌ Invalid number."
		}
		decVal := decimal.NewFromFloat(val)

		if decVal.LessThan(a.config.GetHardMinVolume()) {
			decVal = a.config.GetHardMinVolume()
		}

		user.MinVolume = decVal
		a.userMgr.SetUser(user)
		go a.userRepo.SaveUser(context.Background(), user)

		return fmt.Sprintf("✅ Минимальный объем установлен на $%s", decVal.StringFixed(0))

	case "settimeframe":
		if len(args) < 1 {
			return "Usage: /settimeframe <1m|5m|15m|30m|1h|4h|24h>"
		}
		tf := domain.Timeframe(strings.ToLower(args[0]))
		validTFs := map[domain.Timeframe]bool{
			domain.TF_1m: true, domain.TF_5m: true, domain.TF_15m: true,
			domain.TF_30m: true, domain.TF_1h: true, domain.TF_4h: true, domain.TF_24h: true,
		}
		if !validTFs[tf] {
			return "❌ Invalid timeframe. Use: 1m, 5m, 15m, 30m, 1h, 4h, 24h"
		}

		user.Timeframe = tf
		a.userMgr.SetUser(user)
		go a.userRepo.SaveUser(context.Background(), user)

		return fmt.Sprintf("✅ Таймфрейм для оценки объема установлен на %s", tf)

	case "help":
		return "📋 *Доступные команды:*\n" +
			"/start - Подписаться на сигналы\n" +
			"/stop - Отписаться\n" +
			"/setcross <\\%> - Мин. спред (напр. 1.5)\n" +
			"/setvol <USDT> - Мин. объем (напр. 1000000)\n" +
			"/settimeframe <tf> - Таймфрейм объема (1m, 5m, 15m, 30m, 1h, 4h, 24h)"

	case "addex":
		if len(args) < 1 {
			return "Usage: /addex <name>"
		}
		name := strings.ToUpper(args[0])
		var conn domain.ExchangeConnector
		switch name {
		case "BINANCE":
			conn = binance.NewAdapter()
		default:
			return fmt.Sprintf("❌ Exchange %s not supported", name)
		}
		a.connManager.AddConnector(name, conn, a.ctx)
		return fmt.Sprintf("✅ Hot-swapped IN: %s", name)

	case "rmex":
		if len(args) < 1 {
			return "Usage: /rmex <name>"
		}
		a.connManager.RemoveConnector(strings.ToUpper(args[0]))
		return fmt.Sprintf("🛑 Hot-swapped OUT: %s", strings.ToUpper(args[0]))

	default:
		return "❓ Unknown command. Type /help."
	}
}
