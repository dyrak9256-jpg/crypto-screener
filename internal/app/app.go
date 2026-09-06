package app

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
)

const userPersistenceTimeout = 5 * time.Second

var ErrApplicationAlreadyStarted = errors.New("application has already been started")

type ConnectorFactory func(name string) (domain.ExchangeConnector, bool)

type Application struct {
	connManager      *ConnectorManager
	aggregator       *ShardedAggregator
	volume           *VolumeEngine
	tracker          *Tracker
	fundingMgr       *FundingManager
	config           *domain.ScreenerConfig
	adminMu          sync.RWMutex
	adminIDs         []int64
	userMgr          *domain.UserManager
	userRepo         domain.UserRepository
	router           *NotificationRouter
	tickChan         chan domain.MarketTick
	trackerChan      chan domain.SpreadEvent
	dbChan           chan *domain.ArbitrageSignal
	ctx              atomic.Pointer[context.Context]
	started          atomic.Bool
	accepting        atomic.Bool
	commandMu        sync.Mutex
	ingestionWg      sync.WaitGroup
	trackerWg        sync.WaitGroup
	persistWg        sync.WaitGroup
	ingestionQueues  []chan domain.MarketTick
	connectorFactory atomic.Value
}

func NewApplication(cfg *domain.ScreenerConfig, repo domain.SignalRepository, userRepo domain.UserRepository) *Application {
	if cfg == nil {
		cfg = domain.NewScreenerConfig(decimal.Zero, decimal.Zero)
	}
	tickChan := make(chan domain.MarketTick, 200_000)
	trackerChan := make(chan domain.SpreadEvent, 50_000)
	dbChan := make(chan *domain.ArbitrageSignal, 10_000)
	userMgr := domain.NewUserManager()
	fundingCfg := DefaultFundingConfig()
	fundingCfg.MinSpread = cfg.GetHardMinSpread()
	fundingMgr, err := NewFundingManager(fundingCfg, nil)
	if err != nil {
		panic(fmt.Sprintf("invalid funding configuration: %v", err))
	}
	volume := NewVolumeEngine()
	aggregator := NewShardedAggregator(trackerChan, fundingMgr, cfg, userMgr, volume)
	router := NewNotificationRouter(userMgr, nil, volume)
	tracker := NewTracker(cfg, dbChan, router)
	connMgr := NewConnectorManager(tickChan, fundingMgr, volume)
	return &Application{connManager: connMgr, aggregator: aggregator, tracker: tracker, fundingMgr: fundingMgr, volume: volume, config: cfg, userMgr: userMgr, userRepo: userRepo, router: router, tickChan: tickChan, trackerChan: trackerChan, dbChan: dbChan}
}
func (a *Application) SetTelegramSender(tg domain.TelegramSender) { a.router.telegram = tg }
func (a *Application) SetAdminIDs(ids []int64) {
	copied := append([]int64(nil), ids...)
	a.adminMu.Lock()
	a.adminIDs = copied
	a.adminMu.Unlock()
}
func (a *Application) SetConnectorFactory(factory ConnectorFactory) {
	if factory == nil {
		return
	}
	a.connectorFactory.Store(factory)
}
func (a *Application) GetConnectorManager() *ConnectorManager { return a.connManager }
func (a *Application) isAdmin(chatID int64) bool {
	a.adminMu.RLock()
	defer a.adminMu.RUnlock()
	for _, id := range a.adminIDs {
		if chatID == id {
			return true
		}
	}
	return false
}
func (a *Application) getContext() context.Context {
	if p := a.ctx.Load(); p != nil {
		return *p
	}
	return context.Background()
}

func (a *Application) Run(ctx context.Context, repo domain.SignalRepository) error {
	if !a.started.CompareAndSwap(false, true) {
		return ErrApplicationAlreadyStarted
	}
	if ctx == nil {
		ctx = context.Background()
	}
	a.ctx.Store(&ctx)
	defer a.router.Close()
	var users []*domain.User
	var err error
	if a.userRepo != nil {
		users, err = a.userRepo.GetAllUsers(ctx)
	} else {
		err = errors.New("user repository is nil")
	}
	if err != nil {
		log.Printf("⚠️ Failed to load users from DB: %v", err)
	} else {
		for _, u := range users {
			if u == nil {
				continue
			}
			if u.MinSpread.LessThan(a.config.GetHardMinSpread()) {
				u.MinSpread = a.config.GetHardMinSpread()
			}
			if u.MinVolume.LessThan(a.config.GetHardMinVolume()) {
				u.MinVolume = a.config.GetHardMinVolume()
			}
			a.userMgr.SetUser(u)
		}
		log.Printf("✅ Loaded %d users from DB", len(users))
	}
	factory, hasFactory := a.connectorFactory.Load().(ConnectorFactory)
	if hasFactory && factory == nil {
		return errors.New("connector factory is nil")
	}
	persistCtx, persistCancel := context.WithCancel(context.Background())
	defer persistCancel()
	a.persistWg.Add(1)
	go NewPersistenceWorker(a.dbChan, repo).Start(persistCtx, &a.persistWg)
	workers := runtime.NumCPU() * 2
	if workers < 8 {
		workers = 8
	}
	if workers > 128 {
		workers = 128
	}
	a.ingestionQueues = make([]chan domain.MarketTick, workers)
	for i := range a.ingestionQueues {
		a.ingestionQueues[i] = make(chan domain.MarketTick, 4096)
		a.ingestionWg.Add(1)
		go a.ingestionWorker(a.ingestionQueues[i])
	}
	a.ingestionWg.Add(1)
	go a.dispatchTicks()
	// One tracker worker deliberately preserves event order. The market pipeline is
	// already heavily parallelized; correctness of OPEN/CLOSE ordering is more important here.
	a.trackerWg.Add(1)
	go a.trackerWorker()
	if factory != nil {
		for _, name := range []string{"BINANCE", "BINGX", "BITGET", "BYBIT", "GATEIO", "KUCOIN", "MEXC", "OKX"} {
			conn, ok := factory(name)
			if !ok || conn == nil {
				return fmt.Errorf("connector factory does not provide %s", name)
			}
			if err := a.connManager.AddConnector(ctx, name, conn); err != nil {
				return fmt.Errorf("start connector %s: %w", name, err)
			}
		}
	}
	a.accepting.Store(true)
	<-ctx.Done()
	a.accepting.Store(false)

	if err := a.connManager.StopAll(); err != nil {
		return fmt.Errorf("connector shutdown: %w", err)
	}
	close(a.tickChan)
	a.ingestionWg.Wait()
	for _, event := range a.aggregator.FlushActive(time.Now()) {
		a.trackerChan <- event
	}
	close(a.trackerChan)
	a.trackerWg.Wait()
	close(a.dbChan)
	persistDone := make(chan struct{})
	go func() { a.persistWg.Wait(); close(persistDone) }()
	select {
	case <-persistDone:
	case <-time.After(60 * time.Second):
		persistCancel()
		return errors.New("persistence shutdown timed out after 60s")
	}
	if err := a.tracker.Stop(5 * time.Second); err != nil {
		log.Printf("⚠️ %v", err)
	}
	log.Println("✅ Shutdown complete")
	return nil
}
func (a *Application) dispatchTicks() {
	defer a.ingestionWg.Done()
	if len(a.ingestionQueues) == 0 {
		return
	}
	for tick := range a.tickChan {
		idx := int(crc32.ChecksumIEEE([]byte(tick.Symbol)) % uint32(len(a.ingestionQueues)))
		a.ingestionQueues[idx] <- tick
	}
	for _, q := range a.ingestionQueues {
		close(q)
	}
}

func (a *Application) ingestionWorker(q <-chan domain.MarketTick) {
	defer a.ingestionWg.Done()
	for tick := range q {
		a.aggregator.ProcessTick(tick)
	}
}

func (a *Application) trackerWorker() {
	defer a.trackerWg.Done()
	for event := range a.trackerChan {
		a.tracker.HandleEvent(event)
	}
}

func (a *Application) persistUser(user *domain.User) {
	if user == nil || a.userRepo == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.getContext()), userPersistenceTimeout)
	defer cancel()
	if err := a.userRepo.SaveUser(ctx, user); err != nil {
		log.Printf("⚠️ SaveUser %d: %v", user.ChatID, err)
	}
}
func (a *Application) deleteUser(chatID int64) {
	if a.userRepo == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.getContext()), userPersistenceTimeout)
	defer cancel()
	if err := a.userRepo.DeleteUser(ctx, chatID); err != nil {
		log.Printf("⚠️ DeleteUser %d: %v", chatID, err)
	}
}

func (a *Application) HandleCommand(chatID int64, username, cmd string, args []string) string {
	a.commandMu.Lock()
	defer a.commandMu.Unlock()
	if !a.accepting.Load() {
		return "⚠️ Двигатель сейчас остановлен."
	}
	if (cmd == "addex" || cmd == "rmex") && !a.isAdmin(chatID) {
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
		u := &domain.User{ChatID: chatID, Username: username, MinSpread: a.config.GetHardMinSpread(), MinVolume: a.config.GetHardMinVolume(), Timeframe: domain.TF_15m, MinFundingMinutes: 30}
		a.userMgr.SetUser(u)
		a.persistUser(u)
		return fmt.Sprintf("🎉 Добро пожаловать, @%s!\nВы подписаны на сигналы.\n/help — список команд.", username)
	case "stop":
		a.userMgr.RemoveUser(chatID)
		a.deleteUser(chatID)
		return "👋 Вы отписались. /start — чтобы вернуться."
	case "sethardspread":
		if !a.isAdmin(chatID) {
			return "⛔ Access Denied."
		}
		if len(args) < 1 {
			return "Usage: /sethardspread <percent>"
		}
		v, err := decimal.NewFromString(args[0])
		if err != nil || v.LessThan(decimal.Zero) || v.GreaterThan(decimal.NewFromInt(100)) {
			return "❌ Некорректный процент. Пример: /sethardspread 1"
		}
		spread := v.Div(decimal.NewFromInt(100))
		if spread.LessThan(decimal.RequireFromString("0.01")) {
			return "❌ Административный минимум — 1%."
		}
		spread = a.config.SetHardMinSpread(spread)
		return fmt.Sprintf("✅ Глобальный hard floor спреда: %s%%", spread.Mul(decimal.NewFromInt(100)).StringFixed(2))
	case "setcross":
		if len(args) < 1 {
			return "Usage: /setcross <percent>"
		}
		v, err := decimal.NewFromString(args[0])
		if err != nil || v.IsNegative() {
			return "❌ Некорректное число. Пример: /setcross 1.5"
		}
		if v.GreaterThan(decimal.NewFromInt(100)) {
			return "❌ Спред не может быть больше 100%."
		}
		spread := v.Div(decimal.NewFromInt(100))
		if spread.LessThan(a.config.GetHardMinSpread()) {
			spread = a.config.GetHardMinSpread()
		}
		u := *user
		u.MinSpread = spread
		a.userMgr.SetUser(&u)
		a.persistUser(&u)
		return fmt.Sprintf("✅ Минимальный спред: %s%%", spread.Mul(decimal.NewFromInt(100)).StringFixed(2))
	case "setvol":
		if len(args) < 1 {
			return "Usage: /setvol <usdt_amount>"
		}
		v, err := decimal.NewFromString(args[0])
		if err != nil || v.IsNegative() {
			return "❌ Некорректный объём. Пример: /setvol 500000"
		}
		if v.GreaterThan(decimal.NewFromInt(1_000_000_000)) {
			return "❌ Объём слишком большой."
		}
		if v.LessThan(a.config.GetHardMinVolume()) {
			v = a.config.GetHardMinVolume()
		}
		u := *user
		u.MinVolume = v
		a.userMgr.SetUser(&u)
		a.persistUser(&u)
		return fmt.Sprintf("✅ Минимальный объём: $%s", v.StringFixed(0))
	case "setfundingtime":
		if len(args) < 1 {
			return "Usage: /setfundingtime <minutes>"
		}
		mins, err := strconv.Atoi(args[0])
		if err != nil || mins < 0 || mins > 10080 {
			return "❌ Время должно быть от 0 до 10080 минут."
		}
		u := *user
		u.MinFundingMinutes = mins
		a.userMgr.SetUser(&u)
		a.persistUser(&u)
		return fmt.Sprintf("✅ Не присылать сигнал, если до funding меньше %d мин.", mins)
	case "settimeframe":
		if len(args) < 1 {
			return "Usage: /settimeframe <1m|5m|15m|30m|1h|4h|24h>"
		}
		tf := domain.Timeframe(strings.ToLower(args[0]))
		valid := map[domain.Timeframe]bool{domain.TF_1m: true, domain.TF_5m: true, domain.TF_15m: true, domain.TF_30m: true, domain.TF_1h: true, domain.TF_4h: true, domain.TF_24h: true}
		if !valid[tf] {
			return "❌ Неверный таймфрейм. Доступны: 1m, 5m, 15m, 30m, 1h, 4h, 24h"
		}
		if tf != domain.TF_24h {
			return fmt.Sprintf("❌ Неверный таймфрейм. Доступны: 1m, 5m, 15m, 30m, 1h, 4h, 24h")
		}
		u := *user
		u.Timeframe = tf
		a.userMgr.SetUser(&u)
		a.persistUser(&u)
		return fmt.Sprintf("✅ Таймфрейм: %s", tf)
	case "help":
		return "📋 *Доступные команды:*\n/start — Подписаться на сигналы\n/stop — Отписаться\n/sethardspread <%> — Глобальный минимум спреда\n/setcross <%> — Мин. спред\n/setvol <USDT> — Мин. объём\n/settimeframe <tf> — Таймфрейм\n/setfundingtime <minutes> — Не присылать сигнал ближе к funding"
	case "addex":
		if len(args) < 1 {
			return "Usage: /addex <exchange>"
		}
		name := strings.ToUpper(args[0])
		factory, _ := a.connectorFactory.Load().(ConnectorFactory)
		if factory == nil {
			return "❌ Exchange factory is not configured"
		}
		conn, ok := factory(name)
		if !ok || conn == nil {
			return fmt.Sprintf("❌ Exchange %s not supported", name)
		}
		if err := a.connManager.AddConnector(a.getContext(), name, conn); err != nil {
			return fmt.Sprintf("❌ Failed to add %s: %v", name, err)
		}
		return fmt.Sprintf("✅ Hot-swapped IN: %s", name)
	case "rmex":
		if len(args) < 1 {
			return "Usage: /rmex <exchange>"
		}
		name := strings.ToUpper(args[0])
		if err := a.connManager.RemoveConnector(name); err != nil {
			return fmt.Sprintf("❌ Failed to remove %s: %v", name, err)
		}
		return fmt.Sprintf("🛑 Hot-swapped OUT: %s", name)
	default:
		return "❓ Неизвестная команда. /help — список команд."
	}
}
