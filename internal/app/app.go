package app

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/ingress"
	"crypto-screener/internal/observability"
	"github.com/shopspring/decimal"
)

const userPersistenceTimeout = 5 * time.Second

// SupportedExchanges — полный список бирж, поддерживаемых коннекторами.
// Используется для метрик funding-возраста и стартовой проверки доступности.
var SupportedExchanges = []string{"BINANCE", "BINGX", "BITGET", "BYBIT", "GATEIO", "KUCOIN", "MEXC", "OKX"}

// Ключи персистентных настроек оператора (таблица settings).
const (
	settingsKeyHardSpread = "hard_min_spread"
	settingsKeyFees       = "fees"
)

var ErrApplicationAlreadyStarted = errors.New("application has already been started")

type ConnectorFactory func(name string) (domain.ExchangeConnector, bool)

type Application struct {
	connManager *ConnectorManager
	aggregator  *ShardedAggregator
	volume      *VolumeEngine
	tracker     *Tracker
	fundingMgr  *FundingManager
	config      *domain.ScreenerConfig
	adminMu     sync.RWMutex
	adminIDs    []int64
	userMgr     *domain.UserManager
	userRepo    domain.UserRepository
	// Опциональные возможности репозитория (postgres их реализует);
	// при отсутствии приложение корректно деградирует.
	settingsRepo     domain.SettingsRepository
	statsRepo        domain.StatsRepository
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
	dispatchStop     chan struct{}
	ingestionStop    chan struct{}
	connectorFactory atomic.Value
}

func NewApplication(cfg *domain.ScreenerConfig, repo domain.SignalRepository, userRepo domain.UserRepository) (*Application, error) {
	if cfg == nil {
		cfg = domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.Zero)
	}
	tickChan := make(chan domain.MarketTick, 200_000)
	trackerChan := make(chan domain.SpreadEvent, 50_000)
	// Lifecycle events are sparse compared with market ticks. Keep a generous
	// bounded queue so a short database stall cannot propagate into market-data
	// ingestion; the persistence worker retries each item independently.
	dbChan := make(chan *domain.ArbitrageSignal, 100_000)
	userMgr := domain.NewUserManager()
	fundingCfg := DefaultFundingConfig()
	fundingCfg.MinSpread = cfg.GetHardMinSpread()
	fundingMgr, err := NewFundingManager(fundingCfg, nil)
	if err != nil {
		return nil, fmt.Errorf("create funding manager: %w", err)
	}
	volume := NewVolumeEngine()
	aggregator := NewShardedAggregator(trackerChan, fundingMgr, cfg, userMgr, volume)
	router := NewNotificationRouter(userMgr, nil, volume)
	tracker := NewTracker(cfg, dbChan, router)
	connMgr := NewConnectorManager(tickChan, fundingMgr, volume)
	a := &Application{connManager: connMgr, aggregator: aggregator, tracker: tracker, fundingMgr: fundingMgr, volume: volume, config: cfg, userMgr: userMgr, userRepo: userRepo, router: router, tickChan: tickChan, trackerChan: trackerChan, dbChan: dbChan}
	if settingsRepo, ok := repo.(domain.SettingsRepository); ok {
		a.settingsRepo = settingsRepo
	}
	if statsRepo, ok := repo.(domain.StatsRepository); ok {
		a.statsRepo = statsRepo
	}
	return a, nil
}
func (a *Application) SetTelegramSender(tg domain.TelegramSender) { a.router.SetTelegramSender(tg) }
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
	if repo == nil {
		a.started.Store(false)
		return errors.New("signal repository is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if reconciler, ok := repo.(interface {
		ReconcileActiveSignals(context.Context) error
	}); ok {
		if err := reconciler.ReconcileActiveSignals(ctx); err != nil {
			a.started.Store(false)
			return fmt.Errorf("reconcile active signals: %w", err)
		}
	}
	a.ctx.Store(&ctx)
	// Репозиторий передаётся в Run отдельно: берём из него опциональные
	// возможности, если NewApplication их ещё не увидел.
	if settingsRepo, ok := repo.(domain.SettingsRepository); ok {
		a.settingsRepo = settingsRepo
	}
	if statsRepo, ok := repo.(domain.StatsRepository); ok {
		a.statsRepo = statsRepo
	}
	a.loadOperatorSettings(ctx)

	var persistCancel context.CancelFunc
	persistStarted, dispatchStarted, trackerStarted := false, false, false
	cleanupOnce := sync.Once{}
	cleanup := func() error {
		var first error
		cleanupOnce.Do(func() {
			a.accepting.Store(false)
			if err := a.connManager.StopAll(); err != nil {
				first = err
			}
			if dispatchStarted {
				ingress.DrainAndStop(a.tickChan)
				// Market ticks are transient. During shutdown we must not drain a potentially
				// huge backlog through the arbitrage engine: doing so can keep shutdown
				// behind tracker/DB backpressure for an unbounded amount of time. Stop the
				// dispatch and ingestion workers first, then close their queues.
				close(a.ingestionStop)
				close(a.dispatchStop)
				close(a.tickChan)
				a.ingestionWg.Wait()
				if first == nil {
					ingress.Forget(a.tickChan)
				}
			}
			if trackerStarted {
				flushCtx, cancelFlush := context.WithTimeout(context.Background(), 10*time.Second)
				events := a.aggregator.FlushActive(time.Now())
			flushLoop:
				for _, ev := range events {
					select {
					case a.trackerChan <- ev:
					case <-flushCtx.Done():
						if first == nil {
							first = fmt.Errorf("tracker flush enqueue timed out: %w", flushCtx.Err())
						}
						break flushLoop
					}
				}
				cancelFlush()
				close(a.trackerChan)
				a.trackerWg.Wait()
			}
			if persistStarted {
				close(a.dbChan)
				done := make(chan struct{})
				go func() { a.persistWg.Wait(); close(done) }()
				select {
				case <-done:
				case <-time.After(60 * time.Second):
					if persistCancel != nil {
						persistCancel()
					}
					if first == nil {
						first = errors.New("persistence shutdown timed out after 60s")
					}
					// SaveSignal calls are individually bounded to persistenceTimeout.
					// Wait a final bounded grace period so main never closes the DB pool
					// underneath a still-running persistence goroutine.
					grace := time.NewTimer(persistenceTimeout + time.Second)
					select {
					case <-done:
						grace.Stop()
					case <-grace.C:
					}
				}
			}
		})
		return first
	}
	defer a.router.Close()

	var users []*domain.User
	var err error
	if a.userRepo != nil {
		users, err = a.userRepo.GetAllUsers(ctx)
	} else {
		err = errors.New("user repository is nil")
	}
	if err != nil {
		return fmt.Errorf("load users from database: %w", err)
	}
	{
		for _, u := range users {
			if u == nil {
				continue
			}
			if u.MinSpread.LessThan(a.config.GetHardMinSpread()) {
				u.MinSpread = a.config.GetHardMinSpread()
			}
			if u.MinVolume.IsNegative() {
				u.MinVolume = decimal.Zero
			}
			if u.MinFundingMinutes < 0 {
				u.MinFundingMinutes = 0
			}
			if _, ok := map[domain.Timeframe]bool{domain.TF_1m: true, domain.TF_5m: true, domain.TF_15m: true, domain.TF_30m: true, domain.TF_1h: true, domain.TF_4h: true, domain.TF_24h: true}[u.Timeframe]; !ok {
				u.Timeframe = domain.TF_15m
			}
			a.userMgr.SetUser(u)
		}
		slog.Info("loaded users from database", "count", len(users))
	}
	factory, hasFactory := a.connectorFactory.Load().(ConnectorFactory)
	if hasFactory && factory == nil {
		return errors.New("connector factory is nil")
	}

	// Persistence has its own lifecycle context. The market-data Run context is
	// cancelled when shutdown begins, but accepted DB events must still be drained
	// and retried until the bounded shutdown deadline.
	persistCtx, persistCancel := context.WithCancel(context.Background())
	defer persistCancel()
	a.persistWg.Add(1)
	go NewPersistenceWorker(a.dbChan, repo).Start(persistCtx, &a.persistWg)
	persistStarted = true
	workers := runtime.NumCPU() * 2
	if workers < 8 {
		workers = 8
	}
	if workers > 128 {
		workers = 128
	}
	a.dispatchStop = make(chan struct{})
	a.ingestionStop = make(chan struct{})
	a.ingestionQueues = make([]chan domain.MarketTick, workers)
	for i := range a.ingestionQueues {
		a.ingestionQueues[i] = make(chan domain.MarketTick, 4096)
		a.ingestionWg.Add(1)
		go a.ingestionWorker(a.ingestionQueues[i])
	}
	a.ingestionWg.Add(1)
	go a.dispatchTicks()
	dispatchStarted = true
	a.trackerWg.Add(1)
	go a.trackerWorker()
	trackerStarted = true
	a.registerMetrics()
	if factory != nil {
		for _, name := range SupportedExchanges {
			conn, ok := factory(name)
			if !ok || conn == nil {
				cleanup()
				return fmt.Errorf("connector factory does not provide %s", name)
			}
			if err := a.connManager.AddConnector(ctx, name, conn); err != nil {
				cleanup()
				return fmt.Errorf("start connector %s: %w", name, err)
			}
		}
	}
	a.accepting.Store(true)
	<-ctx.Done()
	if err := cleanup(); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	slog.Info("shutdown complete")
	return nil
}

func (a *Application) dispatchTicks() {
	defer a.ingestionWg.Done()
	defer func() {
		for _, q := range a.ingestionQueues {
			close(q)
		}
	}()
	if len(a.ingestionQueues) == 0 {
		return
	}
	for {
		select {
		case <-a.dispatchStop:
			return
		case tick, ok := <-a.tickChan:
			if !ok {
				return
			}
			idx := int(crc32.ChecksumIEEE([]byte(tick.Symbol)) % uint32(len(a.ingestionQueues)))
			select {
			case a.ingestionQueues[idx] <- tick:
			case <-a.dispatchStop:
				return
			}
		}
	}
}

func (a *Application) ingestionWorker(q <-chan domain.MarketTick) {
	defer a.ingestionWg.Done()
	for {
		select {
		case <-a.ingestionStop:
			return
		case tick, ok := <-q:
			if !ok {
				return
			}
			a.aggregator.ProcessTick(tick)
			observability.Tick(tick.Exchange)
		}
	}
}

func (a *Application) trackerWorker() {
	defer a.trackerWg.Done()
	for event := range a.trackerChan {
		a.tracker.HandleEvent(event)
	}
}

// loadOperatorSettings восстанавливает персистентные настройки оператора
// (hard floor спреда, комиссии) из таблицы settings. Вызывается один раз в Run.
func (a *Application) loadOperatorSettings(ctx context.Context) {
	if a.settingsRepo == nil {
		return
	}
	if v, ok, err := a.settingsRepo.GetSetting(ctx, settingsKeyHardSpread); err != nil {
		slog.Warn("load hard_min_spread setting failed", "error", err)
	} else if ok {
		if d, perr := decimal.NewFromString(v); perr != nil {
			slog.Warn("stored hard_min_spread is not a decimal", "value", v)
		} else if !d.LessThan(decimal.RequireFromString("0.01")) {
			fundingCfg := DefaultFundingConfig()
			fundingCfg.MinSpread = d
			if uerr := a.fundingMgr.UpdateConfig(fundingCfg); uerr != nil {
				slog.Warn("apply stored hard_min_spread to funding config failed", "error", uerr)
			} else {
				a.config.SetHardMinSpread(d)
				slog.Info("restored hard_min_spread from settings", "value", d.String())
			}
		} else {
			slog.Warn("stored hard_min_spread below absolute minimum, ignored", "value", v)
		}
	}
	if v, ok, err := a.settingsRepo.GetSetting(ctx, settingsKeyFees); err != nil {
		slog.Warn("load fees setting failed", "error", err)
	} else if ok {
		if aerr := a.config.ApplyFees(v); aerr != nil {
			slog.Warn("stored fees setting invalid", "value", v, "error", aerr)
		} else {
			slog.Info("restored fees from settings", "fees", a.config.FeesString())
		}
	}
}

// persistSetting сохраняет настройку оператора в БД. Без SettingsRepository
// (например, в тестах с моками) — тихий no-op: значение действует до рестарта.
func (a *Application) persistSetting(key, value string) error {
	if a.settingsRepo == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.getContext()), 5*time.Second)
	defer cancel()
	return a.settingsRepo.SetSetting(ctx, key, value)
}

// registerMetrics подключает очереди и агрегаты приложения к observability.
func (a *Application) registerMetrics() {
	observability.WatchQueue("ticks", func() int { return len(a.tickChan) })
	observability.WatchQueue("tracker_events", func() int { return len(a.trackerChan) })
	observability.WatchQueue("db_signals", func() int { return len(a.dbChan) })
	observability.RegisterGaugeFunc("screener_active_signals", "Currently active arbitrage signals.", func() float64 {
		return float64(a.tracker.ActiveCount())
	})
	observability.RegisterGaugeFunc("screener_connected_exchanges", "Number of connected exchanges.", func() float64 {
		return float64(a.connManager.Count())
	})
	for _, ex := range SupportedExchanges {
		observability.RegisterExchangeGaugeFunc("screener_funding_age_seconds",
			"Seconds since last funding update per exchange; -1 = no data yet.",
			ex, func() float64 {
				if d := a.fundingMgr.FundingAge(ex); d >= 0 {
					return d.Seconds()
				}
				return -1
			})
	}
}

// RouteBot возвращает идентификатор Telegram-бота, обслуживающего чат.
// Используется BotPool'ом в мульти-токенном режиме; для новых/неизвестных
// чатов — бот по умолчанию (id 0).
func (a *Application) RouteBot(chatID int64) int64 {
	if u, ok := a.userMgr.GetUser(chatID); ok && u != nil {
		return u.BotID
	}
	return 0
}

// StatusSnapshot собирает живое состояние приложения для /api/status.
func (a *Application) StatusSnapshot() map[string]any {
	snapshot := map[string]any{
		"active_signals":      a.tracker.ActiveCount(),
		"connected_exchanges": a.connManager.Count(),
	}
	if names := a.connManager.Names(); len(names) > 0 {
		snapshot["exchanges"] = names
	}
	ages := make(map[string]string, len(SupportedExchanges))
	for _, ex := range SupportedExchanges {
		if d := a.fundingMgr.FundingAge(ex); d >= 0 {
			ages[ex] = d.Round(time.Second).String()
		} else {
			ages[ex] = "no data"
		}
	}
	snapshot["funding_age"] = ages
	return snapshot
}

func (a *Application) persistUser(user *domain.User) error {
	if user == nil {
		return errors.New("user is nil")
	}
	if a.userRepo == nil {
		return errors.New("user repository is nil")
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.getContext()), userPersistenceTimeout)
	defer cancel()
	if err := a.userRepo.SaveUser(ctx, user); err != nil {
		return fmt.Errorf("save user %d: %w", user.ChatID, err)
	}
	return nil
}
func (a *Application) deleteUser(chatID int64) error {
	if a.userRepo == nil {
		return errors.New("user repository is nil")
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.getContext()), userPersistenceTimeout)
	defer cancel()
	if err := a.userRepo.DeleteUser(ctx, chatID); err != nil {
		return fmt.Errorf("delete user %d: %w", chatID, err)
	}
	return nil
}

func (a *Application) HandleCommand(botID, chatID int64, username, cmd string, args []string) string {
	a.commandMu.Lock()
	defer a.commandMu.Unlock()
	if !a.accepting.Load() {
		return "⚠️ Двигатель сейчас остановлен."
	}
	// Admin commands must work without a subscription: an administrator is not
	// required to be a signal subscriber to manage the screener.
	isAdminCmd := cmd == "addex" || cmd == "rmex" || cmd == "sethardspread" || cmd == "setfees" || cmd == "stats"
	if isAdminCmd && !a.isAdmin(chatID) {
		return "⛔ Access Denied."
	}
	user, exists := a.userMgr.GetUser(chatID)
	if !exists && cmd != "start" && !isAdminCmd {
		return "⚠️ Вы не подписаны. Отправьте /start для начала работы."
	}
	switch cmd {
	case "start":
		if exists {
			return "✅ Вы уже подписаны! /help — список команд."
		}
		u := &domain.User{ChatID: chatID, Username: username, MinSpread: a.config.GetHardMinSpread(), MinVolume: decimal.Zero, Timeframe: domain.TF_15m, MinFundingMinutes: 30, BotID: botID}
		if err := a.persistUser(u); err != nil {
			return fmt.Sprintf("❌ Не удалось сохранить подписку: %v", err)
		}
		a.userMgr.SetUser(u)
		return fmt.Sprintf("🎉 Добро пожаловать, @%s!\nВы подписаны на сигналы.\n/help — список команд.", username)
	case "stop":
		if err := a.deleteUser(chatID); err != nil {
			return fmt.Sprintf("❌ Не удалось сохранить отписку: %v", err)
		}
		a.userMgr.RemoveUser(chatID)
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
		fundingCfg := DefaultFundingConfig()
		fundingCfg.MinSpread = spread
		if err := a.fundingMgr.UpdateConfig(fundingCfg); err != nil {
			return fmt.Sprintf("❌ Не удалось обновить funding-конфигурацию: %v", err)
		}
		spread = a.config.SetHardMinSpread(spread)
		if err := a.persistSetting(settingsKeyHardSpread, spread.String()); err != nil {
			return fmt.Sprintf("⚠️ Порог применён, но не сохранён в БД (подействует до рестарта): %v", err)
		}
		return fmt.Sprintf("✅ Глобальный hard floor спреда: %s%% (сохранено)", spread.Mul(decimal.NewFromInt(100)).StringFixed(2))
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
		if err := a.persistUser(&u); err != nil {
			return fmt.Sprintf("❌ Не удалось сохранить настройки: %v", err)
		}
		a.userMgr.SetUser(&u)
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
		u := *user
		u.MinVolume = v
		if err := a.persistUser(&u); err != nil {
			return fmt.Sprintf("❌ Не удалось сохранить настройки: %v", err)
		}
		a.userMgr.SetUser(&u)
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
		if err := a.persistUser(&u); err != nil {
			return fmt.Sprintf("❌ Не удалось сохранить настройки: %v", err)
		}
		a.userMgr.SetUser(&u)
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
		u := *user
		u.Timeframe = tf
		if err := a.persistUser(&u); err != nil {
			return fmt.Sprintf("❌ Не удалось сохранить настройки: %v", err)
		}
		a.userMgr.SetUser(&u)
		return fmt.Sprintf("✅ Таймфрейм: %s", tf)
	case "setfees":
		if !a.isAdmin(chatID) {
			return "⛔ Access Denied."
		}
		if len(args) == 0 {
			return "💰 Комиссии (taker, доля на сторону сделки):\n" + a.config.FeesString() + "\n\nUsage: /setfees БИРЖА:ДОЛЯ[,БИРЖА:ДОЛЯ...]\nПример: /setfees DEFAULT:0.0005,BINANCE:0.0004\nDEFAULT применяется ко всем биржам без своего значения."
		}
		parsed, err := domain.ParseFees(strings.Join(args, ","))
		if err != nil {
			return fmt.Sprintf("❌ %v", err)
		}
		for k, v := range parsed {
			a.config.SetFee(k, v)
		}
		if err := a.persistSetting(settingsKeyFees, a.config.FeesString()); err != nil {
			return fmt.Sprintf("⚠️ Комиссии применены, но не сохранены в БД: %v", err)
		}
		return "✅ Комиссии обновлены:\n" + a.config.FeesString()
	case "stats":
		if !a.isAdmin(chatID) {
			return "⛔ Access Denied."
		}
		if a.statsRepo == nil {
			return "❌ Статистика недоступна: репозиторий не поддерживает агрегаты."
		}
		statsCtx, cancel := context.WithTimeout(context.WithoutCancel(a.getContext()), 5*time.Second)
		defer cancel()
		st, err := a.statsRepo.SignalStats24h(statsCtx)
		if err != nil {
			return fmt.Sprintf("❌ Ошибка статистики: %v", err)
		}
		avgPeak := "—"
		if !st.AvgPeakSpread.IsZero() {
			avgPeak = st.AvgPeakSpread.Mul(decimal.NewFromInt(100)).StringFixed(4) + "%"
		}
		avgDur := "—"
		if st.AvgDuration > 0 {
			avgDur = st.AvgDuration.Round(time.Second).String()
		}
		return fmt.Sprintf("📊 Статистика за 24 часа:\nОткрыто сигналов: %d\nЗакрыто: %d\nСредний пик спреда (net): %s\nСредняя длительность: %s\nАктивно сейчас: %d", st.Opened24h, st.Closed24h, avgPeak, avgDur, a.tracker.ActiveCount())
	case "help":
		return "📋 *Доступные команды:*\n/start — Подписаться на сигналы\n/stop — Отписаться\n/setcross <%> — Мин. спред\n/setvol <USDT> — Мин. объём\n/settimeframe <tf> — Таймфрейм\n/setfundingtime <minutes> — Не присылать сигнал ближе к funding\n\n*Администраторам:*\n/sethardspread <%> — Глобальный минимум спреда\n/setfees — Комиссии бирж (учитываются в спреде)\n/addex /rmex — Подключение бирж на лету\n/stats — Статистика сигналов за 24 часа"
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
