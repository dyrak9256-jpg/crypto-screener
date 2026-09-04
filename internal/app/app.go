package app

import (
	"context"
	"crypto-screener/internal/adapters/binance"
	"crypto-screener/internal/domain"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/shopspring/decimal"
)

type Application struct {
	connManager *ConnectorManager
	aggregator  *ShardedAggregator
	tracker     *Tracker
	fundingMgr  *FundingManager
	config      *domain.ScreenerConfig
	tickChan    chan domain.MarketTick
	trackerChan chan domain.SpreadEvent
	dbChan      chan *domain.ArbitrageSignal
	ctx         context.Context
	wg          sync.WaitGroup
}

func NewApplication(cfg *domain.ScreenerConfig, repo domain.SignalRepository) *Application {
	tickChan := make(chan domain.MarketTick, 200000)
	trackerChan := make(chan domain.SpreadEvent, 50000)
	dbChan := make(chan *domain.ArbitrageSignal, 10000)

	fundingMgr := NewFundingManager(cfg)
	tracker := NewTracker(cfg, dbChan)
	aggregator := NewShardedAggregator(trackerChan, fundingMgr, cfg)
	connManager := NewConnectorManager(tickChan, fundingMgr)

	return &Application{
		connManager: connManager, aggregator: aggregator, tracker: tracker,
		fundingMgr: fundingMgr, config: cfg, tickChan: tickChan,
		trackerChan: trackerChan, dbChan: dbChan,
	}
}

func (a *Application) SetTelegramSender(tg domain.TelegramSender) { a.tracker.SetTelegramSender(tg) }

func (a *Application) Run(ctx context.Context, repo domain.SignalRepository) error {
	a.ctx = ctx
	log.Println("🚀 Starting High-Performance Arbitrage Engine...")

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

func (a *Application) HandleCommand(cmd string, args []string) string {
	switch cmd {
	case "setcross":
		if len(args) < 1 {
			return "Usage: /setcross <percent>"
		}
		val, err := strconv.ParseFloat(args[0], 64)
		if err != nil {
			return "Invalid number"
		}
		decVal := decimal.NewFromFloat(val / 100.0)
		effectiveVal := a.config.SetUserCrossSpread(decVal)
		msg := fmt.Sprintf("✅ Cross spread set to %s%%", effectiveVal.Mul(decimal.NewFromInt(100)).StringFixed(2))
		if decVal.LessThan(a.config.GetHardMinSpread()) {
			msg += fmt.Sprintf("\n⚠️ *Note: Hard minimum is %s%%. Overridden.*", a.config.GetHardMinSpread().Mul(decimal.NewFromInt(100)).StringFixed(2))
		}
		return msg
	case "setintra":
		if len(args) < 1 {
			return "Usage: /setintra <percent>"
		}
		val, _ := strconv.ParseFloat(args[0], 64)
		effectiveVal := a.config.SetUserIntraSpread(decimal.NewFromFloat(val / 100.0))
		return fmt.Sprintf("✅ Intra spread set to %s%%", effectiveVal.Mul(decimal.NewFromInt(100)).StringFixed(2))
	case "setvol":
		if len(args) < 1 {
			return "Usage: /setvol <usdt_amount>"
		}
		val, _ := strconv.ParseFloat(args[0], 64)
		effectiveVal := a.config.SetUserMinVolume(decimal.NewFromFloat(val))
		return fmt.Sprintf("✅ Min volume set to $%s", effectiveVal.StringFixed(0))
	case "settimeframe":
		if len(args) < 1 {
			return "Usage: /settimeframe <1m|5m|15m|30m|1h|4h|24h>"
		}
		tf := domain.Timeframe(strings.ToLower(args[0]))
		validTFs := map[domain.Timeframe]bool{domain.TF_1m: true, domain.TF_5m: true, domain.TF_15m: true, domain.TF_30m: true, domain.TF_1h: true, domain.TF_4h: true, domain.TF_24h: true}
		if !validTFs[tf] {
			return "Invalid timeframe."
		}
		a.config.SetUserTimeframe(tf)
		return fmt.Sprintf("✅ Volume timeframe set to %s", tf)
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
			return fmt.Sprintf("Exchange %s not supported", name)
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
		return "Unknown command."
	}
}
