package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"crypto-screener/internal/adapters/binance"
	"crypto-screener/internal/adapters/bingx"
	"crypto-screener/internal/adapters/bitget"
	"crypto-screener/internal/adapters/bybit"
	"crypto-screener/internal/adapters/gateio"
	"crypto-screener/internal/adapters/kucoin"
	"crypto-screener/internal/adapters/mexc"
	"crypto-screener/internal/adapters/okx"
	"crypto-screener/internal/adapters/postgres"
	"crypto-screener/internal/adapters/telegram"
	"crypto-screener/internal/app"
	"crypto-screener/internal/config"
	"crypto-screener/internal/domain"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Printf("⚠️ .env load error: %v", err)
	}
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Config error: %v", err)
	}
	if cfg.TelegramToken == "" {
		log.Fatal("Config error: TELEGRAM_TOKEN is required")
	}
	if len(cfg.AdminChatIDs) == 0 {
		log.Fatal("Config error: ADMIN_CHAT_IDS must contain at least one administrator")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	dbRepo, err := postgres.NewRepository(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("DB Error: %v", err)
	}
	defer dbRepo.Close()

	application, err := app.NewApplication(domain.NewScreenerConfig(cfg.HardMinSpread), dbRepo, dbRepo)
	if err != nil {
		log.Fatalf("Application error: %v", err)
	}
	application.SetAdminIDs(cfg.AdminChatIDs)
	application.SetConnectorFactory(func(name string) (domain.ExchangeConnector, bool) {
		switch name {
		case "BINANCE":
			return binance.NewAdapter(), true
		case "BINGX":
			return bingx.NewAdapter(), true
		case "BITGET":
			return bitget.NewAdapter(), true
		case "BYBIT":
			return bybit.NewAdapter(), true
		case "GATEIO":
			return gateio.NewAdapter(), true
		case "KUCOIN":
			return kucoin.NewAdapter(), true
		case "MEXC":
			return mexc.NewAdapter(), true
		case "OKX":
			return okx.NewAdapter(), true
		default:
			return nil, false
		}
	})

	tgBot, err := telegram.NewBot(cfg.TelegramToken, application)
	if err != nil {
		log.Printf("TG Error: %v", err)
		return
	}
	defer tgBot.Close()
	application.SetTelegramSender(tgBot)

	appErr := make(chan error, 1)
	tgErr := make(chan error, 1)
	go func() { appErr <- application.Run(ctx, dbRepo) }()
	go func() { tgErr <- tgBot.StartPolling(ctx) }()

	select {
	case err := <-appErr:
		if err != nil {
			cancel()
			log.Printf("App failed: %v", err)
			return
		}
	case err := <-tgErr:
		if err != nil {
			log.Printf("Telegram polling stopped: %v", err)
		}
		cancel()
		if err := <-appErr; err != nil {
			log.Printf("App shutdown: %v", err)
		}
	}
}
