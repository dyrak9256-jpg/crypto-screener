package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"crypto-screener/internal/adapters/binance"
	"crypto-screener/internal/adapters/postgres"
	"crypto-screener/internal/adapters/telegram"
	"crypto-screener/internal/app"
	"crypto-screener/internal/config"
	"crypto-screener/internal/domain"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := godotenv.Load(); err != nil {
		log.Println("⚠️ .env file not found. Using system environment variables.")
	}

	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	dbRepo, err := postgres.NewRepository(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("DB Error: %v", err)
	}
	defer dbRepo.Close()

	screenerCfg := domain.NewScreenerConfig(cfg.HardMinSpread, cfg.HardMinVolume)

	// Передаем userRepo в Application
	application := app.NewApplication(screenerCfg, dbRepo, dbRepo)

	// Создаем бота БЕЗ фиксированного chatID
	tgBot, err := telegram.NewBot(cfg.TelegramToken, application)
	if err != nil {
		log.Fatalf("TG Error: %v", err)
	}

	// Инжектим бота в Application (он передаст его в NotificationRouter)
	application.SetTelegramSender(tgBot)

	go tgBot.StartPolling(ctx)

	binanceAdapter := binance.NewAdapter()
	application.GetConnectorManager().AddConnector("BINANCE", binanceAdapter, ctx)

	if err := application.Run(ctx, dbRepo); err != nil {
		log.Fatalf("App failed: %v", err)
	}
}
