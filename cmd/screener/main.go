package main

import (
	"context"
	"crypto-screener/internal/adapters/binance"
	"crypto-screener/internal/adapters/postgres"
	"crypto-screener/internal/adapters/telegram"
	"crypto-screener/internal/app"
	"crypto-screener/internal/config"
	"crypto-screener/internal/domain"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := godotenv.Load(); err != nil {
		log.Println("⚠️ Файл .env не найден. Используем системные переменные окружения.")
	}

	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigChan; log.Println("Received shutdown signal"); cancel() }()

	dbRepo, err := postgres.NewRepository(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("DB Error: %v", err)
	}
	defer dbRepo.Close()

	screenerCfg := domain.NewScreenerConfig(cfg.HardMinSpread, cfg.HardMinVolume)
	application := app.NewApplication(screenerCfg, dbRepo)

	tgBot, err := telegram.NewBot(cfg.TelegramToken, cfg.TelegramChatID, application)
	if err != nil {
		log.Fatalf("TG Error: %v", err)
	}

	application.SetTelegramSender(tgBot)
	go tgBot.StartPolling(ctx)

	binanceAdapter := binance.NewAdapter()
	application.GetConnectorManager().AddConnector("BINANCE", binanceAdapter, ctx)

	if err := application.Run(ctx, dbRepo); err != nil {
		log.Fatalf("App failed: %v", err)
	}
}
