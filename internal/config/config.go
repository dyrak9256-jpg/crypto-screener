package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
)

type Config struct {
	TelegramToken  string
	TelegramChatID int64
	DatabaseURL    string
	HardMinSpread  decimal.Decimal
	HardMinVolume  decimal.Decimal
	AdminChatIDs   []int64 // Comma-separated admin IDs from ADMIN_CHAT_IDS env var
}

func Load() *Config {
	chatID, _ := strconv.ParseInt(getEnv("TELEGRAM_CHAT_ID", "0"), 10, 64)
	hardSpread, _ := decimal.NewFromString(getEnv("HARD_MIN_SPREAD", "0.01"))
	hardVol, _ := decimal.NewFromString(getEnv("HARD_MIN_VOLUME", "1000000"))

	var adminIDs []int64
	if idsStr := getEnv("ADMIN_CHAT_IDS", ""); idsStr != "" {
		for _, idStr := range strings.Split(idsStr, ",") {
			if id, err := strconv.ParseInt(strings.TrimSpace(idStr), 10, 64); err == nil {
				adminIDs = append(adminIDs, id)
			}
		}
	}

	return &Config{
		TelegramToken:  getEnv("TELEGRAM_TOKEN", ""),
		TelegramChatID: chatID,
		DatabaseURL:    getEnv("DATABASE_URL", "postgres://user:pass@localhost:5432/screener?sslmode=disable"),
		HardMinSpread:  hardSpread,
		HardMinVolume:  hardVol,
		AdminChatIDs:   adminIDs,
	}
}


func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
