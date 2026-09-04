package config

package config

import (
	"os"
	"strconv"
	"github.com/shopspring/decimal"
)

type Config struct {
	TelegramToken  string
	TelegramChatID int64
	DatabaseURL    string
	HardMinSpread  decimal.Decimal
	HardMinVolume  decimal.Decimal
}

func Load() *Config {
	chatID, _ := strconv.ParseInt(getEnv("TELEGRAM_CHAT_ID", "0"), 10, 64)
	hardSpread, _ := decimal.NewFromString(getEnv("HARD_MIN_SPREAD", "0.01"))
	hardVol, _ := decimal.NewFromString(getEnv("HARD_MIN_VOLUME", "1000000"))

	return &Config{
		TelegramToken:  getEnv("TELEGRAM_TOKEN", ""),
		TelegramChatID: chatID,
		DatabaseURL:    getEnv("DATABASE_URL", "postgres://user:pass@localhost:5432/screener?sslmode=disable"),
		HardMinSpread:  hardSpread,
		HardMinVolume:  hardVol,
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok { return value }
	return fallback
}