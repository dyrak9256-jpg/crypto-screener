package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
)

type Config struct {
	TelegramToken string
	DatabaseURL   string
	HardMinSpread decimal.Decimal
	HardMinVolume decimal.Decimal
	AdminChatIDs  []int64 // Comma-separated admin IDs from ADMIN_CHAT_IDS env var
}

func Load() *Config {
	// Читаем жёсткие пороги с ВАЛИДАЦИЕЙ ошибок парсинга.
	// Некорректное значение env-переменной не должно молча обнуляться:
	// нулевой порог отключил бы фильтрацию сигналов. При ошибке — фолбэк на дефолт.
	hardSpread, err := decimal.NewFromString(getEnv("HARD_MIN_SPREAD", "0.01"))
	if err != nil {
		log.Printf("⚠️  HARD_MIN_SPREAD = %q is invalid (%v); falling back to 0.01",
			getEnv("HARD_MIN_SPREAD", "0.01"), err)
		hardSpread = decimal.RequireFromString("0.01")
	}
	hardVol, err := decimal.NewFromString(getEnv("HARD_MIN_VOLUME", "1000000"))
	if err != nil {
		log.Printf("⚠️  HARD_MIN_VOLUME = %q is invalid (%v); falling back to 1000000",
			getEnv("HARD_MIN_VOLUME", "1000000"), err)
		hardVol = decimal.RequireFromString("1000000")
	}

	var adminIDs []int64
	if idsStr := getEnv("ADMIN_CHAT_IDS", ""); idsStr != "" {
		for _, idStr := range strings.Split(idsStr, ",") {
			if id, err := strconv.ParseInt(strings.TrimSpace(idStr), 10, 64); err == nil {
				adminIDs = append(adminIDs, id)
			}
		}
	}

	return &Config{
		TelegramToken: getEnv("TELEGRAM_TOKEN", ""),
		DatabaseURL:   getEnv("DATABASE_URL", "postgres://user:pass@localhost:5432/screener?sslmode=disable"),
		HardMinSpread: hardSpread,
		HardMinVolume: hardVol,
		AdminChatIDs:  adminIDs,
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
