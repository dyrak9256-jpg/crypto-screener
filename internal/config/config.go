package config

import (
	"errors"
	"fmt"
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
	AdminChatIDs  []int64
}

func Load() (*Config, error) {
	hardSpread, err := parseDecimalEnvWithDefault("HARD_MIN_SPREAD", "0.01")
	if err != nil {
		return nil, err
	}
	hardVol, err := parseDecimalEnvWithDefault("HARD_MIN_VOLUME", "1000000")
	if err != nil {
		return nil, err
	}
	db := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if db == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	var admins []int64
	if raw := strings.TrimSpace(os.Getenv("ADMIN_CHAT_IDS")); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			id, e := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
			if e != nil {
				return nil, fmt.Errorf("invalid ADMIN_CHAT_IDS value %q: %w", part, e)
			}
			admins = append(admins, id)
		}
	}
	return &Config{TelegramToken: strings.TrimSpace(os.Getenv("TELEGRAM_TOKEN")), DatabaseURL: db, HardMinSpread: hardSpread, HardMinVolume: hardVol, AdminChatIDs: admins}, nil
}
func parseDecimalEnvWithDefault(key, fallback string) (decimal.Decimal, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		raw = fallback
	}
	v, err := decimal.NewFromString(raw)
	if err != nil {
		return decimal.Zero, fmt.Errorf("invalid %s: %w", key, err)
	}
	if v.IsNegative() {
		return decimal.Zero, fmt.Errorf("%s cannot be negative", key)
	}
	if key == "HARD_MIN_SPREAD" && v.LessThan(decimal.RequireFromString("0.01")) {
		return decimal.Zero, fmt.Errorf("%s must be at least 0.01 (1%%)", key)
	}
	return v, nil
}
