package config

import (
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	// TelegramToken — единственный токен бота (режим по умолчанию).
	TelegramToken string
	// TelegramTokens — альтернативный мульти-токенный режим: каждый токен
	// обслуживает своих пользователей (TELEGRAM_TOKENS через запятую).
	TelegramTokens []string
	DatabaseURL    string
	HardMinSpread  decimal.Decimal
	AdminChatIDs   []int64
	// Fees — карта taker-комиссий в формате "БИРЖА:ДОЛЯ" (FEES).
	Fees string
	// MetricsAddr — адрес HTTP-сервера метрик/статуса (METRICS_ADDR).
	MetricsAddr string
	// LogFormat — формат логов: "text" (по умолчанию) или "json" (LOG_FORMAT).
	LogFormat string
	// LogLevel — verbosity логов: debug|info|warn|error (LOG_LEVEL).
	LogLevel string
}

func Load() (*Config, error) {
	hardSpread, err := parseDecimalEnvWithDefault("HARD_MIN_SPREAD", "0.01")
	if err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	db := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if db == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	var admins []int64
	if raw := strings.TrimSpace(os.Getenv("ADMIN_CHAT_IDS")); raw != "" {
		for _, p := range strings.Split(raw, ",") {
			id, e := strconv.ParseInt(strings.TrimSpace(p), 10, 64)
			if e != nil {
				return nil, fmt.Errorf("invalid ADMIN_CHAT_IDS value %q: %w", p, e)
			}
			admins = append(admins, id)
		}
	}
	tokens := parseTokens(os.Getenv("TELEGRAM_TOKENS"), os.Getenv("TELEGRAM_TOKEN"))
	fees := strings.ToUpper(strings.TrimSpace(os.Getenv("FEES")))
	if fees != "" {
		// Ранняя валидация формата, чтобы опечатка в FEES не осталась
		// незамеченной до первого сигнала.
		if err := validateFees(fees); err != nil {
			return nil, err
		}
	}
	return &Config{
		TelegramToken:  strings.TrimSpace(os.Getenv("TELEGRAM_TOKEN")),
		TelegramTokens: tokens,
		DatabaseURL:    db,
		HardMinSpread:  hardSpread,
		AdminChatIDs:   admins,
		Fees:           fees,
		MetricsAddr:    strings.TrimSpace(os.Getenv("METRICS_ADDR")),
		LogFormat:      normalizeLogFormat(os.Getenv("LOG_FORMAT")),
		LogLevel:       normalizeLogLevel(os.Getenv("LOG_LEVEL")),
	}, nil
}

// parseTokens собирает список токенов: приоритет у TELEGRAM_TOKENS (через
// запятую), одиночный TELEGRAM_TOKEN используется как запасной вариант.
func parseTokens(rawList, single string) []string {
	var out []string
	if raw := strings.TrimSpace(rawList); raw != "" {
		for _, p := range strings.Split(raw, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		return out
	}
	if single = strings.TrimSpace(single); single != "" {
		return []string{single}
	}
	return nil
}

func validateFees(raw string) error {
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid FEES value %q: expected EXCHANGE:RATE pairs, e.g. DEFAULT:0.0005,BINANCE:0.0004", part)
		}
		if strings.TrimSpace(kv[0]) == "" {
			return fmt.Errorf("invalid FEES value %q: exchange name is empty", part)
		}
		v, err := decimal.NewFromString(strings.TrimSpace(kv[1]))
		if err != nil || v.IsNegative() || v.GreaterThan(decimal.RequireFromString("0.01")) {
			return fmt.Errorf("invalid FEES value %q: rate must be a decimal between 0 and 0.01", part)
		}
	}
	return nil
}

func normalizeLogFormat(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "json":
		return "json"
	default:
		return "text"
	}
}

func normalizeLogLevel(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return "debug"
	case "warn", "warning":
		return "warn"
	case "error":
		return "error"
	default:
		return "info"
	}
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
