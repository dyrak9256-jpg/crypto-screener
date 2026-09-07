package config

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestConfig_Load_Defaults(t *testing.T) {
	t.Setenv("TELEGRAM_TOKEN", "")
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/screener?sslmode=disable")
	t.Setenv("HARD_MIN_SPREAD", "")
	t.Setenv("ADMIN_CHAT_IDS", "")
	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "", cfg.TelegramToken)
	require.Equal(t, "postgres://user:pass@localhost:5432/screener?sslmode=disable", cfg.DatabaseURL)
	require.True(t, cfg.HardMinSpread.Equal(decimal.RequireFromString("0.01")))
}

func TestConfig_Load_CustomEnv(t *testing.T) {
	t.Setenv("TELEGRAM_TOKEN", "token")
	t.Setenv("DATABASE_URL", "postgres://admin:secret@pg.internal:5432/arbitrage?sslmode=require")
	t.Setenv("HARD_MIN_SPREAD", "0.025")
	t.Setenv("ADMIN_CHAT_IDS", "1, 2")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "token", cfg.TelegramToken)
	require.True(t, cfg.HardMinSpread.Equal(decimal.RequireFromString("0.025")))
	require.Equal(t, []int64{1, 2}, cfg.AdminChatIDs)
}

func TestConfig_Load_InvalidNumericFails(t *testing.T) {
	t.Setenv("HARD_MIN_SPREAD", "not-a-number")
	_, err := Load()
	require.Error(t, err)
}

func TestConfig_Load_MissingDatabaseURLFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	_, err := Load()
	require.Error(t, err)
}

func TestConfig_Load_TelegramTokensMultiMode(t *testing.T) {
	t.Setenv("TELEGRAM_TOKEN", "")
	t.Setenv("TELEGRAM_TOKENS", "tok1, tok2 , tok3")
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/screener")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, []string{"tok1", "tok2", "tok3"}, cfg.TelegramTokens)
	// Одиночный TELEGRAM_TOKEN не теряется как запасной вариант.
}

func TestConfig_Load_SingleTokenFallback(t *testing.T) {
	t.Setenv("TELEGRAM_TOKENS", "")
	t.Setenv("TELEGRAM_TOKEN", "single")
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/screener")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, []string{"single"}, cfg.TelegramTokens)
}

func TestConfig_Load_Fees(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/screener")
	t.Setenv("FEES", "DEFAULT:0.0005,binance:0.0004")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "DEFAULT:0.0005,BINANCE:0.0004", cfg.Fees)

	t.Setenv("FEES", "BAD")
	_, err = Load()
	require.Error(t, err)

	t.Setenv("FEES", "BINANCE:9")
	_, err = Load()
	require.Error(t, err)
}

func TestConfig_Load_LogFormatAndLevel(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/screener")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "text", cfg.LogFormat)
	require.Equal(t, "info", cfg.LogLevel)

	t.Setenv("LOG_FORMAT", "JSON")
	t.Setenv("LOG_LEVEL", "debug")
	cfg, err = Load()
	require.NoError(t, err)
	require.Equal(t, "json", cfg.LogFormat)
	require.Equal(t, "debug", cfg.LogLevel)
}
