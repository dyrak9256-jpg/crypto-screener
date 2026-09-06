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
	t.Setenv("HARD_MIN_VOLUME", "")
	t.Setenv("ADMIN_CHAT_IDS", "")
	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "", cfg.TelegramToken)
	require.Equal(t, "postgres://user:pass@localhost:5432/screener?sslmode=disable", cfg.DatabaseURL)
	require.True(t, cfg.HardMinSpread.Equal(decimal.RequireFromString("0.01")))
	require.True(t, cfg.HardMinVolume.Equal(decimal.RequireFromString("1000000")))
}

func TestConfig_Load_CustomEnv(t *testing.T) {
	t.Setenv("TELEGRAM_TOKEN", "token")
	t.Setenv("DATABASE_URL", "postgres://admin:secret@pg.internal:5432/arbitrage?sslmode=require")
	t.Setenv("HARD_MIN_SPREAD", "0.025")
	t.Setenv("HARD_MIN_VOLUME", "2500000")
	t.Setenv("ADMIN_CHAT_IDS", "1, 2")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "token", cfg.TelegramToken)
	require.True(t, cfg.HardMinSpread.Equal(decimal.RequireFromString("0.025")))
	require.True(t, cfg.HardMinVolume.Equal(decimal.RequireFromString("2500000")))
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
