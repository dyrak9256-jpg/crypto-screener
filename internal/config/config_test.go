package config

import (
	"os"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Load_Defaults(t *testing.T) {
	// Clear any active env vars for the duration of this test
	os.Unsetenv("TELEGRAM_TOKEN")
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("HARD_MIN_SPREAD")
	os.Unsetenv("HARD_MIN_VOLUME")

	cfg := Load()
	require.NotNil(t, cfg)

	assert.Equal(t, "", cfg.TelegramToken)
	assert.Equal(t, "postgres://user:pass@localhost:5432/screener?sslmode=disable", cfg.DatabaseURL)
	assert.True(t, cfg.HardMinSpread.Equal(decimal.RequireFromString("0.01")))
	assert.True(t, cfg.HardMinVolume.Equal(decimal.RequireFromString("1000000")))
}

func TestConfig_Load_CustomEnv(t *testing.T) {
	t.Setenv("TELEGRAM_TOKEN", "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11")
	t.Setenv("DATABASE_URL", "postgres://admin:secret@pg.internal:5432/arbitrage?sslmode=require")
	t.Setenv("HARD_MIN_SPREAD", "0.025")
	t.Setenv("HARD_MIN_VOLUME", "2500000")

	cfg := Load()
	require.NotNil(t, cfg)

	assert.Equal(t, "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", cfg.TelegramToken)
	assert.Equal(t, "postgres://admin:secret@pg.internal:5432/arbitrage?sslmode=require", cfg.DatabaseURL)
	assert.True(t, cfg.HardMinSpread.Equal(decimal.RequireFromString("0.025")))
	assert.True(t, cfg.HardMinVolume.Equal(decimal.RequireFromString("2500000")))
}

func TestGetEnv(t *testing.T) {
	t.Run("returns fallback when key is not set", func(t *testing.T) {
		val := getEnv("NON_EXISTENT_KEY_XYZ_123", "default_val")
		assert.Equal(t, "default_val", val)
	})

	t.Run("returns env var when key is set", func(t *testing.T) {
		t.Setenv("TEST_KEY_EXISTS", "custom_val")
		val := getEnv("TEST_KEY_EXISTS", "default_val")
		assert.Equal(t, "custom_val", val)
	})
}

func TestConfig_Load_InvalidSpreadFallsBackToDefault(t *testing.T) {
	// Некорректное значение HARD_MIN_SPREAD не должно обнулять порог (это
	// отключило бы фильтрацию сигналов) — фолбэк на дефолт 0.01.
	t.Setenv("HARD_MIN_SPREAD", "not-a-number")
	t.Setenv("HARD_MIN_VOLUME", "not-a-number")

	cfg := Load()
	assert.True(t, cfg.HardMinSpread.Equal(decimal.RequireFromString("0.01")),
		"invalid spread must fall back to 0.01")
	assert.True(t, cfg.HardMinVolume.Equal(decimal.RequireFromString("1000000")),
		"invalid volume must fall back to 1000000")
}
