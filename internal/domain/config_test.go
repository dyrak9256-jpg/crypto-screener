package domain

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScreenerConfig_SetUserCrossSpread_EnforcesHardLimit(t *testing.T) {
	t.Parallel()

	hardLimit := decimal.RequireFromString("0.01") // 1% hard limit

	tests := []struct {
		name           string
		inputSpread    decimal.Decimal
		expectedSpread decimal.Decimal
	}{
		{
			name:           "spread below hard limit is clamped to hard limit",
			inputSpread:    decimal.RequireFromString("0.005"), // 0.5% < 1.0%
			expectedSpread: hardLimit,
		},
		{
			name:           "spread much lower than hard limit is clamped",
			inputSpread:    decimal.RequireFromString("0.0001"),
			expectedSpread: hardLimit,
		},
		{
			name:           "negative spread is clamped to hard limit",
			inputSpread:    decimal.RequireFromString("-0.02"),
			expectedSpread: hardLimit,
		},
		{
			name:           "zero spread is clamped to hard limit",
			inputSpread:    decimal.Zero,
			expectedSpread: hardLimit,
		},
		{
			name:           "spread equal to hard limit is accepted",
			inputSpread:    hardLimit,
			expectedSpread: hardLimit,
		},
		{
			name:           "spread above hard limit is accepted as is",
			inputSpread:    decimal.RequireFromString("0.025"), // 2.5% > 1.0%
			expectedSpread: decimal.RequireFromString("0.025"),
		},
		{
			name:           "spread significantly above hard limit is accepted",
			inputSpread:    decimal.RequireFromString("0.10"), // 10%
			expectedSpread: decimal.RequireFromString("0.10"),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := NewScreenerConfig(hardLimit)
			require.NotNil(t, cfg)

			retVal := cfg.SetUserCrossSpread(tc.inputSpread)
			assert.True(t, retVal.Equal(tc.expectedSpread), "returned value should equal expected spread")
			assert.True(t, cfg.GetEffectiveCrossSpread().Equal(hardLimit), "global effective spread must remain the administrative floor")
		})
	}
}

func TestScreenerConfig_GetCloseThreshold(t *testing.T) {
	t.Parallel()

	hardLimit := decimal.RequireFromString("0.01")

	t.Run("calculates half of hard spread", func(t *testing.T) {
		t.Parallel()
		cfg := NewScreenerConfig(hardLimit)
		cfg.SetUserCrossSpread(decimal.RequireFromString("0.04")) // user setting does not change lifecycle threshold
		threshold := cfg.GetCloseThreshold()
		assert.True(t, threshold.Equal(decimal.RequireFromString("0.005")))
	})

	t.Run("never exceeds the open threshold", func(t *testing.T) {
		t.Parallel()
		cfg := NewScreenerConfig(decimal.RequireFromString("0.001"))
		threshold := cfg.GetCloseThreshold()
		assert.True(t, threshold.Equal(decimal.RequireFromString("0.005")))
	})
}

func TestScreenerConfig_Fees(t *testing.T) {
	t.Parallel()

	cfg := NewScreenerConfig(decimal.RequireFromString("0.01"))

	// По умолчанию — консервативные 5 б.п. на сторону.
	require.True(t, cfg.FeeFor("BINANCE").Equal(decimal.RequireFromString("0.0005")))
	// Индивидуальное значение перекрывает DEFAULT.
	cfg.SetFee("BINANCE", decimal.RequireFromString("0.0004"))
	require.True(t, cfg.FeeFor("BINANCE").Equal(decimal.RequireFromString("0.0004")))
	require.True(t, cfg.FeeFor("BYBIT").Equal(decimal.RequireFromString("0.0005")))
	// DEFAULT переопределяет встроенный ориентир для всех остальных.
	cfg.SetFee(FeeKeyDefault, decimal.RequireFromString("0.001"))
	require.True(t, cfg.FeeFor("MEXC").Equal(decimal.RequireFromString("0.001")))
	// Клампинг: отрицательные → 0, гигантские → 1%.
	cfg.SetFee("KUCOIN", decimal.RequireFromString("-1"))
	require.True(t, cfg.FeeFor("KUCOIN").IsZero())
	cfg.SetFee("KUCOIN", decimal.NewFromInt(5))
	require.True(t, cfg.FeeFor("KUCOIN").Equal(decimal.RequireFromString("0.01")))
	// FeesString — детерминированный формат (ключи отсортированы).
	require.Equal(t, "BINANCE:0.0004,DEFAULT:0.001,KUCOIN:0.01", cfg.FeesString())
}

func TestParseFees(t *testing.T) {
	t.Parallel()

	// Валидная строка с пробелами и разным регистром.
	fees, err := ParseFees("DEFAULT:0.0005, binance : 0.0004")
	require.NoError(t, err)
	require.Len(t, fees, 2)
	require.True(t, fees["BINANCE"].Equal(decimal.RequireFromString("0.0004")))
	// Пустая строка/пустые части — пустая карта без ошибки.
	fees, err = ParseFees("  ,")
	require.NoError(t, err)
	require.Empty(t, fees)
	// Ошибки формата.
	for _, bad := range []string{"BINANCE", "BINANCE:abc", ":0.0004", "BINANCE:-1", "BINANCE:0.5", "BINANCE:"} {
		_, err = ParseFees(bad)
		require.Error(t, err, "expected error for %q", bad)
	}
}

func TestScreenerConfig_ApplyFees(t *testing.T) {
	t.Parallel()

	cfg := NewScreenerConfig(decimal.RequireFromString("0.01"))
	require.NoError(t, cfg.ApplyFees("DEFAULT:0.0008,BINANCE:0.0002"))
	require.True(t, cfg.FeeFor("OKX").Equal(decimal.RequireFromString("0.0008")))
	require.True(t, cfg.FeeFor("BINANCE").Equal(decimal.RequireFromString("0.0002")))
	// Некорректная строка не меняет текущее состояние.
	require.Error(t, cfg.ApplyFees("OOPS"))
	require.True(t, cfg.FeeFor("OKX").Equal(decimal.RequireFromString("0.0008")))
}
