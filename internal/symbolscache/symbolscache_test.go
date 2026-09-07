package symbolscache

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSymbolscache_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SYMBOLS_CACHE_DIR", dir)
	// dir вычисляется лениво при первом обращении — после Setenv.
	require.Contains(t, Dir(), filepath.Base(dir))

	// Load до Save — понятная ошибка.
	_, err := Load("bybit-spot")
	require.Error(t, err)

	require.NoError(t, Save("bybit-spot", []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}))
	loaded, err := Load("bybit-spot")
	require.NoError(t, err)
	require.Equal(t, []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}, loaded)

	// Пустой список не перезаписывает существующий кэш.
	require.Error(t, Save("bybit-spot", nil))
	loaded, err = Load("bybit-spot")
	require.NoError(t, err)
	require.Len(t, loaded, 3)

	// Битый файл — ошибка, а не паника.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.json"), []byte("{oops"), 0o644))
	_, err = Load("bad")
	require.Error(t, err)
}
