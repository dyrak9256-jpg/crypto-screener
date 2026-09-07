// Package symbolscache хранит списки символов бирж на диске, чтобы коннекторы
// переживали недоступность REST-эндпоинтов загрузки символов (гео-блоки,
// смена формата, аварии биржи). Каталог задаётся SYMBOLS_CACHE_DIR
// (по умолчанию ./symbols-cache); файлы — обычный JSON-массив строк.
package symbolscache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var mu sync.Mutex

// cacheDir читается при каждом обращении (os.Getenv дёшев, а операции кэша
// редки) — так t.Setenv и смена окружения на лету работают корректно.
func cacheDir() string {
	if v := strings.TrimSpace(os.Getenv("SYMBOLS_CACHE_DIR")); v != "" {
		return v
	}
	return filepath.Join(".", "symbols-cache")
}

// Dir возвращает текущий каталог кэша (для статуса/диагностики).
func Dir() string {
	mu.Lock()
	defer mu.Unlock()
	return cacheDir()
}

func pathFor(name string) string {
	return filepath.Join(cacheDir(), fmt.Sprintf("%s.json", strings.ToLower(strings.TrimSpace(name))))
}

// Save атомарно (tmp+rename) записывает список символов под именем
// вида "bybit-spot", "binance-futures".
func Save(name string, symbols []string) error {
	if len(symbols) == 0 {
		return fmt.Errorf("symbolscache: пустой список для %q — не перезаписываем кэш", name)
	}
	mu.Lock()
	defer mu.Unlock()
	if err := os.MkdirAll(cacheDir(), 0o755); err != nil {
		return fmt.Errorf("symbolscache: %w", err)
	}
	data, err := json.Marshal(symbols)
	if err != nil {
		return fmt.Errorf("symbolscache: %w", err)
	}
	target := pathFor(name)
	tmp := target + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("symbolscache: %w", err)
	}
	if err := os.Rename(tmp, target); err != nil {
		return fmt.Errorf("symbolscache: %w", err)
	}
	return nil
}

// Load читает список символов; ошибка — если файла нет или он битый.
func Load(name string) ([]string, error) {
	mu.Lock()
	defer mu.Unlock()
	data, err := os.ReadFile(pathFor(name))
	if err != nil {
		return nil, fmt.Errorf("symbolscache load %s: %w", name, err)
	}
	var symbols []string
	if err := json.Unmarshal(data, &symbols); err != nil {
		return nil, fmt.Errorf("symbolscache load %s: %w", name, err)
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("symbolscache load %s: файл пуст", name)
	}
	return symbols, nil
}
