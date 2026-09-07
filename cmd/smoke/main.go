// Command smoke — натурная проверка штатного режима crypto-screener.
//
// Поднимает полный конвейер (8 биржевых коннекторов, агрегатор, tracker,
// персистентность) с реальной базой PostgreSQL и noop-доставкой Telegram,
// собирает данные SMOKE_DURATION секунд, печатает /healthz, /api/status и
// screener_*-метрики, затем корректно останавливает приложение.
//
// Использование:
//
//	DATABASE_URL=postgres://user:pass@host:5432/db?sslmode=disable \
//	SMOKE_DURATION=30s METRICS_ADDR=:9090 go run ./cmd/smoke
//
// Коды выхода: 0 — прогрев, метрики и graceful shutdown прошли; 1 — сбой.
// Это ручной инструмент эксплуатации; в CI не запускается.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"crypto-screener/internal/adapters/binance"
	"crypto-screener/internal/adapters/bingx"
	"crypto-screener/internal/adapters/bitget"
	"crypto-screener/internal/adapters/bybit"
	"crypto-screener/internal/adapters/gateio"
	"crypto-screener/internal/adapters/kucoin"
	"crypto-screener/internal/adapters/mexc"
	"crypto-screener/internal/adapters/okx"
	"crypto-screener/internal/adapters/postgres"
	"crypto-screener/internal/app"
	"crypto-screener/internal/domain"
	"crypto-screener/internal/observability"
	"github.com/shopspring/decimal"
)

// noopSender заменяет Telegram: доставка всегда "успешна" и мгновенна.
type noopSender struct{}

func (noopSender) SendPrivateMessage(int64, string) {}
func (noopSender) Broadcast(string, []int64)        {}
func (noopSender) Close()                           {}

func httpGet(url string) string {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func filterScreenerMetrics(body string) string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "screener_") {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dbURL == "" {
		slog.Error("smoke: DATABASE_URL обязателен")
		os.Exit(1)
	}
	duration := 30 * time.Second
	if raw := strings.TrimSpace(os.Getenv("SMOKE_DURATION")); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			duration = d
		}
	}
	metricsAddr := strings.TrimSpace(os.Getenv("METRICS_ADDR"))
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	repo, err := postgres.NewRepository(ctx, dbURL)
	if err != nil {
		slog.Error("smoke: init postgres", "error", err)
		os.Exit(1)
	}
	defer repo.Close()
	slog.Info("smoke: postgres ok, схема применена")

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"))
	application, err := app.NewApplication(cfg, repo, repo)
	if err != nil {
		slog.Error("smoke: init application", "error", err)
		os.Exit(1)
	}
	application.SetAdminIDs([]int64{0})
	application.SetTelegramSender(noopSender{})
	application.SetConnectorFactory(func(name string) (domain.ExchangeConnector, bool) {
		switch name {
		case "BINANCE":
			return binance.NewAdapter(), true
		case "BINGX":
			return bingx.NewAdapter(), true
		case "BITGET":
			return bitget.NewAdapter(), true
		case "BYBIT":
			return bybit.NewAdapter(), true
		case "GATEIO":
			return gateio.NewAdapter(), true
		case "KUCOIN":
			return kucoin.NewAdapter(), true
		case "MEXC":
			return mexc.NewAdapter(), true
		case "OKX":
			return okx.NewAdapter(), true
		}
		return nil, false
	})

	srv := observability.NewServer(metricsAddr, func() any { return application.StatusSnapshot() })
	go func() {
		if err := srv.Run(ctx); err != nil {
			slog.Error("smoke: metrics server", "error", err)
		}
	}()

	runErr := make(chan error, 1)
	go func() { runErr <- application.Run(ctx, repo) }()

	slog.Info("smoke: конвейер запущен, сбор данных", "duration", duration.String())
	select {
	case <-time.After(duration):
	case err := <-runErr:
		slog.Error("smoke: приложение упало во время прогрева", "error", err)
		os.Exit(1)
	}

	fmt.Println("\n========== /healthz ==========")
	fmt.Println(strings.TrimSpace(httpGet("http://127.0.0.1:9090/healthz")))
	fmt.Println("\n========== /api/status ==========")
	fmt.Println(strings.TrimSpace(httpGet("http://127.0.0.1:9090/api/status")))
	fmt.Println("\n========== /metrics (screener_*) ==========")
	fmt.Println(filterScreenerMetrics(httpGet("http://127.0.0.1:9090/metrics")))

	slog.Info("smoke: graceful shutdown...")
	stop()
	select {
	case err := <-runErr:
		if err != nil {
			slog.Error("smoke: Run завершился с ошибкой", "error", err)
			os.Exit(1)
		}
		slog.Info("smoke: ✅ штатный режим подтверждён (прогрев, метрики, shutdown)")
	case <-time.After(90 * time.Second):
		slog.Error("smoke: ❌ shutdown не завершился за 90с")
		os.Exit(1)
	}
}
