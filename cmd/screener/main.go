package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"crypto-screener/internal/adapters/binance"
	"crypto-screener/internal/adapters/bingx"
	"crypto-screener/internal/adapters/bitget"
	"crypto-screener/internal/adapters/bybit"
	"crypto-screener/internal/adapters/gateio"
	"crypto-screener/internal/adapters/kucoin"
	"crypto-screener/internal/adapters/mexc"
	"crypto-screener/internal/adapters/okx"
	"crypto-screener/internal/adapters/postgres"
	"crypto-screener/internal/adapters/telegram"
	"crypto-screener/internal/app"
	"crypto-screener/internal/config"
	"crypto-screener/internal/domain"
	"crypto-screener/internal/observability"
)

// exchangeCheckURLs — дешёвые REST-эндпоинты на тех же хостах, что используют
// адаптеры. Стартовая проверка выявляет гео-блоки (403/451) и сетевые проблемы
// до того, как коннекторы молча уйдут в retry-циклы.
var exchangeCheckURLs = map[string]string{
	"BINANCE": "https://api.binance.com/api/v3/ping",
	"BINGX":   "https://open-api.bingx.com/openApi/spot/v1/ticker/price?symbol=BTCUSDT",
	"BITGET":  "https://api.bitget.com/api/v2/public/time",
	"BYBIT":   "https://api.bybit.com/v5/market/time",
	"GATEIO":  "https://api.gateio.ws/api/v4/spot/time",
	"KUCOIN":  "https://api.kucoin.com/api/v1/timestamp",
	"MEXC":    "https://api.mexc.com/api/v3/ping",
	"OKX":     "https://www.okx.com/api/v5/public/time",
}

func main() {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, ".env load error: %v\n", err)
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Config error: %v\n", err)
		os.Exit(1)
	}
	setupLogger(cfg.LogFormat, cfg.LogLevel)
	tokens := cfg.TelegramTokens
	if len(tokens) == 0 {
		slog.Error("Config error: TELEGRAM_TOKEN (или TELEGRAM_TOKENS) is required")
		os.Exit(1)
	}
	if len(cfg.AdminChatIDs) == 0 {
		slog.Error("Config error: ADMIN_CHAT_IDS must contain at least one administrator")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	screenerConfig := domain.NewScreenerConfig(cfg.HardMinSpread)
	if cfg.Fees != "" {
		if err := screenerConfig.ApplyFees(cfg.Fees); err != nil {
			slog.Error("Config error: invalid FEES", "error", err)
			os.Exit(1)
		}
		slog.Info("taker fees configured from env", "fees", screenerConfig.FeesString())
	}

	dbRepo, err := postgres.NewRepository(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("DB init failed", "error", err)
		os.Exit(1)
	}
	defer dbRepo.Close()

	application, err := app.NewApplication(screenerConfig, dbRepo, dbRepo)
	if err != nil {
		slog.Error("Application init failed", "error", err)
		os.Exit(1)
	}
	application.SetAdminIDs(cfg.AdminChatIDs)
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
		default:
			return nil, false
		}
	})

	// Стартовая проверка доступности бирж: гео-блоки видны сразу, а не через
	// часы молчаливых retry. Результат попадает в /api/status.
	availability := checkExchangeAvailability()

	// Мульти-токенный режим: каждый бот получает свой id (индекс токена),
	// пользователь закрепляется за ботом через который подписался.
	bots := make([]*telegram.Bot, 0, len(tokens))
	for i, token := range tokens {
		b, err := telegram.NewBot(int64(i), token, application)
		if err != nil {
			slog.Error("telegram bot init failed", "bot_id", i, "error", err)
			os.Exit(1)
		}
		bots = append(bots, b)
	}
	pool := telegram.NewBotPool(bots, application.RouteBot)
	defer pool.Close()
	application.SetTelegramSender(pool)
	slog.Info("telegram bots started", "count", len(bots))

	// HTTP-сервер метрик/статуса (Prometheus, /healthz, /api/status, pprof).
	metricsAddr := cfg.MetricsAddr
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	statusSnapshot := func() any {
		snapshot := application.StatusSnapshot()
		snapshot["exchange_availability"] = availability
		return snapshot
	}
	metricsServer := observability.NewServer(metricsAddr, statusSnapshot)
	// Вебхук Alertmanager (docker-compose сервис alertmanager) → Telegram админам.
	metricsServer.SetAlertsHandler(application.HandleAlerts)
	go func() {
		if err := metricsServer.Run(ctx); err != nil {
			slog.Error("metrics server stopped", "addr", metricsAddr, "error", err)
		}
	}()
	slog.Info("metrics/status server listening", "addr", metricsAddr, "endpoints", "/metrics /healthz /api/status /alerts /debug/pprof/")

	appErr := make(chan error, 1)
	tgDone := make(chan struct{})
	go func() { appErr <- application.Run(ctx, dbRepo) }()
	go func() {
		defer close(tgDone)
		if err := pool.StartPolling(ctx); err != nil {
			slog.Error("telegram polling failed", "error", err)
		}
	}()

	select {
	case err := <-appErr:
		if err != nil {
			slog.Error("app failed", "error", err)
			cancel()
			return
		}
	case <-tgDone:
		// Все боты остановились — без доставки уведомлений скринер не имеет
		// смысла, поэтому корректно завершаем всё приложение.
		slog.Warn("telegram polling stopped for all bots, shutting down")
		cancel()
		if err := <-appErr; err != nil {
			slog.Error("app shutdown error", "error", err)
		}
	}
}

// setupLogger настраивает slog по LOG_FORMAT/LOG_LEVEL. slog.SetDefault
// перенаправляет и классический log.Printf адаптеров в этот же хендлер.
func setupLogger(format, level string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(handler))
}

// checkExchangeAvailability параллельно пингует REST-хосты бирж и логирует
// результат. Гео-блоки (403/451) — громкая ошибка с подсказкой о регионе.
func checkExchangeAvailability() map[string]string {
	type result struct {
		name   string
		status string
	}
	results := make(chan result, len(exchangeCheckURLs))
	client := &http.Client{Timeout: 4 * time.Second}
	for name, url := range exchangeCheckURLs {
		go func(name, url string) {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
			if err != nil {
				results <- result{name, "request build error: " + err.Error()}
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				results <- result{name, "network error: " + err.Error()}
				return
			}
			defer resp.Body.Close()
			switch {
			case resp.StatusCode == 200:
				results <- result{name, "ok"}
			case resp.StatusCode == 403 || resp.StatusCode == 451:
				results <- result{name, fmt.Sprintf("HTTP %d (geo-blocked)", resp.StatusCode)}
			default:
				results <- result{name, fmt.Sprintf("HTTP %d", resp.StatusCode)}
			}
		}(name, url)
	}
	statuses := make(map[string]string, len(exchangeCheckURLs))
	for range exchangeCheckURLs {
		r := <-results
		statuses[r.name] = r.status
		switch {
		case r.status == "ok":
			slog.Info("exchange reachable", "exchange", r.name)
		case len(r.status) > 4 && r.status[:4] == "HTTP" && (r.status[len(r.status)-13:] == "(geo-blocked)"):
			slog.Error("exchange GEO-BLOCKED from this host — connector will not receive data; deploy in an allowed region or use a proxy",
				"exchange", r.name, "status", r.status)
		default:
			slog.Warn("exchange unreachable at startup", "exchange", r.name, "status", r.status)
		}
	}
	return statuses
}
