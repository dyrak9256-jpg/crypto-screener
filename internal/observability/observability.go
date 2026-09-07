// Package observability — метрики, health-эндпоинты и pprof скринера.
//
// Экспонирует Prometheus-метрики (собственный реестр, не глобальный):
//
//	screener_ticks_total{exchange}        — обработанных тиков по биржам
//	screener_signals_opened_total         — открытых сигналов
//	screener_signals_closed_total         — закрытых сигналов
//	screener_telegram_sent_total          — доставленных Telegram-уведомлений
//	screener_telegram_dropped_total       — потерянных уведомлений (переполнение/ошибка)
//	screener_db_errors_total              — ошибок записи в БД
//	screener_queue_depth{queue}           — текущая глубина внутренних каналов
//	screener_active_signals               — активных сигналов (из Tracker)
//	screener_connected_exchanges          — подключённых бирж (из ConnectorManager)
//	screener_funding_age_seconds{exchange}— возраст последних funding-данных
//
// HTTP-сервер (METRICS_ADDR, по умолчанию :9090) отдаёт:
//
//	/metrics    — Prometheus scrape
//	/healthz    — liveness-проба (JSON)
//	/api/status — расширенный статус: uptime, очереди, рантайм, снапшот приложения
//	/debug/pprof/... — профилирование Go
package observability

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // регистрирует pprof-хендлеры в DefaultServeMux
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	registry = prometheus.NewRegistry()

	startTime = time.Now()

	ticksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "screener_ticks_total",
			Help: "Market ticks processed, by exchange.",
		},
		[]string{"exchange"},
	)
	signalsOpenedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "screener_signals_opened_total",
		Help: "Arbitrage signals opened.",
	})
	signalsClosedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "screener_signals_closed_total",
		Help: "Arbitrage signals closed.",
	})
	telegramSentTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "screener_telegram_sent_total",
		Help: "Telegram notifications successfully delivered.",
	})
	telegramDroppedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "screener_telegram_dropped_total",
		Help: "Telegram notifications dropped (overflow or send error).",
	})
	dbErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "screener_db_errors_total",
		Help: "Database write errors.",
	})
)

// queueCollector собирает глубину внутренних каналов в момент scrape —
// значения читаются колбэками, зарегистрированными через WatchQueue.
type queueCollector struct {
	mx     sync.RWMutex
	depths map[string]func() int
}

var queues = &queueCollector{depths: make(map[string]func() int)}

var queueDesc = prometheus.NewDesc("screener_queue_depth", "Current depth of internal channels.", []string{"queue"}, nil)

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	c.mx.RLock()
	defer c.mx.RUnlock()
	for name, fn := range c.depths {
		ch <- prometheus.MustNewConstMetric(queueDesc, prometheus.GaugeValue, float64(fn()), name)
	}
}

func init() {
	registry.MustRegister(
		ticksTotal,
		signalsOpenedTotal,
		signalsClosedTotal,
		telegramSentTotal,
		telegramDroppedTotal,
		dbErrorsTotal,
		queues,
	)
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())
}

// WatchQueue регистрирует функцию текущей глубины канала под именем очереди.
func WatchQueue(name string, depth func() int) {
	queues.mx.Lock()
	defer queues.mx.Unlock()
	queues.depths[name] = depth
}

// QueueDepths возвращает срез глубин всех очередей (для /api/status).
func QueueDepths() map[string]int {
	queues.mx.RLock()
	defer queues.mx.RUnlock()
	out := make(map[string]int, len(queues.depths))
	for name, fn := range queues.depths {
		out[name] = fn()
	}
	return out
}

// RegisterGaugeFunc добавляет кастомную метрику-функцию.
// Повторная регистрация того же имени игнорируется.
func RegisterGaugeFunc(name, help string, fn func() float64) {
	registry.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, fn))
}

// RegisterExchangeGaugeFunc добавляет метрику-функцию с константной меткой биржи.
func RegisterExchangeGaugeFunc(name, help, exchange string, fn func() float64) {
	registry.Register(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: prometheus.Labels{"exchange": exchange}},
		fn,
	))
}

// Tick инкрементирует счётчик обработанных тиков биржи.
func Tick(exchange string) { ticksTotal.WithLabelValues(exchange).Inc() }

// SignalOpened/SignalClosed — счётчики открытия/закрытия сигналов.
func SignalOpened() { signalsOpenedTotal.Inc() }
func SignalClosed() { signalsClosedTotal.Inc() }

// TelegramSent/TelegramDropped — доставка/потери уведомлений.
func TelegramSent()    { telegramSentTotal.Inc() }
func TelegramDropped() { telegramDroppedTotal.Inc() }

// DBError инкрементирует счётчик ошибок записи в БД.
func DBError() { dbErrorsTotal.Inc() }

// Server — HTTP-сервер метрик и статуса.
type Server struct {
	srv           *http.Server
	snapshot      func() any
	alertsMu      sync.RWMutex
	alertsHandler func(Alert)
}

// NewServer создаёт сервер; snapshot (может быть nil) возвращает map с
// приложенными данными для /api/status (активные сигналы, биржи и т.п.).
func NewServer(addr string, snapshot func() any) *Server {
	s := &Server{snapshot: snapshot}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/alerts", s.handleAlerts)
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	s.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

// Alert — одно уведомление из Alertmanager webhook.
type Alert struct {
	Status string            `json:"status"`
	Alerts []AlertmanagerMsg `json:"alerts"`
}

type AlertmanagerMsg struct {
	Status      string            `json:"status"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    string            `json:"startsAt"`
}

// SetAlertsHandler подключает обработчик уведомлений Alertmanager
// (например, пересылку в Telegram администраторам). Обработчик обязан
// возвращаться быстро — доставка идёт в фоне на стороне приложения.
func (s *Server) SetAlertsHandler(fn func(Alert)) {
	s.alertsMu.Lock()
	s.alertsHandler = fn
	s.alertsMu.Unlock()
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var alert Alert
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&alert); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]any{"status": "ok", "received": len(alert.Alerts)})
	s.alertsMu.RLock()
	fn := s.alertsHandler
	s.alertsMu.RUnlock()
	if fn != nil && len(alert.Alerts) > 0 {
		fn(alert)
	}
}

// Run блокирует до отмены контекста, затем корректно завершает сервер.
// Вызывается в горутине main.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"status":     "ok",
		"uptime":     time.Since(startTime).Round(time.Second).String(),
		"goroutines": runtime.NumGoroutine(),
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	info := map[string]any{
		"status":           "ok",
		"uptime":           time.Since(startTime).Round(time.Second).String(),
		"goroutines":       runtime.NumGoroutine(),
		"heap_alloc_bytes": ms.HeapAlloc,
		"num_gc":           ms.NumGC,
		"queues":           QueueDepths(),
	}
	if s.snapshot != nil {
		if m, ok := s.snapshot().(map[string]any); ok {
			for k, v := range m {
				info[k] = v
			}
		}
	}
	writeJSON(w, info)
}

func writeJSON(w http.ResponseWriter, payload map[string]any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("observability: encode status json", "error", err)
	}
}

// FormatUptime — строковое представление аптайма для переиспользования.
func FormatUptime() string { return time.Since(startTime).Round(time.Second).String() }
