package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServer_HealthzAndStatus(t *testing.T) {
	s := NewServer(":0", func() any {
		return map[string]any{"active_signals": 3}
	})

	rr := httptest.NewRecorder()
	s.handleHealthz(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	require.Contains(t, rr.Header().Get("Content-Type"), "application/json")
	require.Contains(t, rr.Body.String(), `"status":"ok"`)
	require.Contains(t, rr.Body.String(), "uptime")

	rr = httptest.NewRecorder()
	s.handleStatus(rr, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	require.Contains(t, rr.Body.String(), `"active_signals":3`)
	require.Contains(t, rr.Body.String(), "queues")
	require.Contains(t, rr.Body.String(), "goroutines")
}

func TestWatchQueue_Depths(t *testing.T) {
	ch := make(chan int, 7)
	for i := 0; i < 7; i++ {
		ch <- i
	}
	WatchQueue("test_queue", func() int { return len(ch) })
	require.Equal(t, 7, QueueDepths()["test_queue"])
}

func TestMetricsEndpoint_ContainsCoreSeries(t *testing.T) {
	// /metrics должен отдавать все базовые серии: счётчики и очереди.
	// CounterVec с метками отдаёт серию только после первого инкремента.
	Tick("TESTEX")
	SignalOpened()
	SignalClosed()
	TelegramSent()
	TelegramDropped()
	DBError()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	// Мультиплексор сервера строится в NewServer; проверяем хендлер напрямую.
	s := NewServer(":0", nil)
	s.srv.Handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	for _, series := range []string{
		`screener_ticks_total{exchange="TESTEX"}`,
		"screener_signals_opened_total",
		"screener_signals_closed_total",
		"screener_telegram_sent_total",
		"screener_telegram_dropped_total",
		"screener_db_errors_total",
		"screener_queue_depth",
	} {
		require.Contains(t, body, series, "metric %s missing", series)
	}
}
