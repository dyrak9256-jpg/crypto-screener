package observability

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Вебхук Alertmanager: POST /alerts парсит уведомления и передаёт их
// обработчику; не-POST и битый JSON отклоняются.
func TestServer_AlertsWebhook(t *testing.T) {
	s := NewServer(":0", nil)

	var (
		mu  sync.Mutex
		got []Alert
	)
	s.SetAlertsHandler(func(a Alert) {
		mu.Lock()
		got = append(got, a)
		mu.Unlock()
	})

	body := `{"status":"firing","alerts":[
		{"status":"firing","labels":{"alertname":"ExchangeSilent","exchange":"BITGET","severity":"warning"},
		 "annotations":{"summary":"Биржа BITGET молчит","description":"Ноль тиков за 5 минут."},"startsAt":"2026-09-08T18:00:00Z"},
		{"status":"resolved","labels":{"alertname":"DbErrors"},"annotations":{}}
	]}`
	req := httptest.NewRequest(http.MethodPost, "/alerts", bytes.NewReader([]byte(body)))
	rec := httptest.NewRecorder()
	s.handleAlerts(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	mu.Lock()
	require.Len(t, got, 1)
	require.Len(t, got[0].Alerts, 2)
	require.Equal(t, "ExchangeSilent", got[0].Alerts[0].Labels["alertname"])
	require.Equal(t, "Биржа BITGET молчит", got[0].Alerts[0].Annotations["summary"])
	require.Equal(t, "resolved", got[0].Alerts[1].Status)
	mu.Unlock()

	// Битый JSON → 400.
	req = httptest.NewRequest(http.MethodPost, "/alerts", bytes.NewReader([]byte("{oops")))
	rec = httptest.NewRecorder()
	s.handleAlerts(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	// GET → 405.
	req = httptest.NewRequest(http.MethodGet, "/alerts", nil)
	rec = httptest.NewRecorder()
	s.handleAlerts(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	require.NoError(t, s.srv.Shutdown(context.Background()))
}
