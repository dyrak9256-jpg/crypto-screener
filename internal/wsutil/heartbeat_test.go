package wsutil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestStartHeartbeatUsesControlPing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetPingHandler(func(string) error { return conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(time.Second)) })
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	url := "ws" + server.URL[len("http"):]
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	require.NoError(t, err)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, StartHeartbeat(ctx, conn, 5*time.Millisecond, time.Second))

	// If the implementation used WriteMessage from a second goroutine this
	// test would be especially prone to exposing the concurrent-writer race
	// under `go test -race` when combined with a data writer.
	for i := 0; i < 10; i++ {
		require.NoError(t, conn.WriteJSON(map[string]string{"type": "test"}))
		time.Sleep(time.Millisecond)
	}
}
