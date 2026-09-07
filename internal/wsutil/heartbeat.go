package wsutil

import (
	"context"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

// StartHeartbeat installs a read deadline and sends protocol-level Ping control
// frames. WriteControl is safe to call concurrently with the single data
// writer used by an adapter, unlike WriteMessage/WriteJSON. This is important
// because adapters may have their own application-level keepalive messages.
func StartHeartbeat(ctx context.Context, conn *websocket.Conn, interval, readTimeout time.Duration) error {
	if conn == nil {
		return fmt.Errorf("websocket connection is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if interval <= 0 {
		interval = 20 * time.Second
	}
	if readTimeout <= 0 {
		readTimeout = 60 * time.Second
	}
	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return fmt.Errorf("set websocket read deadline: %w", err)
	}
	if err := conn.SetPongHandler(func(string) error {
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return fmt.Errorf("refresh websocket read deadline after pong: %w", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("set websocket pong handler: %w", err)
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				deadline := time.Now().Add(5 * time.Second)
				if err := conn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
					// A failed control write means the connection is no longer usable.
					// Close it here so the owning read loop wakes immediately instead of
					// waiting for the read deadline. The caller's connection context will
					// stop this goroutine when the read loop returns.
					_ = conn.Close()
					return
				}
			}
		}
	}()
	return nil
}

func TouchReadDeadline(conn *websocket.Conn, timeout time.Duration) error {
	if conn == nil {
		return fmt.Errorf("websocket connection is nil")
	}
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("refresh websocket read deadline: %w", err)
	}
	return nil
}
