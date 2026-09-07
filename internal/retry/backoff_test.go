package retry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackoffHonorsCancellation(t *testing.T) {
	b := New(time.Second, 2*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, b.Wait(ctx.Done()))
}

func TestBackoffResetRestartsAttemptSequence(t *testing.T) {
	b := New(time.Millisecond, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.True(t, b.Wait(ctx.Done()))
	require.True(t, b.Wait(ctx.Done()))
	b.Reset()
	require.Equal(t, uint32(0), b.attempt.Load())
}
