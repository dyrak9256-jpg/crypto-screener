package postgres

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"crypto-screener/internal/domain"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	pgmodule "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestPostgres(t *testing.T) (*Repository, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schemaPath, err := filepath.Abs(filepath.Join("..", "..", "..", "schema.sql"))
	require.NoError(t, err)

	if _, err := os.Stat(schemaPath); err != nil {
		t.Fatalf("schema.sql not found at %s: %v", schemaPath, err)
	}

	pgContainer, err := pgmodule.Run(ctx,
		"postgres:16-alpine",
		pgmodule.WithInitScripts(schemaPath),
		pgmodule.WithDatabase("screener_test"),
		pgmodule.WithUsername("testuser"),
		pgmodule.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Skipf("Skipping Postgres integration test: Docker daemon is unavailable or failed to start container: %v", err)
		return nil, func() {}
	}

	connStr, err := pgContainer.ConnectionString(context.Background(), "sslmode=disable")
	if err != nil {
		_ = pgContainer.Terminate(context.Background())
		t.Fatalf("failed to obtain container connection string: %v", err)
	}

	repo, err := NewRepository(context.Background(), connStr)
	if err != nil {
		_ = pgContainer.Terminate(context.Background())
		t.Fatalf("failed to create postgres repository: %v", err)
	}

	cleanup := func() {
		repo.Close()
		_ = pgContainer.Terminate(context.Background())
	}

	return repo, cleanup
}

func TestPostgresRepository_Users(t *testing.T) {
	repo, cleanup := setupTestPostgres(t)
	if repo == nil {
		return // Skipped due to Docker unavailability
	}
	defer cleanup()

	ctx := context.Background()

	// 1. Initial State: No users
	initialUsers, err := repo.GetAllUsers(ctx)
	require.NoError(t, err)
	assert.Empty(t, initialUsers)

	// 2. SaveUser (Create new users)
	user1 := &domain.User{
		ChatID:    1001,
		Username:  "alice",
		MinSpread: decimal.RequireFromString("0.015"),
		MinVolume: decimal.RequireFromString("500000"),
		Timeframe: domain.TF_15m,
	}
	user2 := &domain.User{
		ChatID:    1002,
		Username:  "bob",
		MinSpread: decimal.RequireFromString("0.020"),
		MinVolume: decimal.RequireFromString("1000000"),
		Timeframe: domain.TF_1h,
	}

	err = repo.SaveUser(ctx, user1)
	require.NoError(t, err)
	err = repo.SaveUser(ctx, user2)
	require.NoError(t, err)

	// 3. GetAllUsers
	users, err := repo.GetAllUsers(ctx)
	require.NoError(t, err)
	require.Len(t, users, 2)

	userMap := make(map[int64]*domain.User)
	for _, u := range users {
		userMap[u.ChatID] = u
	}

	assert.Contains(t, userMap, int64(1001))
	assert.Equal(t, "alice", userMap[1001].Username)
	assert.True(t, userMap[1001].MinSpread.Equal(decimal.RequireFromString("0.015")))
	assert.True(t, userMap[1001].MinVolume.Equal(decimal.RequireFromString("500000")))
	assert.Equal(t, domain.TF_15m, userMap[1001].Timeframe)

	assert.Contains(t, userMap, int64(1002))
	assert.Equal(t, "bob", userMap[1002].Username)
	assert.True(t, userMap[1002].MinSpread.Equal(decimal.RequireFromString("0.020")))
	assert.True(t, userMap[1002].MinVolume.Equal(decimal.RequireFromString("1000000")))
	assert.Equal(t, domain.TF_1h, userMap[1002].Timeframe)

	// 4. SaveUser UPSERT (Update existing user)
	updatedUser1 := &domain.User{
		ChatID:    1001,
		Username:  "alice_pro",
		MinSpread: decimal.RequireFromString("0.030"),
		MinVolume: decimal.RequireFromString("750000"),
		Timeframe: domain.TF_4h,
	}
	err = repo.SaveUser(ctx, updatedUser1)
	require.NoError(t, err)

	usersAfterUpsert, err := repo.GetAllUsers(ctx)
	require.NoError(t, err)
	require.Len(t, usersAfterUpsert, 2, "total count should still be 2 after upsert")

	for _, u := range usersAfterUpsert {
		if u.ChatID == 1001 {
			assert.Equal(t, "alice_pro", u.Username)
			assert.True(t, u.MinSpread.Equal(decimal.RequireFromString("0.030")))
			assert.True(t, u.MinVolume.Equal(decimal.RequireFromString("750000")))
			assert.Equal(t, domain.TF_4h, u.Timeframe)
		}
	}

	// 5. DeleteUser
	err = repo.DeleteUser(ctx, 1002)
	require.NoError(t, err)

	usersAfterDelete, err := repo.GetAllUsers(ctx)
	require.NoError(t, err)
	require.Len(t, usersAfterDelete, 1)
	assert.Equal(t, int64(1001), usersAfterDelete[0].ChatID)
}

func TestPostgresRepository_Signals_Upsert(t *testing.T) {
	repo, cleanup := setupTestPostgres(t)
	if repo == nil {
		return // Skipped due to Docker unavailability
	}
	defer cleanup()

	ctx := context.Background()

	signalID := uuid.NewString()
	openedAt := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)

	// 1. Initial Insert: Open Signal
	signal := &domain.ArbitrageSignal{
		ID:            signalID,
		Symbol:        "BTCUSDT",
		SpreadType:    domain.CrossExchange,
		ExchangeA:     "BINANCE",
		ExchangeB:     "BYBIT",
		OpenedAt:      openedAt,
		IsActive:      true,
		InitialSpread: decimal.RequireFromString("0.025"),
		PeakSpread:    decimal.RequireFromString("0.025"),
		FinalSpread:   decimal.Zero,
		Duration:      0,
	}

	err := repo.SaveSignal(ctx, signal)
	require.NoError(t, err)

	// Verify row in database
	var dbID, dbSymbol, dbSpreadType, dbExA, dbExB string
	var dbIsActive bool
	var dbInitialSpread, dbPeakSpread, dbFinalSpread string
	var dbDurationMs int64
	var dbOpenedAt, dbClosedAt time.Time

	row := repo.pool.QueryRow(ctx, `
		SELECT id, symbol, spread_type, exchange_a, exchange_b, opened_at, is_active,
		       initial_spread, peak_spread, final_spread, duration_ms
		FROM signals WHERE id = $1`, signalID)
	err = row.Scan(&dbID, &dbSymbol, &dbSpreadType, &dbExA, &dbExB, &dbOpenedAt, &dbIsActive,
		&dbInitialSpread, &dbPeakSpread, &dbFinalSpread, &dbDurationMs)
	require.NoError(t, err)

	assert.Equal(t, signalID, dbID)
	assert.Equal(t, "BTCUSDT", dbSymbol)
	assert.Equal(t, "CROSS_EXCHANGE", dbSpreadType)
	assert.Equal(t, "BINANCE", dbExA)
	assert.Equal(t, "BYBIT", dbExB)
	assert.True(t, dbIsActive)
	assert.Equal(t, "0.02500000", dbInitialSpread)
	assert.Equal(t, "0.02500000", dbPeakSpread)
	assert.Equal(t, "0.00000000", dbFinalSpread)
	assert.Equal(t, int64(0), dbDurationMs)

	// 2. UPSERT: Update Peak Spread while still active
	signal.PeakSpread = decimal.RequireFromString("0.055")
	err = repo.SaveSignal(ctx, signal)
	require.NoError(t, err)

	err = repo.pool.QueryRow(ctx, "SELECT peak_spread, is_active FROM signals WHERE id = $1", signalID).
		Scan(&dbPeakSpread, &dbIsActive)
	require.NoError(t, err)
	assert.Equal(t, "0.05500000", dbPeakSpread)
	assert.True(t, dbIsActive)

	// 3. UPSERT: Close Signal
	closedAt := openedAt.Add(45 * time.Second)
	signal.Close(closedAt, decimal.RequireFromString("0.005"))
	err = repo.SaveSignal(ctx, signal)
	require.NoError(t, err)

	err = repo.pool.QueryRow(ctx, `
		SELECT is_active, peak_spread, final_spread, duration_ms, closed_at
		FROM signals WHERE id = $1`, signalID).
		Scan(&dbIsActive, &dbPeakSpread, &dbFinalSpread, &dbDurationMs, &dbClosedAt)
	require.NoError(t, err)

	assert.False(t, dbIsActive, "is_active should be updated to false on close")
	assert.Equal(t, "0.05500000", dbPeakSpread, "peak_spread should remain highest peak")
	assert.Equal(t, "0.00500000", dbFinalSpread, "final_spread should be saved")
	assert.Equal(t, int64(45000), dbDurationMs, "duration_ms should match 45000 ms")
	assert.True(t, closedAt.Equal(dbClosedAt), "closed_at should match event time")
}
