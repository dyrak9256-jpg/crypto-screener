package postgres

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"crypto-screener/internal/domain"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

type Repository struct {
	pool *pgxpool.Pool
}

//go:embed schema.sql
var schemaSQL string

func NewRepository(ctx context.Context, dbURL string) (*Repository, error) {
	if dbURL == "" {
		return nil, fmt.Errorf("database URL is empty")
	}
	poolCfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	poolCfg.MaxConns = 20
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	poolCfg.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	// ✅ Проверяем реальное соединение при старте
	// pgxpool.New создаёт пул лениво — без Ping ошибки конфигурации
	// обнаружатся только при первом запросе в продакшене
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply database schema: %w", err)
	}

	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

// ReconcileActiveSignals closes signals that were left active by a previous
// process instance. Active lifecycle state is in-memory, so after a crash there
// is no valid in-memory route that can keep those rows active.
func (r *Repository) ReconcileActiveSignals(ctx context.Context) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres repository is nil")
	}
	const query = `
		UPDATE signals
		SET is_active = FALSE,
			closed_at = NOW(),
			final_spread = NULL,
			duration_ms = GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (NOW() - opened_at)) * 1000))
		WHERE is_active = TRUE`
	if _, err := r.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("reconcile active signals: %w", err)
	}
	return nil
}

// --- Signals ---

func (r *Repository) SaveSignal(ctx context.Context, s *domain.ArbitrageSignal) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres repository is nil")
	}
	if s == nil {
		return fmt.Errorf("signal is nil")
	}
	const query = `
		INSERT INTO signals (
			id, symbol, spread_type, exchange_a, exchange_b, buy_exchange, sell_exchange,
			buy_market, sell_market, opened_at, closed_at, is_active,
			initial_spread, peak_spread, final_spread, quote_volume, buy_funding_rate, sell_funding_rate, buy_next_funding_at, sell_next_funding_at, duration_ms
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
		ON CONFLICT (id) DO UPDATE SET
			closed_at = EXCLUDED.closed_at, is_active = EXCLUDED.is_active,
			peak_spread = EXCLUDED.peak_spread, final_spread = EXCLUDED.final_spread,
			quote_volume = EXCLUDED.quote_volume, buy_funding_rate = EXCLUDED.buy_funding_rate,
			sell_funding_rate = EXCLUDED.sell_funding_rate, buy_next_funding_at = EXCLUDED.buy_next_funding_at,
			sell_next_funding_at = EXCLUDED.sell_next_funding_at, duration_ms = EXCLUDED.duration_ms`

	// ✅ Используем *time.Time вместо sql.NullTime
	// pgx/v5 нативно понимает указатели как NULL
	// Активный сигнал: ClosedAt = time.Time{} → передаём nil → PostgreSQL NULL
	var closedAt *time.Time
	if !s.ClosedAt.IsZero() {
		closedAt = &s.ClosedAt
	}

	_, err := r.pool.Exec(ctx, query,
		s.ID, s.Symbol, string(s.SpreadType),
		s.ExchangeA, s.ExchangeB, s.BuyExchange, s.SellExchange,
		string(s.BuyMarket), string(s.SellMarket), s.OpenedAt, closedAt, s.IsActive,
		s.InitialSpread.String(), s.PeakSpread.String(), s.FinalSpread.String(),
		s.QuoteVolume.String(), s.BuyFundingRate.String(), s.SellFundingRate.String(), nextFundingPtr(s.BuyNextFunding), nextFundingPtr(s.SellNextFunding),
		s.Duration.Milliseconds(),
	)
	if err != nil {
		return fmt.Errorf("save signal %s: %w", s.ID, err)
	}

	return nil
}

// --- Users ---

func (r *Repository) SaveUser(ctx context.Context, u *domain.User) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres repository is nil")
	}
	if u == nil {
		return fmt.Errorf("user is nil")
	}
	const query = `
		INSERT INTO users (chat_id, username, min_spread, min_volume, timeframe, min_funding_minutes)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (chat_id) DO UPDATE SET
			username   = EXCLUDED.username,
			min_spread = EXCLUDED.min_spread,
			min_volume = EXCLUDED.min_volume,
			timeframe  = EXCLUDED.timeframe,
			min_funding_minutes = EXCLUDED.min_funding_minutes`

	_, err := r.pool.Exec(ctx, query,
		u.ChatID,
		u.Username,
		u.MinSpread.String(),
		u.MinVolume.String(),
		string(u.Timeframe),
		u.MinFundingMinutes,
	)
	if err != nil {
		return fmt.Errorf("save user %d: %w", u.ChatID, err)
	}

	return nil
}

func (r *Repository) DeleteUser(ctx context.Context, chatID int64) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres repository is nil")
	}
	_, err := r.pool.Exec(ctx, "DELETE FROM users WHERE chat_id = $1", chatID)
	if err != nil {
		return fmt.Errorf("delete user %d: %w", chatID, err)
	}
	return nil
}

func (r *Repository) GetAllUsers(ctx context.Context) ([]*domain.User, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("postgres repository is nil")
	}
	const query = `
		SELECT chat_id, username, min_spread, min_volume, timeframe, min_funding_minutes
		FROM users`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query all users: %w", err)
	}
	defer rows.Close()

	var users []*domain.User

	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, fmt.Errorf("GetAllUsers: %w", err)
		}
		users = append(users, u)
	}

	// ✅ Критично: rows.Next() → false может означать сетевой сбой
	// Без этой проверки возвращаем частичные данные как полные
	// Часть пользователей не получит уведомления — молчаливая потеря данных
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate users: %w", err)
	}

	return users, nil
}

func (r *Repository) GetUserByChatID(ctx context.Context, chatID int64) (*domain.User, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("postgres repository is nil")
	}
	const query = `
		SELECT chat_id, username, min_spread, min_volume, timeframe, min_funding_minutes
		FROM users WHERE chat_id = $1`

	rows, err := r.pool.Query(ctx, query, chatID)
	if err != nil {
		return nil, fmt.Errorf("query user %d: %w", chatID, err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("query user %d: %w", chatID, err)
		}
		return nil, nil // пользователь не найден
	}

	return scanUser(rows)
}

// scanUser читает одну строку и возвращает User
// Выделено отдельно чтобы не дублировать логику в GetAllUsers и GetUserByChatID
func scanUser(rows interface {
	Scan(dest ...any) error
}) (*domain.User, error) {
	u := &domain.User{}
	var spreadStr, volStr string

	if err := rows.Scan(
		&u.ChatID,
		&u.Username,
		&spreadStr,
		&volStr,
		&u.Timeframe,
		&u.MinFundingMinutes,
	); err != nil {
		return nil, fmt.Errorf("scan user row: %w", err)
	}

	// ✅ Строгая проверка decimal — ошибка парсинга не должна
	// обнуляться в молчании. MinSpread = 0 → пользователь получит
	// ВСЕ сигналы независимо от настроек
	minSpread, err := decimal.NewFromString(spreadStr)
	if err != nil {
		return nil, fmt.Errorf("invalid min_spread %q for user %d: %w",
			spreadStr, u.ChatID, err)
	}

	minVolume, err := decimal.NewFromString(volStr)
	if err != nil {
		return nil, fmt.Errorf("invalid min_volume %q for user %d: %w",
			volStr, u.ChatID, err)
	}

	u.MinSpread = minSpread
	u.MinVolume = minVolume

	return u, nil
}

func nextFundingPtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
