package postgres

import (
	"context"
	"fmt"

	"crypto-screener/internal/domain"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(ctx context.Context, dbURL string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("unable to create pool: %w", err)
	}
	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() {
	r.pool.Close()
}

// --- Signals ---

func (r *Repository) SaveSignal(ctx context.Context, s *domain.ArbitrageSignal) error {
	query := `
		INSERT INTO signals (id, symbol, spread_type, exchange_a, exchange_b, opened_at, closed_at, is_active, initial_spread, peak_spread, final_spread, duration_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (id) DO UPDATE SET closed_at = EXCLUDED.closed_at, is_active = EXCLUDED.is_active, peak_spread = EXCLUDED.peak_spread, final_spread = EXCLUDED.final_spread, duration_ms = EXCLUDED.duration_ms`

	_, err := r.pool.Exec(ctx, query, s.ID, s.Symbol, string(s.SpreadType), s.ExchangeA, s.ExchangeB,
		s.OpenedAt, s.ClosedAt, s.IsActive, s.InitialSpread.String(), s.PeakSpread.String(), s.FinalSpread.String(), s.Duration.Milliseconds())
	return err
}

// --- Users ---

func (r *Repository) SaveUser(ctx context.Context, u *domain.User) error {
	query := `
		INSERT INTO users (chat_id, username, min_spread, min_volume, timeframe)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (chat_id) DO UPDATE SET 
			username = EXCLUDED.username, 
			min_spread = EXCLUDED.min_spread, 
			min_volume = EXCLUDED.min_volume, 
			timeframe = EXCLUDED.timeframe`
	_, err := r.pool.Exec(ctx, query, u.ChatID, u.Username, u.MinSpread.String(), u.MinVolume.String(), string(u.Timeframe))
	return err
}

func (r *Repository) DeleteUser(ctx context.Context, chatID int64) error {
	_, err := r.pool.Exec(ctx, "DELETE FROM users WHERE chat_id = $1", chatID)
	return err
}

func (r *Repository) GetAllUsers(ctx context.Context) ([]*domain.User, error) {
	rows, err := r.pool.Query(ctx, "SELECT chat_id, username, min_spread, min_volume, timeframe FROM users")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []*domain.User
	for rows.Next() {
		u := &domain.User{}
		var spreadStr, volStr string
		if err := rows.Scan(&u.ChatID, &u.Username, &spreadStr, &volStr, &u.Timeframe); err != nil {
			return nil, err
		}
		u.MinSpread, _ = decimal.NewFromString(spreadStr)
		u.MinVolume, _ = decimal.NewFromString(volStr)
		users = append(users, u)
	}
	return users, nil
}
