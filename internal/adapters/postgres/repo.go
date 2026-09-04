package postgres

import (
	"context"
	"crypto-screener/internal/domain"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct{ pool *pgxpool.Pool }

func NewRepository(ctx context.Context, dbURL string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("unable to create pool: %w", err)
	}
	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() { r.pool.Close() }

func (r *Repository) SaveSignal(ctx context.Context, s *domain.ArbitrageSignal) error {
	query := `
		INSERT INTO signals (id, symbol, spread_type, exchange_a, exchange_b, opened_at, closed_at, is_active, initial_spread, peak_spread, final_spread, duration_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (id) DO UPDATE SET closed_at = EXCLUDED.closed_at, is_active = EXCLUDED.is_active, peak_spread = EXCLUDED.peak_spread, final_spread = EXCLUDED.final_spread, duration_ms = EXCLUDED.duration_ms`

	_, err := r.pool.Exec(ctx, query, s.ID, s.Symbol, string(s.SpreadType), s.ExchangeA, s.ExchangeB,
		s.OpenedAt, s.ClosedAt, s.IsActive, s.InitialSpread.String(), s.PeakSpread.String(), s.FinalSpread.String(), s.Duration.Milliseconds())
	return err
}
