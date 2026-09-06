package postgres

import (
	"context"
	"fmt"
	"time"

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
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	// ✅ Проверяем реальное соединение при старте
	// pgxpool.New создаёт пул лениво — без Ping ошибки конфигурации
	// обнаружатся только при первом запросе в продакшене
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() {
	r.pool.Close()
}

// --- Signals ---

func (r *Repository) SaveSignal(ctx context.Context, s *domain.ArbitrageSignal) error {
	const query = `
		INSERT INTO signals (
			id, symbol, spread_type, exchange_a, exchange_b,
			opened_at, closed_at, is_active,
			initial_spread, peak_spread, final_spread, duration_ms
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (id) DO UPDATE SET
			closed_at     = EXCLUDED.closed_at,
			is_active     = EXCLUDED.is_active,
			peak_spread   = EXCLUDED.peak_spread,
			final_spread  = EXCLUDED.final_spread,
			duration_ms   = EXCLUDED.duration_ms`

	// ✅ Используем *time.Time вместо sql.NullTime
	// pgx/v5 нативно понимает указатели как NULL
	// Активный сигнал: ClosedAt = time.Time{} → передаём nil → PostgreSQL NULL
	var closedAt *time.Time
	if !s.ClosedAt.IsZero() {
		closedAt = &s.ClosedAt
	}

	_, err := r.pool.Exec(ctx, query,
		s.ID,
		s.Symbol,
		string(s.SpreadType),
		s.ExchangeA,
		s.ExchangeB,
		s.OpenedAt,
		closedAt, // NULL для активных сигналов
		s.IsActive,
		s.InitialSpread.String(),
		s.PeakSpread.String(),
		s.FinalSpread.String(),
		s.Duration.Milliseconds(),
	)
	if err != nil {
		return fmt.Errorf("save signal %s: %w", s.ID, err)
	}

	return nil
}

// --- Users ---

func (r *Repository) SaveUser(ctx context.Context, u *domain.User) error {
	const query = `
		INSERT INTO users (chat_id, username, min_spread, min_volume, timeframe)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (chat_id) DO UPDATE SET
			username   = EXCLUDED.username,
			min_spread = EXCLUDED.min_spread,
			min_volume = EXCLUDED.min_volume,
			timeframe  = EXCLUDED.timeframe`

	_, err := r.pool.Exec(ctx, query,
		u.ChatID,
		u.Username,
		u.MinSpread.String(),
		u.MinVolume.String(),
		string(u.Timeframe),
	)
	if err != nil {
		return fmt.Errorf("save user %d: %w", u.ChatID, err)
	}

	return nil
}

func (r *Repository) DeleteUser(ctx context.Context, chatID int64) error {
	_, err := r.pool.Exec(ctx, "DELETE FROM users WHERE chat_id = $1", chatID)
	if err != nil {
		return fmt.Errorf("delete user %d: %w", chatID, err)
	}
	return nil
}

func (r *Repository) GetAllUsers(ctx context.Context) ([]*domain.User, error) {
	const query = `
		SELECT chat_id, username, min_spread, min_volume, timeframe
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
			return nil, err
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
	const query = `
		SELECT chat_id, username, min_spread, min_volume, timeframe
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
