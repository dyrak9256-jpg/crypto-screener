CREATE TABLE signals (
    id UUID PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    spread_type VARCHAR(50) NOT NULL,
    exchange_a VARCHAR(50) NOT NULL,
    exchange_b VARCHAR(50) NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL,
    initial_spread NUMERIC(20, 8) NOT NULL,
    peak_spread NUMERIC(20, 8) NOT NULL,
    final_spread NUMERIC(20, 8),
    duration_ms BIGINT
);
CREATE INDEX idx_signals_symbol ON signals(symbol);
CREATE INDEX idx_signals_opened_at ON signals(opened_at);