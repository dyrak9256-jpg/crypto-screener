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
    duration_ms BIGINT,
    quote_volume NUMERIC(20, 8) NOT NULL DEFAULT 0   -- 24h rolling quote volume на момент сигнала
);


CREATE TABLE users (
    chat_id BIGINT PRIMARY KEY,
    username VARCHAR(100),
    min_spread NUMERIC(20, 8) NOT NULL DEFAULT 0.01,      -- Дробь (0.01 = 1%)
    min_volume NUMERIC(20, 8) NOT NULL DEFAULT 1000000,  -- USDT
    timeframe VARCHAR(10) NOT NULL DEFAULT '15m',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_signals_symbol ON signals(symbol);
CREATE INDEX idx_signals_opened_at ON signals(opened_at);