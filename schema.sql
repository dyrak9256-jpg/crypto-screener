CREATE TABLE IF NOT EXISTS signals (
    id UUID PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    spread_type VARCHAR(50) NOT NULL,
    exchange_a VARCHAR(50) NOT NULL,
    exchange_b VARCHAR(50) NOT NULL,
    buy_exchange VARCHAR(50) NOT NULL,
    sell_exchange VARCHAR(50) NOT NULL,
    buy_market VARCHAR(20) NOT NULL,
    sell_market VARCHAR(20) NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL,
    initial_spread NUMERIC(20, 8) NOT NULL,
    peak_spread NUMERIC(20, 8) NOT NULL,
    final_spread NUMERIC(20, 8),
    quote_volume NUMERIC(30, 8) NOT NULL DEFAULT 0,
    buy_funding_rate NUMERIC(20, 12) NOT NULL DEFAULT 0,
    sell_funding_rate NUMERIC(20, 12) NOT NULL DEFAULT 0,
    buy_next_funding_at TIMESTAMPTZ,
    sell_next_funding_at TIMESTAMPTZ,
    duration_ms BIGINT
);

CREATE TABLE IF NOT EXISTS users (
    chat_id BIGINT PRIMARY KEY,
    username VARCHAR(100),
    min_spread NUMERIC(20, 8) NOT NULL DEFAULT 0.01,
    min_volume NUMERIC(30, 8) NOT NULL DEFAULT 0,
    timeframe VARCHAR(10) NOT NULL DEFAULT '15m',
    min_funding_minutes INTEGER NOT NULL DEFAULT 30,
    update_step NUMERIC(20, 8) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_signals_opened_at ON signals(opened_at);
CREATE INDEX IF NOT EXISTS idx_signals_active ON signals(is_active);

-- Backward-compatible migration for databases created by older versions.
ALTER TABLE users ADD COLUMN IF NOT EXISTS min_funding_minutes INTEGER NOT NULL DEFAULT 30;
ALTER TABLE users ADD COLUMN IF NOT EXISTS update_step NUMERIC(20, 8) NOT NULL DEFAULT 0;

ALTER TABLE signals ADD COLUMN IF NOT EXISTS buy_exchange VARCHAR(50) NOT NULL DEFAULT '';
ALTER TABLE signals ADD COLUMN IF NOT EXISTS sell_exchange VARCHAR(50) NOT NULL DEFAULT '';
ALTER TABLE signals ADD COLUMN IF NOT EXISTS buy_market VARCHAR(20) NOT NULL DEFAULT '';
ALTER TABLE signals ADD COLUMN IF NOT EXISTS sell_market VARCHAR(20) NOT NULL DEFAULT '';
ALTER TABLE signals ADD COLUMN IF NOT EXISTS quote_volume NUMERIC(30, 8) NOT NULL DEFAULT 0;
ALTER TABLE signals ADD COLUMN IF NOT EXISTS buy_funding_rate NUMERIC(20, 12) NOT NULL DEFAULT 0;
ALTER TABLE signals ADD COLUMN IF NOT EXISTS sell_funding_rate NUMERIC(20, 12) NOT NULL DEFAULT 0;
ALTER TABLE signals ADD COLUMN IF NOT EXISTS buy_next_funding_at TIMESTAMPTZ;
ALTER TABLE signals ADD COLUMN IF NOT EXISTS sell_next_funding_at TIMESTAMPTZ;

-- Настройки оператора (персистентность /sethardspread и /setfees между рестартами).
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Мульти-ботовый режим: какой бот обслуживает пользователя.
ALTER TABLE users ADD COLUMN IF NOT EXISTS bot_id BIGINT NOT NULL DEFAULT 0;
