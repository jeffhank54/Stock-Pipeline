DROP TABLE IF EXISTS ticks;
DROP TABLE IF EXISTS alerts;

CREATE TABLE ticks (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    price NUMERIC(12,2) NOT NULL,
    tick_timestamp TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_time ON ticks (symbol, tick_timestamp);

CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    pct_change NUMERIC(8,2),
    triggered_at TIMESTAMP DEFAULT NOW(),
    details TEXT
);