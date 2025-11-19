CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS tickers (
    symbol VARCHAR(20) PRIMARY KEY,
    region VARCHAR(10) NOT NULL,
    asset_type VARCHAR(20) NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS ohlcv (
    symbol VARCHAR(20),
    date TIMESTAMPTZ,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (symbol, date)
);

CREATE INDEX idx_ohlcv_symbol_date ON ohlcv (symbol, date DESC);

SELECT create_hypertable('ohlcv', 'date', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS fundamentals (
    symbol VARCHAR(20) PRIMARY KEY,
    roe DOUBLE PRECISION,
    roa DOUBLE PRECISION,
    debt_to_equity DOUBLE PRECISION,
    current_ratio DOUBLE PRECISION,
    net_margin DOUBLE PRECISION,
    gross_margin DOUBLE PRECISION,
    operating_margin DOUBLE PRECISION,
    interest_coverage DOUBLE PRECISION,
    pe_ratio DOUBLE PRECISION,
    market_cap DOUBLE PRECISION,
    source VARCHAR(50),
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol VARCHAR(20) NOT NULL,
    action VARCHAR(10) NOT NULL,
    confidence DOUBLE PRECISION,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    price DOUBLE PRECISION,
    executed BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    action VARCHAR(10) NOT NULL,
    quantity INTEGER NOT NULL,
    order_type VARCHAR(20) NOT NULL,
    limit_price DOUBLE PRECISION,
    status VARCHAR(20) DEFAULT 'pending',
    broker VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    executed_price DOUBLE PRECISION,
    executed_quantity INTEGER
);

CREATE TABLE IF NOT EXISTS positions (
    symbol VARCHAR(20) PRIMARY KEY,
    quantity INTEGER NOT NULL,
    avg_cost DOUBLE PRECISION,
    market_value DOUBLE PRECISION,
    unrealized_pnl DOUBLE PRECISION,
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(50) NOT NULL,
    user_id VARCHAR(50),
    ticker VARCHAR(20),
    action VARCHAR(50),
    details JSONB,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_timestamp ON audit_log (timestamp DESC);
