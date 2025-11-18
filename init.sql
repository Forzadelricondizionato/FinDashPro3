-- Database initialization for FinDashPro
CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    action VARCHAR(10) NOT NULL,
    confidence FLOAT NOT NULL,
    predicted_return FLOAT,
    timestamp TIMESTAMPTZ NOT NULL,
    execution_status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    INDEX idx_ticker_timestamp (ticker, timestamp),
    INDEX idx_timestamp (timestamp)
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(100) UNIQUE NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    action VARCHAR(10) NOT NULL,
    quantity FLOAT NOT NULL,
    price FLOAT,
    status VARCHAR(20) DEFAULT 'submitted',
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ml_metrics (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value FLOAT NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    INDEX idx_ticker_metric (ticker, metric_name, timestamp)
);

-- Immutabile audit trail per compliance
CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB NOT NULL,
    user_id VARCHAR(100),
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    origin_ip INET,
    INDEX idx_event_type (event_type, timestamp)
);

