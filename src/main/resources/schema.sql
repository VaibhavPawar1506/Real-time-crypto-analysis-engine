CREATE TABLE IF NOT EXISTS crypto_market_history (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(255) NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    vwap DOUBLE PRECISION,
    ticks_count INT,
    persisted_at TIMESTAMP
);
