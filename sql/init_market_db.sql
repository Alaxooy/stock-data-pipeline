CREATE TABLE IF NOT EXISTS market_data (
    symbol        TEXT        NOT NULL,
    price         NUMERIC     NULL,
    currency      TEXT        NULL,
    volume        BIGINT      NULL,
    market_time   TIMESTAMPTZ NOT NULL,
    raw_json      JSONB       NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT market_data_pk PRIMARY KEY (symbol, market_time)
);

CREATE INDEX IF NOT EXISTS idx_market_data_time
    ON market_data (market_time DESC);
