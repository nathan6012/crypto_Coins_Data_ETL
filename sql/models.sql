-- # OLAP Schemas 
CREATE TABLE dim_crypto_asset (
    id VARCHAR PRIMARY KEY,
    symbol VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL
);


CREATE TABLE fact_crypto_market_snapshot (
    snapshot_id VARCHAR PRIMARY KEY,
    coin_id VARCHAR NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    current_price DOUBLE PRECISION,
    high_24h DOUBLE PRECISION,
    low_24h DOUBLE PRECISION,
    price_change_24h DOUBLE PRECISION,
    price_change_percentage_24h DOUBLE PRECISION,
    market_cap DOUBLE PRECISION,
    market_cap_rank INTEGER,
    market_cap_change_24h DOUBLE PRECISION,
    market_cap_change_percentage_24h DOUBLE PRECISION,
    fully_diluted_valuation DOUBLE PRECISION,
    total_volume DOUBLE PRECISION,
    circulating_supply DOUBLE PRECISION,
    total_supply DOUBLE PRECISION,
    max_supply DOUBLE PRECISION,

    ath DOUBLE PRECISION,
    ath_change_percentage DOUBLE PRECISION,
    ath_date TIMESTAMP,
    atl DOUBLE PRECISION,
    atl_change_percentage DOUBLE PRECISION,
    atl_date TIMESTAMP,
    roi TEXT,
    roi_times DOUBLE PRECISION,
    roi_currency VARCHAR(50),
    roi_percentage DOUBLE PRECISION,
    CONSTRAINT fk_coin
        FOREIGN KEY (coin_id)
        REFERENCES dim_crypto_asset(id)
);





CREATE INDEX idx_fact_coin_time
ON fact_crypto_market_snapshot (coin_id, last_updated);

CREATE INDEX idx_fact_market_cap_rank
ON fact_crypto_market_snapshot (market_cap_rank);