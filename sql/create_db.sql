-- TODO: Add users table

CREATE TABLE stock_information(
	ticker VARCHAR(16) PRIMARY KEY,
	exchange_name VARCHAR(16),
	company_name VARCHAR(128),
	last_price NUMERIC(11,4),
	change_amount NUMERIC(11,4),
	change_percent NUMERIC(8,3),
	share_volume BIGINT,
	price_updated TIMESTAMP,
	trade_history TEXT,
	history_updated TIMESTAMP,
	created TIMESTAMP NOT NULL
);

CREATE TABLE stock_statistics (
	id BIGSERIAL PRIMARY KEY,
	ticker_ref VARCHAR(16) REFERENCES stock_information(ticker) ON DELETE CASCADE NOT NULL,
	stat_type VARCHAR(64) NOT NULL,
	stat_source SMALLINT,
	price_data NUMERIC(11,4),
	int_data BIGINT,
	precision_data NUMERIC(12,8),
	date_data TIMESTAMP
);

CREATE TABLE alphavantage_api_log (
	id SERIAL PRIMARY KEY,
	log_date DATE NOT NULL,
	date_total INTEGER NOT NULL DEFAULT 1,
	last_call_minute TIMESTAMP NOT NULL,
	last_minute_count SMALLINT NOT NULL DEFAULT 1
);