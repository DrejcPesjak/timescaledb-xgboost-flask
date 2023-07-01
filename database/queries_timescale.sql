/*
Based on the Timescale tutorial
https://docs.timescale.com/getting-started/latest/
*/

/*CONNECT TO DB*/
/*psql "postgres://username:password@host:port/dbname?sslmode=require"*/


/*TABLE CREATION*/
CREATE TABLE stocks_real_time (
  time TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NULL,
  day_volume INT NULL
);

SELECT create_hypertable('stocks_real_time','time');

CREATE INDEX ix_symbol_time ON stocks_real_time (symbol, time DESC);


CREATE TABLE company (
  symbol TEXT NOT NULL,
  name TEXT NOT NULL
);

\dt


/*
# GET THE STOCKS DATASET
wget "https://assets.timescale.com/docs/downloads/get-started/real_time_stock_data.zip"
unzip real_time_stock_data.zip
*/

\COPY stocks_real_time from './tutorial_sample_tick.csv' DELIMITER ',' CSV HEADER;
\COPY company from './tutorial_sample_company.csv' DELIMITER ',' CSV HEADER;


/*QUERIES*/
SELECT * FROM stocks_real_time srt
WHERE time > now() - INTERVAL '9 days';

SELECT * FROM stocks_real_time srt
WHERE symbol='TSLA' and day_volume is not null
ORDER BY time DESC, day_volume desc
LIMIT 10;

SELECT symbol, first(price,time), last(price, time)
FROM stocks_real_time srt
WHERE time > now() - INTERVAL '9 days'
GROUP BY symbol
ORDER BY symbol;

SELECT
  time_bucket('1 day', time) AS bucket,
  symbol,
  avg(price)
FROM stocks_real_time srt
WHERE time > now() - INTERVAL '1 week'
GROUP BY bucket, symbol
ORDER BY bucket, symbol;

SELECT
  time_bucket('1 day', "time") AS day,
  symbol,
  max(price) AS high,
  first(price, time) AS open,
  last(price, time) AS close,
  min(price) AS low
FROM stocks_real_time srt
GROUP BY day, symbol
ORDER BY day DESC, symbol;

/*continuous aggregate to aggregate data for each day*/
CREATE MATERIALIZED VIEW stock_candlestick_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', "time") AS day,
  symbol,
  max(price) AS high,
  first(price, time) AS open,
  last(price, time) AS close,
  min(price) AS low
FROM stocks_real_time srt
GROUP BY day, symbol;

SELECT * FROM stock_candlestick_daily
  ORDER BY day DESC, symbol;

SELECT * FROM stock_candlestick_daily
WHERE symbol='TSLA';

/*continuous aggregate to aggregate data for each day*/
CREATE MATERIALIZED VIEW one_day_candle
WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 day', time) AS bucket,
        symbol,
        FIRST(price, time) AS "open",
        MAX(price) AS high,
        MIN(price) AS low,
        LAST(price, time) AS "close",
        LAST(day_volume, time) AS day_volume
    FROM stocks_real_time
    GROUP BY bucket, symbol;

SELECT add_continuous_aggregate_policy('one_day_candle',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');

SELECT * FROM one_day_candle
WHERE symbol = 'BTC/USD' AND bucket >= NOW() - INTERVAL '14 days'
ORDER BY bucket;

/*continuous aggregate to aggregate data every hour*/
CREATE MATERIALIZED VIEW one_hour_candle
WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 hour', time) AS bucket,
        symbol,
        FIRST(price, time) AS "open",
        MAX(price) AS high,
        MIN(price) AS low,
        LAST(price, time) AS "close",
        LAST(day_volume, time) AS day_volume
    FROM stocks_real_time
    GROUP BY bucket, symbol;

SELECT add_continuous_aggregate_policy('one_hour_candle',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');