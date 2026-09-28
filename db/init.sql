-- Ticklag TimescaleDB schema.
-- Runs automatically on first container start (mounted into
-- /docker-entrypoint-initdb.d). Creates the extension, two hypertables,
-- and two continuous aggregates (1-minute rollups).

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------- raw tables ----------

CREATE TABLE IF NOT EXISTS ticks (
    event_time  TIMESTAMPTZ      NOT NULL,
    symbol      TEXT             NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    volume      DOUBLE PRECISION,
    zscore      DOUBLE PRECISION,
    is_anomaly  BOOLEAN          DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS sensor_readings (
    event_time   TIMESTAMPTZ      NOT NULL,
    machine_id   TEXT             NOT NULL,
    sensor_id    TEXT             NOT NULL,
    sensor_type  TEXT             NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    zscore       DOUBLE PRECISION,
    is_anomaly   BOOLEAN          DEFAULT FALSE
);

-- Convert to hypertables. Timescale partitions by time into "chunks" under
-- the hood; queries and inserts hit only the relevant chunks, which is what
-- makes time-range queries fast at scale. This is the core reason to use
-- Timescale over vanilla Postgres for this workload.
SELECT create_hypertable('ticks', 'event_time', if_not_exists => TRUE);
SELECT create_hypertable('sensor_readings', 'event_time', if_not_exists => TRUE);

-- Indexes for the dashboard query patterns (filter by symbol/machine, order by time)
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_time ON ticks (symbol, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_machine_time ON sensor_readings (machine_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_anomaly ON sensor_readings (is_anomaly, event_time DESC) WHERE is_anomaly;

-- ---------- continuous aggregates (1-minute rollups) ----------
-- A continuous aggregate is a materialized view Timescale keeps incrementally
-- up to date in the background. Dashboards query the rollup instead of scanning
-- raw rows, so panel load stays cheap even as raw data grows.

CREATE MATERIALIZED VIEW IF NOT EXISTS ticks_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', event_time) AS bucket,
    symbol,
    avg(price)   AS avg_price,
    max(price)   AS max_price,
    min(price)   AS min_price,
    count(*)     AS tick_count,
    sum(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count
FROM ticks
GROUP BY bucket, symbol
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_readings_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', event_time) AS bucket,
    machine_id,
    sensor_type,
    avg(value)   AS avg_value,
    max(value)   AS max_value,
    stddev(value) AS stddev_value,
    count(*)     AS reading_count,
    sum(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count
FROM sensor_readings
GROUP BY bucket, machine_id, sensor_type
WITH NO DATA;

-- Refresh policies: keep the rollups current automatically.
SELECT add_continuous_aggregate_policy('ticks_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);

SELECT add_continuous_aggregate_policy('sensor_readings_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);
