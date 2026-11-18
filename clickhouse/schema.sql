-- Kafka → ClickHouse. Rendered by clickhouse/render.py: the {{...}} placeholders
-- are the Kafka API key of the "clickhouse" service account and never live in git.

CREATE DATABASE IF NOT EXISTS marketdata;

-- The archive. ReplacingMergeTree: Kafka is at-least-once, a replayed batch
-- collapses on (exchange, symbol, ts, trade_id).
CREATE TABLE IF NOT EXISTS marketdata.trades
(
    exchange LowCardinality(String),
    symbol   LowCardinality(String),
    base     LowCardinality(String),
    quote    LowCardinality(String),
    trade_id String,
    price    Decimal(38, 12),
    qty      Decimal(38, 12),
    side     Enum8('buy' = 1, 'sell' = 2),
    ts       DateTime64(3, 'UTC'),
    recv_ts  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (exchange, symbol, ts, trade_id);

-- The consumer. One consumer: connections are capped at 20 on the free eCKU.
-- SASL goes in table settings, so the server config needs no <kafka> section
-- and the server needs no restart.
CREATE TABLE IF NOT EXISTS marketdata.trades_queue
(
    exchange String,
    symbol   String,
    base     String,
    quote    String,
    trade_id String,
    price    String,
    qty      String,
    side     String,
    ts       UInt64,
    recv_ts  UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{{KAFKA_BOOTSTRAP}}',
    kafka_topic_list = 'marketdata.binance.trades,marketdata.kraken.trades',
    kafka_group_name = 'clickhouse-marketdata',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_security_protocol = 'sasl_ssl',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '{{KAFKA_API_KEY}}',
    kafka_sasl_password = '{{KAFKA_API_SECRET}}';

CREATE MATERIALIZED VIEW IF NOT EXISTS marketdata.trades_mv TO marketdata.trades AS
SELECT
    exchange,
    symbol,
    base,
    quote,
    trade_id,
    toDecimal128(price, 12) AS price,
    toDecimal128(qty, 12)   AS qty,
    CAST(side, 'Enum8(\'buy\' = 1, \'sell\' = 2)') AS side,
    fromUnixTimestamp64Milli(toInt64(ts), 'UTC')      AS ts,
    fromUnixTimestamp64Milli(toInt64(recv_ts), 'UTC') AS recv_ts
FROM marketdata.trades_queue;
