-- Kafka → ClickHouse. SASL credentials are not here: they live in the server
-- config (<kafka> section, service account "clickhouse").

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
    kafka_broker_list = 'pkc-l7q2j.europe-north1.gcp.confluent.cloud:9092',
    kafka_topic_list = 'marketdata.binance.trades,marketdata.kraken.trades',
    kafka_group_name = 'clickhouse-marketdata',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

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
