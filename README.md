# market-data-collector

Public trade streams from Binance and Kraken, normalized into one record shape
and produced to Apache Kafka (Confluent Cloud). ClickHouse consumes the topics
with its Kafka engine and keeps the archive.

```
Binance WS ─┐                      ┌─ marketdata.binance.trades ─┐
            ├─ market-data-collector ─► Kafka (Confluent Cloud)  ├─► ClickHouse
Kraken WS  ─┘                      └─ marketdata.kraken.trades  ─┘   marketdata.trades
```

No exchange API keys are involved: both feeds are public market data.

## Record

One JSON object per trade, keyed by `symbol`:

```json
{"exchange":"binance","symbol":"BNBUSDT","base":"BNB","quote":"USDT","trade_id":"1623040619",
 "price":"589.91000000","qty":"0.00900000","side":"buy","ts":1795004076650,"recv_ts":1795004076945}
```

- `price` / `qty` are strings: the exchange's decimal text, never a binary float.
- `side` is the aggressor (taker) side.
- `ts` is the exchange trade time, `recv_ts` the local receive time, both in ms.

## Run

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest

# Dry run: print records instead of producing, stop after 30 s.
.venv/bin/market-data-collector --stdout --seconds 30

# Production: needs KAFKA_BOOTSTRAP / KAFKA_API_KEY / KAFKA_API_SECRET (see env.example).
pm2 start ecosystem.config.js
```

Pairs default to the trade-lab basket (`BTC, ETH, BNB, SOL, ADA, XRP, DOGE`):
quoted in USDT on Binance and in USD on Kraken. Override with `MDC_BASES`,
`MDC_BINANCE_QUOTE`, `MDC_KRAKEN_QUOTE`.

## Sized for a free Kafka cluster

The cluster is a Confluent Cloud Basic cluster capped at one eCKU (free compute):
30 partitions, 20 client connections, 100 requests/s.

- One producer for both exchanges, one partition per topic.
- `linger.ms=500` and zstd: a few large requests per second instead of one per trade.
- Trades only. Order-book depth streams are orders of magnitude larger.

Delivery is at-least-once (idempotent producer, reconnect on any socket error).
A gap while a socket is down is not backfilled.

## ClickHouse

[`clickhouse/schema.sql`](clickhouse/schema.sql): a Kafka-engine table over both
topics, a materialized view, and `marketdata.trades` (ReplacingMergeTree, so a
replayed batch collapses on `exchange, symbol, ts, trade_id`).

Infrastructure (cluster, topics, service accounts, ACLs) lives in
[gistrec-cloud/infra](https://github.com/gistrec-cloud/infra) under `terraform/confluent`.
