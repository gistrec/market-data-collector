"""Where normalized trades go: Kafka in production, stdout for a dry run."""
from __future__ import annotations

import json
import logging
import sys
import time
from collections import Counter

from .config import Config

log = logging.getLogger(__name__)


class StdoutSink:
    def __init__(self) -> None:
        self.sent: Counter[str] = Counter()
        self.acked: dict[str, float] = {}  # nothing is delivered, so no markers on a dry run

    def send(self, topic: str, record: dict) -> None:
        self.sent[topic] += 1
        sys.stdout.write(f"{topic} {json.dumps(record, separators=(',', ':'))}\n")

    def poll(self) -> None:
        sys.stdout.flush()

    def close(self) -> None:
        sys.stdout.flush()


class KafkaSink:
    """One producer for every exchange: the free eCKU caps the cluster at 20 connections."""

    def __init__(self, cfg: Config) -> None:
        from confluent_kafka import Producer

        if not (cfg.kafka_bootstrap and cfg.kafka_key and cfg.kafka_secret):
            raise SystemExit("KAFKA_BOOTSTRAP, KAFKA_API_KEY and KAFKA_API_SECRET are required")
        self.sent: Counter[str] = Counter()
        self.failed = 0
        # topic -> time.time() of the last broker ack: what the freshness markers report.
        self.acked: dict[str, float] = {}
        self._producer = Producer({
            "bootstrap.servers": cfg.kafka_bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": cfg.kafka_key,
            "sasl.password": cfg.kafka_secret,
            "client.id": "market-data-collector",
            "enable.idempotence": True,
            # Batch hard: the free eCKU allows 100 requests/s, and ingress is billed per GB.
            "linger.ms": cfg.linger_ms,
            "compression.type": "zstd",
        })

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed: topic=%s err=%s", msg.topic(), err)
        else:
            self.acked[msg.topic()] = time.time()

    def send(self, topic: str, record: dict) -> None:
        payload = json.dumps(record, separators=(",", ":")).encode()
        key = record["symbol"].encode()
        try:
            self._producer.produce(topic, payload, key=key, on_delivery=self._on_delivery)
        except BufferError:
            # Local queue full (broker unreachable for a while): drain, then retry once.
            self._producer.poll(1.0)
            self._producer.produce(topic, payload, key=key, on_delivery=self._on_delivery)
        self.sent[topic] += 1

    def poll(self) -> None:
        self._producer.poll(0)

    def close(self) -> None:
        left = self._producer.flush(15)
        if left:
            log.error("%d messages were still queued at shutdown", left)
