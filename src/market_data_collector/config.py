"""Configuration from the environment (pm2 loads .env; nothing is read from disk here)."""
from __future__ import annotations

import os
from dataclasses import dataclass

# The trade-lab basket (execution/config.py: _DEFAULT_BASKET).
DEFAULT_BASES = "BTC,ETH,BNB,SOL,ADA,XRP,DOGE"


def _csv(name: str, default: str) -> tuple[str, ...]:
    return tuple(s.strip().upper() for s in os.environ.get(name, default).split(",") if s.strip())


@dataclass(frozen=True)
class Config:
    bases: tuple[str, ...]
    exchanges: tuple[str, ...]
    binance_quote: str
    kraken_quote: str
    binance_ws: str
    kraken_ws: str
    topic_prefix: str
    kafka_bootstrap: str
    kafka_key: str
    kafka_secret: str
    linger_ms: int
    marker_dir: str

    @property
    def binance_symbols(self) -> tuple[str, ...]:
        return tuple(f"{b}{self.binance_quote}" for b in self.bases)

    @property
    def kraken_symbols(self) -> tuple[str, ...]:
        return tuple(f"{b}/{self.kraken_quote}" for b in self.bases)

    def topic(self, exchange: str) -> str:
        return f"{self.topic_prefix}.{exchange}.trades"


def load() -> Config:
    env = os.environ
    return Config(
        bases=_csv("MDC_BASES", DEFAULT_BASES),
        exchanges=tuple(e.lower() for e in _csv("MDC_EXCHANGES", "binance,kraken")),
        binance_quote=env.get("MDC_BINANCE_QUOTE", "USDT").upper(),
        kraken_quote=env.get("MDC_KRAKEN_QUOTE", "USD").upper(),
        # Market-data-only host: no account endpoints behind it.
        binance_ws=env.get("MDC_BINANCE_WS", "wss://data-stream.binance.vision"),
        kraken_ws=env.get("MDC_KRAKEN_WS", "wss://ws.kraken.com/v2"),
        topic_prefix=env.get("MDC_TOPIC_PREFIX", "marketdata"),
        kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", ""),
        kafka_key=env.get("KAFKA_API_KEY", ""),
        kafka_secret=env.get("KAFKA_API_SECRET", ""),
        linger_ms=int(env.get("MDC_LINGER_MS", "500")),
        # Relative to the pm2 cwd; empty disables the markers.
        marker_dir=env.get("MDC_MARKER_DIR", ".markers"),
    )
