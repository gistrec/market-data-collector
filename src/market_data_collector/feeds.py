"""WebSocket feeds: connect, subscribe, normalize, reconnect forever."""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Callable, Iterator
from typing import Any

import websockets

from . import normalize
from .config import Config

log = logging.getLogger(__name__)

Normalizer = Callable[[Any, int], Iterator[dict]]


async def _run(
    name: str,
    url: str,
    subscribe: dict | None,
    normalizer: Normalizer,
    emit: Callable[[dict], None],
) -> None:
    backoff = 1.0
    while True:
        started = time.monotonic()
        try:
            # The library answers the exchange's pings itself and sends its own:
            # a silent dead socket is closed after ping_timeout instead of hanging.
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                if subscribe is not None:
                    await ws.send(json.dumps(subscribe))
                log.info("%s: connected", name)
                async for raw in ws:
                    recv_ms = time.time_ns() // 1_000_000
                    msg = normalize.loads(raw)
                    if isinstance(msg, dict) and msg.get("success") is False:
                        log.error("%s: request rejected: %s", name, msg.get("error"))
                        continue
                    for record in normalizer(msg, recv_ms):
                        emit(record)
            log.warning("%s: connection closed by the server", name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — network errors, bad frames: reconnect either way
            log.warning("%s: %s: %s", name, type(exc).__name__, exc)
        # A connection that lived a while was healthy: start the backoff over.
        if time.monotonic() - started > 60:
            backoff = 1.0
        await asyncio.sleep(backoff + random.random())
        backoff = min(backoff * 2, 60.0)


def binance(cfg: Config, emit: Callable[[dict], None]):
    streams = "/".join(f"{s.lower()}@trade" for s in cfg.binance_symbols)
    url = f"{cfg.binance_ws}/stream?streams={streams}"
    return _run("binance", url, None, normalize.binance_trades, emit)


def kraken(cfg: Config, emit: Callable[[dict], None]):
    subscribe = {
        "method": "subscribe",
        "params": {"channel": "trade", "symbol": list(cfg.kraken_symbols), "snapshot": False},
    }
    return _run("kraken", cfg.kraken_ws, subscribe, normalize.kraken_trades, emit)


FEEDS = {"binance": binance, "kraken": kraken}
