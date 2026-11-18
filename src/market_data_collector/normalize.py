"""Exchange payloads → one flat trade record shared by every topic.

Prices and quantities stay strings end to end: both exchanges are parsed with
``parse_float=str`` so nothing passes through a binary float on the way.
"""
from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

# Longest first, so BTCUSDT splits as BTC/USDT rather than BTCUSD/T.
_BINANCE_QUOTES = ("FDUSD", "USDT", "USDC", "TUSD", "BUSD", "BTC", "ETH", "BNB", "EUR", "TRY")


def loads(raw: str | bytes) -> Any:
    return json.loads(raw, parse_float=str)


def split_binance_symbol(symbol: str) -> tuple[str, str]:
    for quote in _BINANCE_QUOTES:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)], quote
    return symbol, ""


def _iso_to_ms(ts: str) -> int:
    # Kraken sends RFC 3339 with a trailing Z and up to nanosecond precision;
    # fromisoformat on 3.10 takes neither.
    head, _, frac = ts.rstrip("Z").partition(".")
    seconds = datetime.fromisoformat(head).replace(tzinfo=timezone.utc).timestamp()
    return int(seconds) * 1000 + int((frac + "000")[:3])


def binance_trades(msg: Any, recv_ms: int) -> Iterator[dict]:
    """Combined-stream envelope ``{"stream": ..., "data": {...}}`` or a bare event."""
    data = msg.get("data", msg) if isinstance(msg, dict) else None
    if not isinstance(data, dict) or data.get("e") != "trade":
        return
    symbol = data["s"]
    base, quote = split_binance_symbol(symbol)
    yield {
        "exchange": "binance",
        "symbol": symbol,
        "base": base,
        "quote": quote,
        "trade_id": str(data["t"]),
        "price": str(data["p"]),
        "qty": str(data["q"]),
        # m = the buyer is the maker, so the aggressor (taker) sold.
        "side": "sell" if data["m"] else "buy",
        "ts": int(data["T"]),
        "recv_ts": recv_ms,
    }


def kraken_trades(msg: Any, recv_ms: int) -> Iterator[dict]:
    """WebSocket v2 ``trade`` channel; heartbeats, status and acks yield nothing."""
    if not isinstance(msg, dict) or msg.get("channel") != "trade":
        return
    for t in msg.get("data") or ():
        symbol = t["symbol"]
        base, _, quote = symbol.partition("/")
        yield {
            "exchange": "kraken",
            "symbol": symbol,
            "base": base,
            "quote": quote,
            "trade_id": str(t["trade_id"]),
            "price": str(t["price"]),
            "qty": str(t["qty"]),
            "side": t["side"],
            "ts": _iso_to_ms(t["timestamp"]),
            "recv_ts": recv_ms,
        }
