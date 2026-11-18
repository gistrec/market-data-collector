"""Entry point: one task per exchange, one shared sink."""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from . import config, feeds
from .sink import KafkaSink, StdoutSink

log = logging.getLogger("market_data_collector")

STATS_EVERY_S = 60


async def _housekeeping(sink) -> None:
    last: dict[str, int] = {}
    ticks = 0
    while True:
        await asyncio.sleep(0.2)
        sink.poll()  # serve Kafka delivery callbacks
        ticks += 1
        if ticks % (STATS_EVERY_S * 5) == 0:
            now = dict(sink.sent)
            delta = {t: n - last.get(t, 0) for t, n in sorted(now.items())}
            log.info("last %ds: %s failed_total=%d", STATS_EVERY_S, delta or "no trades",
                     getattr(sink, "failed", 0))
            last = now


async def run(args: argparse.Namespace) -> None:
    cfg = config.load()
    unknown = set(cfg.exchanges) - set(feeds.FEEDS)
    if unknown:
        raise SystemExit(f"unknown exchanges: {sorted(unknown)}")

    sink = StdoutSink() if args.stdout else KafkaSink(cfg)
    log.info("exchanges=%s bases=%s sink=%s", ",".join(cfg.exchanges), ",".join(cfg.bases),
             "stdout" if args.stdout else cfg.kafka_bootstrap)

    tasks = [asyncio.create_task(_housekeeping(sink))]
    for name in cfg.exchanges:
        topic = cfg.topic(name)
        tasks.append(asyncio.create_task(
            feeds.FEEDS[name](cfg, lambda record, topic=topic: sink.send(topic, record))))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    stopper = asyncio.create_task(stop.wait())
    timeout = args.seconds or None
    done, _ = await asyncio.wait([stopper, *tasks], timeout=timeout,
                                 return_when=asyncio.FIRST_COMPLETED)
    crashed = [t for t in done if t is not stopper and t.exception() is not None]
    for t in (stopper, *tasks):
        t.cancel()
    await asyncio.gather(stopper, *tasks, return_exceptions=True)
    sink.close()
    log.info("stopped: sent=%s", dict(sink.sent))
    if crashed:
        raise crashed[0].exception()


def main() -> None:
    parser = argparse.ArgumentParser(prog="market-data-collector", description=__doc__)
    parser.add_argument("--stdout", action="store_true",
                        help="print records instead of producing to Kafka (dry run)")
    parser.add_argument("--seconds", type=float, default=0,
                        help="stop after this many seconds (0 = run forever)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
