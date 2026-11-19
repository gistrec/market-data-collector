import os

from market_data_collector.main import touch_markers


def test_marker_mtime_is_last_ack(tmp_path):
    d = tmp_path / "m"
    touched: dict[str, float] = {}
    touch_markers(str(d), {"marketdata.binance.trades": 1_700_000_000.5}, touched)
    path = d / "marketdata.binance.trades.last-delivery"
    assert os.stat(path).st_mtime == 1_700_000_000.5

    # no new ack -> file left alone
    os.utime(path, (1, 1))
    touch_markers(str(d), {"marketdata.binance.trades": 1_700_000_000.5}, touched)
    assert os.stat(path).st_mtime == 1
