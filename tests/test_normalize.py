from market_data_collector import normalize
from market_data_collector.normalize import binance_trades, kraken_trades, loads

BINANCE = (
    '{"stream":"btcusdt@trade","data":{"e":"trade","E":1700000000123,"s":"BTCUSDT",'
    '"t":12345,"p":"37000.10000000","q":"0.00100000","T":1700000000120,"m":true,"M":true}}'
)
KRAKEN = (
    '{"channel":"trade","type":"update","data":['
    '{"symbol":"BTC/USD","side":"buy","price":37000.1,"qty":0.001,"ord_type":"market",'
    '"trade_id":777,"timestamp":"2023-11-14T22:13:20.123456Z"},'
    '{"symbol":"DOGE/USD","side":"sell","price":0.07512,"qty":1500,"ord_type":"limit",'
    '"trade_id":778,"timestamp":"2023-11-14T22:13:21Z"}]}'
)


def test_binance_trade():
    (rec,) = binance_trades(loads(BINANCE), 1700000000200)
    assert rec == {
        "exchange": "binance", "symbol": "BTCUSDT", "base": "BTC", "quote": "USDT",
        "trade_id": "12345", "price": "37000.10000000", "qty": "0.00100000",
        "side": "sell", "ts": 1700000000120, "recv_ts": 1700000000200,
    }


def test_binance_taker_buy_when_buyer_is_not_maker():
    msg = loads(BINANCE)
    msg["data"]["m"] = False
    (rec,) = binance_trades(msg, 0)
    assert rec["side"] == "buy"


def test_binance_ignores_other_events():
    assert list(binance_trades({"result": None, "id": 1}, 0)) == []
    assert list(binance_trades({"data": {"e": "kline"}}, 0)) == []


def test_kraken_trades_keep_exact_decimals():
    a, b = kraken_trades(loads(KRAKEN), 5)
    assert (a["symbol"], a["base"], a["quote"]) == ("BTC/USD", "BTC", "USD")
    assert (a["price"], a["qty"], a["side"], a["trade_id"]) == ("37000.1", "0.001", "buy", "777")
    assert a["ts"] == 1700000000123
    # integer-looking qty and a timestamp without a fraction
    assert (b["qty"], b["side"], b["ts"]) == ("1500", "sell", 1700000001000)
    assert b["recv_ts"] == 5


def test_kraken_ignores_heartbeat_status_and_acks():
    for raw in (
        '{"channel":"heartbeat"}',
        '{"channel":"status","type":"update","data":[{"system":"online"}]}',
        '{"method":"subscribe","success":true,"result":{"channel":"trade","symbol":"BTC/USD"}}',
    ):
        assert list(kraken_trades(loads(raw), 0)) == []


def test_split_binance_symbol():
    assert normalize.split_binance_symbol("BTCUSDT") == ("BTC", "USDT")
    assert normalize.split_binance_symbol("BTCFDUSD") == ("BTC", "FDUSD")
    assert normalize.split_binance_symbol("ETHBTC") == ("ETH", "BTC")
    assert normalize.split_binance_symbol("WEIRD") == ("WEIRD", "")
