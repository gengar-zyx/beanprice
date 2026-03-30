import datetime
import os
import unittest
from decimal import Decimal
from unittest import mock

import requests
from dateutil import tz

from beanprice import source
from beanprice.sources import rapid


def response(content, status_code=requests.codes.ok, text=""):
    mocked = mock.Mock()
    mocked.status_code = status_code
    mocked.text = text
    mocked.json.return_value = content
    return mock.patch("requests.get", return_value=mocked)


LATEST_RESPONSE = {
    "meta": {
        "symbol": "AAPL",
        "interval": "1day",
        "currency": "USD",
        "exchange_timezone": "America/New_York",
    },
    "values": [
        {
            "datetime": "2025-03-28",
            "open": "221.31",
            "high": "223.50",
            "low": "220.88",
            "close": "222.13",
            "volume": "39876543",
        }
    ],
    "status": "ok",
}


HISTORICAL_RESPONSE = {
    "meta": {
        "symbol": "AAPL",
        "interval": "1day",
        "currency": "USD",
        "exchange_timezone": "America/New_York",
    },
    "values": [
        {
            "datetime": "2025-03-28",
            "close": "222.13",
        },
        {
            "datetime": "2025-03-27",
            "close": "219.80",
        },
        {
            "datetime": "2025-03-26",
            "close": "221.12",
        },
    ],
    "status": "ok",
}


SERIES_RESPONSE = {
    "meta": {
        "symbol": "AAPL",
        "interval": "1day",
        "currency": "USD",
        "exchange_timezone": "America/New_York",
    },
    "values": [
        {
            "datetime": "2025-03-28",
            "close": "222.13",
        },
        {
            "datetime": "2025-03-26",
            "close": "221.12",
        },
        {
            "datetime": "2025-03-27",
            "close": "219.80",
        },
    ],
    "status": "ok",
}


class RapidPriceFetcher(unittest.TestCase):
    def test_error_invalid_ticker(self):
        with self.assertRaises(ValueError):
            rapid.Source().get_latest_price("NASDAQ:AAPL")

    def test_error_missing_api_key(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(rapid.RapidApiError):
                rapid.Source().get_latest_price("AAPL")

    def test_get_latest_price(self):
        timezone = tz.gettz("America/New_York")
        with mock.patch.dict(os.environ, {"RAPID_API_KEY": "test-key"}, clear=True):
            with response(LATEST_RESPONSE):
                srcprice = rapid.Source().get_latest_price("AAPL")
        self.assertIsInstance(srcprice, source.SourcePrice)
        self.assertEqual(Decimal("222.13"), srcprice.price)
        self.assertEqual("USD", srcprice.quote_currency)
        self.assertEqual(
            datetime.datetime(2025, 3, 28, 16, 0, tzinfo=timezone),
            srcprice.time,
        )

    def test_get_historical_price_uses_latest_trading_day(self):
        request_time = datetime.datetime(2025, 3, 30, 16, 0, tzinfo=tz.tzutc())
        timezone = tz.gettz("America/New_York")
        with mock.patch.dict(os.environ, {"RAPID_API_KEY": "test-key"}, clear=True):
            with response(HISTORICAL_RESPONSE):
                srcprice = rapid.Source().get_historical_price("AAPL", request_time)
        self.assertIsInstance(srcprice, source.SourcePrice)
        self.assertEqual(Decimal("222.13"), srcprice.price)
        self.assertEqual("USD", srcprice.quote_currency)
        self.assertEqual(
            datetime.datetime(2025, 3, 28, 16, 0, tzinfo=timezone),
            srcprice.time,
        )

    def test_get_prices_series_returns_sorted_values(self):
        with mock.patch.dict(os.environ, {"RAPID_API_KEY": "test-key"}, clear=True):
            with response(SERIES_RESPONSE):
                prices = rapid.Source().get_prices_series(
                    "AAPL",
                    datetime.datetime(2025, 3, 25, 0, 0, tzinfo=tz.tzutc()),
                    datetime.datetime(2025, 3, 28, 0, 0, tzinfo=tz.tzutc()),
                )
        self.assertEqual(3, len(prices))
        self.assertEqual(Decimal("221.12"), prices[0].price)
        self.assertEqual(Decimal("219.80"), prices[1].price)
        self.assertEqual(Decimal("222.13"), prices[2].price)


if __name__ == "__main__":
    unittest.main()
