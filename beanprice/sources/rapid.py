"""A source fetching stock prices from RapidAPI's Twelve Data endpoints.

This source supports bare stock symbols such as ``AAPL`` and uses the
``time_series`` endpoint with ``interval=1day`` for both latest and historical
prices.

It requires an API key in the ``RAPID_API_KEY`` environment variable.
"""

from __future__ import annotations

import datetime
from decimal import Decimal
import os
import re
from typing import Any, Optional

from dateutil import parser, tz
import requests

from beanprice import source


API_URL = "https://twelve-data1.p.rapidapi.com/time_series"
API_HOST = "twelve-data1.p.rapidapi.com"
DEFAULT_LOOKBACK_DAYS = 10


class RapidApiError(ValueError):
    "An error from the RapidAPI Twelve Data API."


def _parse_ticker(ticker: str) -> str:
    match = re.match(r"^[A-Za-z0-9][A-Za-z0-9.\-]*$", ticker)
    if not match:
        raise ValueError('Invalid ticker. Use a bare stock symbol such as "AAPL".')
    return ticker.upper()


def _get_api_key() -> str:
    api_key = os.environ.get("RAPID_API_KEY")
    if not api_key:
        raise RapidApiError("RAPID_API_KEY environment variable is not set")
    return api_key


def _build_headers() -> dict[str, str]:
    return {
        "x-rapidapi-key": _get_api_key(),
        "x-rapidapi-host": API_HOST,
    }


def _parse_price_time(value: str, timezone_name: str) -> datetime.datetime:
    parsed = parser.isoparse(value)
    timezone = tz.gettz(timezone_name) or datetime.timezone.utc
    if parsed.tzinfo is None:
        if len(value) == 10:
            # Daily bars only include the trading date; represent them as the
            # close of a typical US trading day in the exchange timezone.
            parsed = datetime.datetime.combine(parsed.date(), datetime.time(16, 0))
        parsed = parsed.replace(tzinfo=timezone)
    return parsed


def _parse_payload(payload: dict[str, Any]) -> tuple[list[source.SourcePrice], Optional[str]]:
    status = payload.get("status")
    if isinstance(status, str) and status.lower() == "error":
        raise RapidApiError(payload.get("message", "Unknown error from Rapid API"))
    if "code" in payload and "message" in payload and not payload.get("values"):
        raise RapidApiError(payload.get("message", "Unknown error from Rapid API"))

    meta = payload.get("meta")
    if not isinstance(meta, dict):
        raise RapidApiError("Invalid response from Rapid API: missing meta")

    timezone_name = meta.get("exchange_timezone") or "UTC"
    quote_currency = meta.get("currency")
    values = payload.get("values")
    if not isinstance(values, list) or not values:
        raise RapidApiError("No data returned from Rapid API")

    prices = []
    for item in values:
        if not isinstance(item, dict):
            continue
        try:
            trade_time = _parse_price_time(item["datetime"], timezone_name)
            close_price = Decimal(item["close"])
        except (KeyError, ValueError, ArithmeticError) as exc:
            raise RapidApiError(
                f"Invalid response from Rapid API: {item!r}"
            ) from exc
        prices.append(source.SourcePrice(close_price, trade_time, quote_currency))

    if not prices:
        raise RapidApiError("No valid price points returned from Rapid API")

    prices.sort(key=lambda item: item.time or datetime.datetime.min.replace(tzinfo=tz.tzutc()))
    return prices, quote_currency


def _request_time_series(
    symbol: str,
    *,
    outputsize: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> list[source.SourcePrice]:
    params: dict[str, Any] = {
        "symbol": symbol,
        "interval": "1day",
        "outputsize": outputsize,
        "format": "json",
    }
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    response = requests.get(API_URL, params=params, headers=_build_headers())
    if response.status_code != requests.codes.ok:
        raise RapidApiError(
            "Invalid response ({}): {}".format(response.status_code, response.text)
        )

    payload = response.json()
    prices, _ = _parse_payload(payload)
    return prices


class Source(source.Source):
    def get_latest_price(self, ticker: str) -> Optional[source.SourcePrice]:
        symbol = _parse_ticker(ticker)
        return _request_time_series(symbol, outputsize=1)[-1]

    def get_historical_price(
        self, ticker: str, time: datetime.datetime
    ) -> Optional[source.SourcePrice]:
        symbol = _parse_ticker(ticker)
        end_date = time.date().isoformat()
        start_date = (time.date() - datetime.timedelta(days=DEFAULT_LOOKBACK_DAYS)).isoformat()
        series = _request_time_series(
            symbol,
            outputsize=DEFAULT_LOOKBACK_DAYS + 5,
            start_date=start_date,
            end_date=end_date,
        )

        latest = None
        for datapoint in series:
            if datapoint.time is not None and datapoint.time <= time:
                latest = datapoint
        if latest is None:
            raise RapidApiError(f"Could not find price before {time.isoformat()} for {symbol}")
        return latest

    def get_prices_series(
        self, ticker: str, time_begin: datetime.datetime, time_end: datetime.datetime
    ) -> list[source.SourcePrice]:
        symbol = _parse_ticker(ticker)
        return _request_time_series(
            symbol,
            outputsize=max((time_end.date() - time_begin.date()).days + 5, 5),
            start_date=time_begin.date().isoformat(),
            end_date=time_end.date().isoformat(),
        )
