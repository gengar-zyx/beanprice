#!/usr/bin/env python3
"""Import Twelve Data stock symbols into Meilisearch."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path

import requests


DEFAULT_FILE = "stocks"
DEFAULT_MEILI_URL = "http://127.0.0.1:7700"
DEFAULT_INDEX = "beanprice_rapid_stocks"
DEFAULT_SOURCE = "beanprice.rapid"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_EXCHANGES = ("NASDAQ", "NYSE", "SSE", "SZSE")
DEFAULT_TYPES = ("Common Stock",)
DEFAULT_COUNTRIES = ("United States", "China")
TASK_WAIT_SECONDS = 60
TASK_POLL_INTERVAL = 0.2


class MeilisearchImportError(RuntimeError):
    """Raised when the import into Meilisearch fails."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Twelve Data stock symbols into a Meilisearch index."
    )
    parser.add_argument(
        "--file",
        default=DEFAULT_FILE,
        help="Path to the Twelve Data stocks JSON export",
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("MEILISEARCH_URL", DEFAULT_MEILI_URL),
        help="Meilisearch base URL",
    )
    parser.add_argument(
        "--index",
        default=os.environ.get("MEILISEARCH_INDEX", DEFAULT_INDEX),
        help="Meilisearch index uid",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("MEILISEARCH_API_KEY", ""),
        help="Meilisearch API key",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of documents per upload batch",
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help="Value written to the source field",
    )
    parser.add_argument(
        "--exchanges",
        nargs="+",
        default=list(DEFAULT_EXCHANGES),
        help="Exchanges to import",
    )
    parser.add_argument(
        "--countries",
        nargs="+",
        default=list(DEFAULT_COUNTRIES),
        help="Countries to import",
    )
    parser.add_argument(
        "--types",
        nargs="+",
        default=list(DEFAULT_TYPES),
        help="Security types to import",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse documents and print a summary without uploading",
    )
    return parser.parse_args()


def load_rows(path: Path) -> list[dict[str, str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError(f"Unexpected payload in {path}")
    return rows


def normalize_market(country: str, exchange: str) -> str:
    if country == "United States":
        return "US"
    if country == "China" and exchange in {"SSE", "SZSE"}:
        return "CN"
    return country


def make_document_id(market: str, exchange: str, symbol: str) -> str:
    safe_market = re.sub(r"[^0-9A-Za-z_-]", "_", market)
    safe_exchange = re.sub(r"[^0-9A-Za-z_-]", "_", exchange)
    safe_symbol = re.sub(r"[^0-9A-Za-z_-]", "_", symbol)
    digest = hashlib.sha1(
        f"{market}|{exchange}|{symbol}".encode("utf-8")
    ).hexdigest()[:12]
    return f"{safe_market}_{safe_exchange}_{safe_symbol}_{digest}"


def build_documents(
    rows: list[dict[str, str]],
    *,
    source_name: str,
    countries: set[str],
    exchanges: set[str],
    security_types: set[str],
) -> list[dict[str, str]]:
    documents = []
    for row in rows:
        country = row.get("country", "")
        exchange = row.get("exchange", "")
        security_type = row.get("type", "")
        symbol = row.get("symbol", "")
        name = row.get("name", "")

        if country not in countries:
            continue
        if exchange not in exchanges:
            continue
        if security_type not in security_types:
            continue
        if not symbol or not name:
            continue

        market = normalize_market(country, exchange)
        document_id = make_document_id(market, exchange, symbol)
        search_text = " ".join(
            part
            for part in [
                symbol,
                name,
                exchange,
                row.get("mic_code", ""),
                country,
                security_type,
                market,
                source_name,
            ]
            if part
        )
        documents.append(
            {
                "id": document_id,
                "symbol": symbol,
                "ticker": symbol,
                "name": name,
                "exchange": exchange,
                "mic_code": row.get("mic_code", ""),
                "country": country,
                "market": market,
                "currency": row.get("currency", ""),
                "security_type": security_type,
                "figi_code": row.get("figi_code", ""),
                "cfi_code": row.get("cfi_code", ""),
                "source": source_name,
                "search_text": search_text,
            }
        )
    documents.sort(key=lambda item: (item["market"], item["exchange"], item["symbol"]))
    return documents


class MeilisearchClient:
    def __init__(self, base_url: str, api_key: str = ""):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        if api_key:
            self.session.headers["Authorization"] = f"Bearer {api_key}"

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        response = self.session.request(method, f"{self.base_url}{path}", **kwargs)
        if response.status_code >= 400:
            raise MeilisearchImportError(
                f"{method} {path} failed: {response.status_code} {response.text}"
            )
        return response

    def health(self) -> dict:
        return self._request("GET", "/health").json()

    def ensure_index(self, uid: str, primary_key: str = "id") -> None:
        response = self.session.get(f"{self.base_url}/indexes/{uid}")
        if response.status_code == 404:
            task_uid = self._request(
                "POST", "/indexes", json={"uid": uid, "primaryKey": primary_key}
            ).json()["taskUid"]
            self.wait_for_task(task_uid)
            return
        if response.status_code >= 400:
            raise MeilisearchImportError(
                f"GET /indexes/{uid} failed: {response.status_code} {response.text}"
            )

    def update_settings(self, uid: str) -> None:
        settings = {
            "searchableAttributes": [
                "symbol",
                "ticker",
                "name",
                "exchange",
                "mic_code",
                "search_text",
            ],
            "filterableAttributes": [
                "source",
                "market",
                "country",
                "exchange",
                "currency",
                "security_type",
            ],
            "sortableAttributes": ["symbol", "exchange", "market"],
            "displayedAttributes": [
                "id",
                "symbol",
                "ticker",
                "name",
                "exchange",
                "mic_code",
                "country",
                "market",
                "currency",
                "security_type",
                "figi_code",
                "cfi_code",
                "source",
            ],
            "rankingRules": [
                "words",
                "typo",
                "proximity",
                "attribute",
                "sort",
                "exactness",
            ],
        }
        task_uid = self._request(
            "PATCH", f"/indexes/{uid}/settings", json=settings
        ).json()["taskUid"]
        self.wait_for_task(task_uid)

    def add_documents(self, uid: str, documents: list[dict[str, str]]) -> None:
        task_uid = self._request(
            "POST", f"/indexes/{uid}/documents", json=documents
        ).json()["taskUid"]
        self.wait_for_task(task_uid)

    def wait_for_task(
        self, task_uid: int, timeout_seconds: int = TASK_WAIT_SECONDS
    ) -> dict:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            task = self._request("GET", f"/tasks/{task_uid}").json()
            status = task.get("status")
            if status == "succeeded":
                return task
            if status == "failed":
                raise MeilisearchImportError(
                    f"Task {task_uid} failed: {task.get('error')}"
                )
            time.sleep(TASK_POLL_INTERVAL)
        raise MeilisearchImportError(f"Timed out waiting for task {task_uid}")


def chunked(items: list[dict[str, str]], size: int):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def summarize_documents(documents: list[dict[str, str]]) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {"market": {}, "exchange": {}}
    for document in documents:
        market = document["market"]
        exchange = document["exchange"]
        summary["market"][market] = summary["market"].get(market, 0) + 1
        summary["exchange"][exchange] = summary["exchange"].get(exchange, 0) + 1
    return summary


def main() -> int:
    args = parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be > 0")

    path = Path(args.file)
    rows = load_rows(path)
    documents = build_documents(
        rows,
        source_name=args.source,
        countries=set(args.countries),
        exchanges=set(args.exchanges),
        security_types=set(args.types),
    )

    print(f"Loaded {len(rows)} rows from {path}")
    print(f"Selected {len(documents)} rows for index {args.index}")
    print(
        "Filters: "
        f"countries={sorted(set(args.countries))} "
        f"exchanges={sorted(set(args.exchanges))} "
        f"types={sorted(set(args.types))}"
    )
    print(f"Summary: {json.dumps(summarize_documents(documents), ensure_ascii=False)}")
    if documents:
        print(f"Sample: {json.dumps(documents[0], ensure_ascii=False)}")

    if args.dry_run:
        return 0

    client = MeilisearchClient(args.url, args.api_key)
    health = client.health()
    print(f"Meilisearch health: {json.dumps(health, ensure_ascii=False)}")

    client.ensure_index(args.index)
    client.update_settings(args.index)

    uploaded = 0
    for batch in chunked(documents, args.batch_size):
        client.add_documents(args.index, batch)
        uploaded += len(batch)
        print(f"Uploaded {uploaded}/{len(documents)}")

    print(f"Imported {uploaded} stock symbols into {args.index}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (
        MeilisearchImportError,
        OSError,
        ValueError,
        requests.RequestException,
    ) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
