#!/usr/bin/env python3
"""Import Twelve Data forex pairs into Meilisearch."""

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


DEFAULT_DATA_URL = "https://api.twelvedata.com/forex_pairs"
DEFAULT_MEILI_URL = "http://127.0.0.1:7700"
DEFAULT_INDEX = "twelvedata_forex_pairs"
DEFAULT_SOURCE = "twelvedata.forex_pairs"
DEFAULT_BATCH_SIZE = 1000
TASK_WAIT_SECONDS = 60
TASK_POLL_INTERVAL = 0.2


class MeilisearchImportError(RuntimeError):
    """Raised when the import into Meilisearch fails."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Twelve Data forex pairs into a Meilisearch index."
    )
    parser.add_argument(
        "--file",
        help="Read Twelve Data forex pairs from a local JSON file instead of the API",
    )
    parser.add_argument(
        "--data-url",
        default=DEFAULT_DATA_URL,
        help="Twelve Data forex_pairs endpoint",
    )
    parser.add_argument(
        "--twelvedata-api-key",
        default=os.environ.get("TWELVE_DATA_API_KEY", ""),
        help="Optional Twelve Data API key",
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
        "--dry-run",
        action="store_true",
        help="Parse documents and print a summary without uploading",
    )
    return parser.parse_args()


def fetch_rows(data_url: str, api_key: str) -> list[dict[str, str]]:
    params = {}
    if api_key:
        params["apikey"] = api_key
    response = requests.get(data_url, params=params, timeout=30)
    if response.status_code != requests.codes.ok:
        raise MeilisearchImportError(
            f"GET {data_url} failed: {response.status_code} {response.text}"
        )
    payload = response.json()
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError(f"Unexpected payload from {data_url}: {payload!r}")
    return rows


def load_rows_from_file(path: Path) -> list[dict[str, str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError(f"Unexpected payload in {path}")
    return rows


def make_document_id(symbol: str) -> str:
    safe_symbol = re.sub(r"[^0-9A-Za-z_-]", "_", symbol)
    digest = hashlib.sha1(symbol.encode("utf-8")).hexdigest()[:12]
    return f"FX_{safe_symbol}_{digest}"


def split_symbol(symbol: str) -> tuple[str, str]:
    if "/" not in symbol:
        return symbol, ""
    base_code, quote_code = symbol.split("/", 1)
    return base_code, quote_code


def build_documents(
    rows: list[dict[str, str]], source_name: str
) -> list[dict[str, str]]:
    documents = []
    for row in rows:
        symbol = row.get("symbol", "")
        currency_group = row.get("currency_group", "")
        currency_base = row.get("currency_base", "")
        currency_quote = row.get("currency_quote", "")
        if not symbol or not currency_base or not currency_quote:
            continue

        base_code, quote_code = split_symbol(symbol)
        display_name = f"{currency_base} / {currency_quote}"
        search_text = " ".join(
            part
            for part in [
                symbol,
                base_code,
                quote_code,
                currency_base,
                currency_quote,
                currency_group,
                display_name,
                source_name,
            ]
            if part
        )
        documents.append(
            {
                "id": make_document_id(symbol),
                "symbol": symbol,
                "ticker": symbol,
                "display_name": display_name,
                "base_code": base_code,
                "quote_code": quote_code,
                "currency_base": currency_base,
                "currency_quote": currency_quote,
                "currency_group": currency_group,
                "asset_class": "forex",
                "source": source_name,
                "search_text": search_text,
            }
        )
    documents.sort(key=lambda item: item["symbol"])
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
                "display_name",
                "base_code",
                "quote_code",
                "currency_base",
                "currency_quote",
                "currency_group",
                "search_text",
            ],
            "filterableAttributes": [
                "source",
                "asset_class",
                "currency_group",
                "base_code",
                "quote_code",
            ],
            "sortableAttributes": ["symbol", "base_code", "quote_code"],
            "displayedAttributes": [
                "id",
                "symbol",
                "ticker",
                "display_name",
                "base_code",
                "quote_code",
                "currency_base",
                "currency_quote",
                "currency_group",
                "asset_class",
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


def summarize_documents(documents: list[dict[str, str]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for document in documents:
        group = document["currency_group"] or "unknown"
        summary[group] = summary.get(group, 0) + 1
    return summary


def main() -> int:
    args = parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be > 0")

    if args.file:
        path = Path(args.file)
        rows = load_rows_from_file(path)
        origin = str(path)
    else:
        rows = fetch_rows(args.data_url, args.twelvedata_api_key)
        origin = args.data_url

    documents = build_documents(rows, args.source)

    print(f"Loaded {len(rows)} rows from {origin}")
    print(f"Selected {len(documents)} rows for index {args.index}")
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

    print(f"Imported {uploaded} forex pairs into {args.index}")
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
