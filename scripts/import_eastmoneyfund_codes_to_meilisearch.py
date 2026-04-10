#!/usr/bin/env python3
"""Import EastMoney fund codes into Meilisearch."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

import requests


DEFAULT_MEILI_URL = "http://127.0.0.1:7700"
DEFAULT_INDEX = "beanprice_eastmoneyfund"
DEFAULT_SOURCE = "beanprice.eastmoneyfund"
DEFAULT_BATCH_SIZE = 1000
TASK_WAIT_SECONDS = 60
TASK_POLL_INTERVAL = 0.2


class MeilisearchImportError(RuntimeError):
    """Raised when the import into Meilisearch fails."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import fundcode_search.js into a Meilisearch index."
    )
    parser.add_argument(
        "--file",
        default="fundcode_search.js",
        help="Path to EastMoney fundcode_search.js",
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


def load_fund_rows(path: Path) -> list[list[str]]:
    raw = path.read_text(encoding="utf-8-sig")
    match = re.search(r"var\s+r\s*=\s*(\[.*\])\s*;?\s*$", raw, re.DOTALL)
    if not match:
        raise ValueError(f"Unable to extract fund list from {path}")
    rows = json.loads(match.group(1))
    if not isinstance(rows, list):
        raise ValueError(f"Unexpected payload type in {path}")
    return rows


def build_documents(rows: list[list[str]], source_name: str) -> list[dict[str, str]]:
    documents = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 5:
            raise ValueError(f"Unexpected fund row: {row!r}")
        code, abbr, name, fund_type, pinyin = row[:5]
        search_text = " ".join(
            part for part in [code, name, abbr, pinyin, fund_type, source_name] if part
        )
        documents.append(
            {
                "code": code,
                "ticker": code,
                "name": name,
                "abbr": abbr,
                "fund_type": fund_type,
                "pinyin": pinyin,
                "quote_currency": "CNY",
                "source": source_name,
                "search_text": search_text,
            }
        )
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

    def ensure_index(self, uid: str, primary_key: str = "code") -> None:
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
                "code",
                "ticker",
                "name",
                "abbr",
                "pinyin",
                "fund_type",
                "search_text",
            ],
            "filterableAttributes": ["source", "quote_currency", "fund_type"],
            "sortableAttributes": ["code"],
            "displayedAttributes": [
                "code",
                "ticker",
                "name",
                "abbr",
                "fund_type",
                "pinyin",
                "quote_currency",
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

    def add_documents(self, uid: str, documents: list[dict[str, str]]) -> int:
        task_uid = self._request(
            "POST", f"/indexes/{uid}/documents", json=documents
        ).json()["taskUid"]
        self.wait_for_task(task_uid)
        return task_uid

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


def main() -> int:
    args = parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be > 0")

    path = Path(args.file)
    rows = load_fund_rows(path)
    documents = build_documents(rows, args.source)

    print(f"Loaded {len(documents)} funds from {path}")
    print(f"Index: {args.index}")
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

    print(f"Imported {uploaded} funds into {args.index}")
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
