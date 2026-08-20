"""Fetch ENTSOG connection points and optionally upsert them to MongoDB."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import sys
import time
from typing import Any, Dict, Iterable, List, Optional

import requests
from requests import RequestException
from pymongo import UpdateOne

from modern_pipeline.db import get_database

ENTSOG_CONNECTIONPOINTS_CSV = "https://transparency.entsog.eu/api/v1/connectionpoints.csv"
REQUEST_TIMEOUT = 45
MIN_REQUEST_INTERVAL_SEC = 10.5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
BOOL_TRUE_VALUES = {"1", "true", "True", "TRUE", "yes", "Yes", "YES"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the initial ENTSOG connection_points catalog."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and summarize candidates without writing to MongoDB. This is the default.",
    )
    mode.add_argument(
        "--write",
        action="store_true",
        help="Upsert fetched candidates into MongoDB.",
    )
    parser.add_argument(
        "--eu-crossing",
        default="EUNONEU",
        help="ENTSOG euCrossing filter to request and keep. Default: EUNONEU.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Fetch a single page with this many rows. Useful for quick tests. "
            "If omitted, fetches all pages using --page-size."
        ),
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=25,
        help="Rows per paginated ENTSOG request when --limit is omitted. Default: 25.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of pages to fetch in paginated mode. Useful for dry-run tests.",
    )
    parser.add_argument(
        "--collection",
        default="entsog_connection_points",
        help="MongoDB collection to write. Default: entsog_connection_points.",
    )
    parser.add_argument(
        "--show-excluded",
        type=int,
        default=5,
        help="Number of fetched-but-excluded rows to show in dry-run diagnostics. Default: 5.",
    )
    parser.add_argument(
        "--require-connectionpoint-has-data",
        action="store_true",
        help=(
            "Require connectionpoints.csv hasData=true. Off by default because "
            "operatorPointDirections is a better has-data source for import points."
        ),
    )
    return parser.parse_args()


def as_bool(value: Any) -> Optional[bool]:
    if value in (None, ""):
        return None
    return str(value).strip() in BOOL_TRUE_VALUES


def clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def get_csv_with_retries(url: str, params: Dict[str, Any]) -> Optional[str]:
    session = requests.Session()
    for attempt in range(1, 6):
        if attempt > 1:
            time.sleep(MIN_REQUEST_INTERVAL_SEC)
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except RequestException as exc:
            if attempt >= 5:
                raise RuntimeError(f"ENTSOG request failed after retries: {exc}") from exc
            print(
                f"ENTSOG request failed: {exc}. Waiting {MIN_REQUEST_INTERVAL_SEC:.1f}s "
                f"(attempt {attempt}/5).",
                file=sys.stderr,
            )
            time.sleep(MIN_REQUEST_INTERVAL_SEC)
            continue
        if response.status_code in RETRY_STATUS_CODES and attempt < 5:
            retry_after = response.headers.get("Retry-After")
            wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else MIN_REQUEST_INTERVAL_SEC
            print(
                f"Retryable ENTSOG response {response.status_code}; waiting {wait_seconds:.1f}s "
                f"(attempt {attempt}/5).",
                file=sys.stderr,
            )
            time.sleep(wait_seconds)
            continue
        response.raise_for_status()
        if response.text.lstrip().startswith("{"):
            if "No result found" in response.text:
                return None
            raise RuntimeError(f"ENTSOG returned JSON instead of CSV: {response.text[:300]}")
        if response.text.lstrip().startswith("<"):
            raise RuntimeError("ENTSOG returned HTML instead of CSV; try a smaller --limit or rerun later.")
        return response.text
    raise RuntimeError("Failed to fetch ENTSOG connection points after retries.")


def parse_connection_points_csv(csv_text: Optional[str]) -> List[Dict[str, str]]:
    if not csv_text:
        return []
    return list(csv.DictReader(io.StringIO(csv_text)))


def fetch_connection_points_page(eu_crossing: str, limit: int, offset: int) -> List[Dict[str, str]]:
    params: Dict[str, Any] = {
        "euCrossing": eu_crossing,
        "limit": limit,
        "offset": offset,
    }
    csv_text = get_csv_with_retries(ENTSOG_CONNECTIONPOINTS_CSV, params)
    return parse_connection_points_csv(csv_text)


def fetch_connection_points_single_page(eu_crossing: str, limit: int) -> List[Dict[str, str]]:
    return fetch_connection_points_page(eu_crossing=eu_crossing, limit=limit, offset=0)


def fetch_connection_points_paginated(
    eu_crossing: str,
    page_size: int,
    max_pages: Optional[int],
) -> List[Dict[str, str]]:
    if page_size < 1:
        raise ValueError("--page-size must be >= 1")

    all_rows: List[Dict[str, str]] = []
    page = 0
    while True:
        if max_pages is not None and page >= max_pages:
            break

        offset = page * page_size
        print(f"Fetching ENTSOG connection points page {page + 1} (limit={page_size}, offset={offset})...")
        page_rows = fetch_connection_points_page(eu_crossing=eu_crossing, limit=page_size, offset=offset)
        print(f"  fetched {len(page_rows)} rows")

        if not page_rows:
            break

        all_rows.extend(page_rows)

        if len(page_rows) < page_size:
            break

        page += 1
        time.sleep(MIN_REQUEST_INTERVAL_SEC)

    return all_rows


def normalize_eu_crossing(raw_value: Any, requested_eu_crossing: str) -> Optional[str]:
    text = clean_str(raw_value)
    if text in BOOL_TRUE_VALUES:
        return requested_eu_crossing
    return text


def normalize_connection_point(
    row: Dict[str, str],
    catalog_version: str,
    fetched_at: dt.datetime,
    requested_eu_crossing: str,
) -> Dict[str, Any]:
    eu_crossing_raw = clean_str(row.get("euCrossing"))
    return {
        "pointKey": clean_str(row.get("pointKey")),
        "pointLabel": clean_str(row.get("pointLabel")),
        "pointEicCode": clean_str(row.get("pointEicCode")),
        "controlPointType": clean_str(row.get("controlPointType")),
        "commercialType": clean_str(row.get("commercialType")),
        "importFromCountryKey": clean_str(row.get("importFromCountryKey")),
        "importFromCountryLabel": clean_str(row.get("importFromCountryLabel")),
        "hasData": as_bool(row.get("hasData")),
        "isPlanned": as_bool(row.get("isPlanned")),
        "isInterconnection": as_bool(row.get("isInterconnection")),
        "isImport": as_bool(row.get("isImport")),
        "infrastructureKey": clean_str(row.get("infrastructureKey")),
        "infrastructureLabel": clean_str(row.get("infrastructureLabel")),
        "isCrossBorder": as_bool(row.get("isCrossBorder")),
        "euCrossing": normalize_eu_crossing(eu_crossing_raw, requested_eu_crossing),
        "euCrossingRaw": eu_crossing_raw,
        "isInvalid": as_bool(row.get("isInvalid")),
        "isMacroPoint": as_bool(row.get("isMacroPoint")),
        "isCAMRelevant": as_bool(row.get("isCAMRelevant")),
        "isPipeInPipe": as_bool(row.get("isPipeInPipe")),
        "isCMPRelevant": as_bool(row.get("isCMPRelevant")),
        "entsogId": clean_str(row.get("id")),
        "dataSet": clean_str(row.get("dataSet")),
        "catalogStatus": "candidate",
        "catalogVersion": catalog_version,
        "fetchedAt": fetched_at,
        "source": {
            "provider": "ENTSOG",
            "endpoint": ENTSOG_CONNECTIONPOINTS_CSV,
        },
    }


def exclusion_reasons(doc: Dict[str, Any], eu_crossing: str, require_has_data: bool) -> List[str]:
    reasons: List[str] = []
    is_interconnection_like = (
        doc.get("isInterconnection") is True
        or doc.get("infrastructureKey") == "ITP"
        or doc.get("infrastructureLabel") == "ITP"
        or str(doc.get("pointKey") or "").startswith("ITP-")
    )
    if doc.get("pointKey") is None:
        reasons.append("missing_pointKey")
    if doc.get("euCrossing") != eu_crossing:
        reasons.append("euCrossing")
    if require_has_data and doc.get("hasData") is not True:
        reasons.append("hasData")
    if not is_interconnection_like:
        reasons.append("isInterconnection")
    if doc.get("isImport") is not True:
        reasons.append("isImport")
    if doc.get("isInvalid") is True:
        reasons.append("isInvalid")
    return reasons


def candidate_filter(doc: Dict[str, Any], eu_crossing: str, require_has_data: bool) -> bool:
    return not exclusion_reasons(doc, eu_crossing, require_has_data)


def summarize(docs: Iterable[Dict[str, Any]]) -> None:
    docs = list(docs)
    print(f"Candidate connection points: {len(docs)}")

    by_source: Dict[str, int] = {}
    by_infra: Dict[str, int] = {}
    for doc in docs:
        by_source[doc.get("importFromCountryLabel") or "Unknown"] = by_source.get(doc.get("importFromCountryLabel") or "Unknown", 0) + 1
        by_infra[doc.get("infrastructureLabel") or "Unknown"] = by_infra.get(doc.get("infrastructureLabel") or "Unknown", 0) + 1

    print("\nBy importFromCountryLabel:")
    for key, count in sorted(by_source.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nBy infrastructureLabel:")
    for key, count in sorted(by_infra.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nSample:")
    for doc in docs[:10]:
        print(
            "  "
            f"{doc['pointKey']}: {doc.get('pointLabel')} "
            f"from {doc.get('importFromCountryLabel')} "
            f"({doc.get('infrastructureLabel')})"
        )


def summarize_filters(
    all_docs: List[Dict[str, Any]],
    eu_crossing: str,
    show_excluded: int,
    require_has_data: bool,
) -> None:
    reason_counts: Dict[str, int] = {}
    excluded_examples: List[Dict[str, Any]] = []

    for doc in all_docs:
        reasons = exclusion_reasons(doc, eu_crossing, require_has_data)
        if not reasons:
            continue
        for reason in reasons:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        if len(excluded_examples) < show_excluded:
            excluded_examples.append(
                {
                    "pointKey": doc.get("pointKey"),
                    "pointLabel": doc.get("pointLabel"),
                    "euCrossing": doc.get("euCrossing"),
                    "euCrossingRaw": doc.get("euCrossingRaw"),
                    "hasData": doc.get("hasData"),
                    "isInterconnection": doc.get("isInterconnection"),
                    "isImport": doc.get("isImport"),
                    "isInvalid": doc.get("isInvalid"),
                    "reasons": reasons,
                }
            )

    print(f"\nFetched rows: {len(all_docs)}")
    print("Excluded-row reason counts:")
    if not reason_counts:
        print("  none")
    for reason, count in sorted(reason_counts.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {reason}: {count}")

    if excluded_examples:
        print("\nExcluded examples:")
        for example in excluded_examples:
            print(f"  {example}")


def upsert_connection_points(docs: List[Dict[str, Any]], collection_name: str) -> Dict[str, int]:
    db = get_database()
    collection = db[collection_name]
    collection.create_index("pointKey", unique=True)
    collection.create_index([("euCrossing", 1), ("catalogStatus", 1)])
    collection.create_index("importFromCountryKey")

    operations = [
        UpdateOne(
            {"pointKey": doc["pointKey"]},
            {
                "$set": doc,
                "$setOnInsert": {
                    "createdAt": dt.datetime.now(dt.timezone.utc),
                    "pointDirections": [],
                    "interconnections": [],
                },
            },
            upsert=True,
        )
        for doc in docs
    ]
    if not operations:
        return {"matched": 0, "modified": 0, "upserted": 0}

    result = collection.bulk_write(operations, ordered=False)
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "upserted": len(result.upserted_ids),
    }


def main() -> None:
    args = parse_args()
    dry_run = not args.write
    fetched_at = dt.datetime.now(dt.timezone.utc)
    catalog_version = fetched_at.date().isoformat()

    if args.limit is not None:
        if args.limit < 1:
            raise SystemExit("--limit must be >= 1 when provided. Omit --limit for full paginated fetch.")
        rows = fetch_connection_points_single_page(eu_crossing=args.eu_crossing, limit=args.limit)
    else:
        rows = fetch_connection_points_paginated(
            eu_crossing=args.eu_crossing,
            page_size=args.page_size,
            max_pages=args.max_pages,
        )
    docs = [
        normalize_connection_point(
            row,
            catalog_version=catalog_version,
            fetched_at=fetched_at,
            requested_eu_crossing=args.eu_crossing,
        )
        for row in rows
    ]
    candidates = [
        doc
        for doc in docs
        if candidate_filter(
            doc,
            eu_crossing=args.eu_crossing,
            require_has_data=args.require_connectionpoint_has_data,
        )
    ]

    summarize_filters(
        docs,
        eu_crossing=args.eu_crossing,
        show_excluded=args.show_excluded,
        require_has_data=args.require_connectionpoint_has_data,
    )
    summarize(candidates)

    if dry_run:
        print("\nDry run only. No MongoDB writes performed.")
        return

    stats = upsert_connection_points(candidates, collection_name=args.collection)
    print(
        "\nMongo upsert complete "
        f"(matched={stats['matched']}, modified={stats['modified']}, upserted={stats['upserted']})."
    )


if __name__ == "__main__":
    main()
