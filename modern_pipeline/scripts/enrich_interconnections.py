"""Attach ENTSOG interconnections topology metadata to connection points."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import sys
import time
from typing import Any, Dict, Iterable, List, Optional

import requests
from pymongo import UpdateOne
from requests import RequestException

from modern_pipeline.db import get_database
from modern_pipeline.scripts.build_connection_points import as_bool, clean_str

ENTSOG_INTERCONNECTIONS_CSV = "https://transparency.entsog.eu/api/v1/interconnections.csv"
REQUEST_TIMEOUT = 45
MIN_REQUEST_INTERVAL_SEC = 10.5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich connection points with ENTSOG interconnections topology metadata."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and summarize interconnections without writing to MongoDB. This is the default.",
    )
    mode.add_argument(
        "--write",
        action="store_true",
        help="Write fetched interconnections arrays back to MongoDB.",
    )
    parser.add_argument(
        "--collection",
        default="entsog_connection_points",
        help="MongoDB connection-points collection. Default: entsog_connection_points.",
    )
    parser.add_argument(
        "--point-key",
        action="append",
        dest="point_keys",
        help="Specific pointKey to enrich. Can be passed multiple times. Defaults to all catalog candidates.",
    )
    parser.add_argument(
        "--max-points",
        type=int,
        default=None,
        help="Maximum number of pointKeys to enrich. Useful for dry-run testing.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum interconnections rows requested per pointKey. Default: 100.",
    )
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="When writing, also mark points where ENTSOG returns no interconnections rows.",
    )
    return parser.parse_args()


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

        if response.status_code == 404:
            return None

        response.raise_for_status()
        body = response.text.lstrip()
        if body.startswith("{"):
            if "No result found" in body:
                return None
            raise RuntimeError(f"ENTSOG returned JSON instead of CSV: {response.text[:300]}")
        if body.startswith("<"):
            raise RuntimeError("ENTSOG returned HTML instead of CSV; rerun later or lower --limit.")
        return response.text

    raise RuntimeError("Failed to fetch ENTSOG interconnections after retries.")


def parse_csv(csv_text: Optional[str]) -> List[Dict[str, str]]:
    if not csv_text:
        return []
    return list(csv.DictReader(io.StringIO(csv_text)))


def normalize_interconnection(row: Dict[str, str]) -> Dict[str, Any]:
    return {
        "pointKey": clean_str(row.get("pointKey")),
        "pointLabel": clean_str(row.get("pointLabel")),
        "isSingleOperator": as_bool(row.get("isSingleOperator")),
        "fromSystemLabel": clean_str(row.get("fromSystemLabel")),
        "fromInfrastructureTypeLabel": clean_str(row.get("fromInfrastructureTypeLabel")),
        "fromCountryKey": clean_str(row.get("fromCountryKey")),
        "fromCountryLabel": clean_str(row.get("fromCountryLabel")),
        "fromBalancingZoneKey": clean_str(row.get("fromBzKey")),
        "fromBalancingZoneLabel": clean_str(row.get("fromBzLabel")),
        "fromBalancingZoneLabelLong": clean_str(row.get("fromBzLabelLong")),
        "fromOperatorKey": clean_str(row.get("fromOperatorKey")),
        "fromOperatorLabel": clean_str(row.get("fromOperatorLabel")),
        "fromOperatorLongLabel": clean_str(row.get("fromOperatorLongLabel")),
        "fromPointKey": clean_str(row.get("fromPointKey")),
        "fromPointLabel": clean_str(row.get("fromPointLabel")),
        "fromDirectionKey": clean_str(row.get("fromDirectionKey")),
        "fromHasData": as_bool(row.get("fromHasData")),
        "fromTsoItemIdentifier": clean_str(row.get("fromTsoItemIdentifier")),
        "fromTsoPointLabel": clean_str(row.get("fromTsoPointLabel")),
        "toSystemLabel": clean_str(row.get("toSystemLabel")),
        "toInfrastructureTypeLabel": clean_str(row.get("toInfrastructureTypeLabel")),
        "toCountryKey": clean_str(row.get("toCountryKey")),
        "toCountryLabel": clean_str(row.get("toCountryLabel")),
        "toBalancingZoneKey": clean_str(row.get("toBzKey")),
        "toBalancingZoneLabel": clean_str(row.get("toBzLabel")),
        "toBalancingZoneLabelLong": clean_str(row.get("toBzLabelLong")),
        "toOperatorKey": clean_str(row.get("toOperatorKey")),
        "toOperatorLabel": clean_str(row.get("toOperatorLabel")),
        "toOperatorLongLabel": clean_str(row.get("toOperatorLongLabel")),
        "toPointKey": clean_str(row.get("toPointKey")),
        "toPointLabel": clean_str(row.get("toPointLabel")),
        "toDirectionKey": clean_str(row.get("toDirectionKey")),
        "toHasData": as_bool(row.get("toHasData")),
        "toTsoItemIdentifier": clean_str(row.get("toTsoItemIdentifier")),
        "toTsoPointLabel": clean_str(row.get("toTsoPointLabel")),
        "validFrom": clean_str(row.get("validFrom")),
        "validTo": clean_str(row.get("validto")),
        "lastUpdateDateTime": clean_str(row.get("lastUpdateDateTime")),
        "isInvalid": as_bool(row.get("isInvalid")),
        "entryTpNeMoUsage": clean_str(row.get("entryTpNeMoUsage")),
        "exitTpNeMoUsage": clean_str(row.get("exitTpNeMoUsage")),
        "entsogId": clean_str(row.get("id")),
        "dataSet": clean_str(row.get("dataSet")),
    }


def related_operational_point_keys(point_key: str, interconnections: Iterable[Dict[str, Any]]) -> List[str]:
    related = {point_key}
    for item in interconnections:
        for key_name in ("fromPointKey", "toPointKey"):
            value = item.get(key_name)
            if value:
                related.add(value)
    return sorted(related)


def fetch_interconnections(point_key: str, limit: int) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "pointKey": point_key,
        "limit": limit,
    }
    csv_text = get_csv_with_retries(ENTSOG_INTERCONNECTIONS_CSV, params)
    rows = parse_csv(csv_text)
    if len(rows) == limit:
        print(
            f"  warning: {point_key} returned exactly --limit={limit} rows; "
            "interconnections may be truncated.",
            file=sys.stderr,
        )
    return [normalize_interconnection(row) for row in rows]


def load_point_keys(collection_name: str, point_keys: Optional[List[str]], max_points: Optional[int]) -> List[str]:
    if point_keys:
        keys = point_keys
    else:
        db = get_database()
        collection = db[collection_name]
        cursor = collection.find(
            {"catalogStatus": "candidate"},
            {"pointKey": 1, "_id": 0},
        ).sort("pointKey", 1)
        keys = [doc["pointKey"] for doc in cursor if doc.get("pointKey")]

    deduped = list(dict.fromkeys(keys))
    if max_points is not None:
        if max_points < 1:
            raise ValueError("--max-points must be >= 1 when provided")
        return deduped[:max_points]
    return deduped


def summarize(results: Dict[str, List[Dict[str, Any]]]) -> None:
    total_rows = sum(len(rows) for rows in results.values())
    empty_points = [point_key for point_key, rows in results.items() if not rows]
    print(f"\nPointKeys checked: {len(results)}")
    print(f"interconnections rows: {total_rows}")
    print(f"PointKeys with no rows: {len(empty_points)}")

    from_countries: Dict[str, int] = {}
    to_countries: Dict[str, int] = {}
    related_key_counts: Dict[int, int] = {}

    for point_key, rows in results.items():
        related_count = len(related_operational_point_keys(point_key, rows))
        related_key_counts[related_count] = related_key_counts.get(related_count, 0) + 1
        for row in rows:
            from_country = row.get("fromCountryKey") or "Unknown"
            to_country = row.get("toCountryKey") or "Unknown"
            from_countries[from_country] = from_countries.get(from_country, 0) + 1
            to_countries[to_country] = to_countries.get(to_country, 0) + 1

    print("\nBy fromCountryKey:")
    for key, count in sorted(from_countries.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nBy toCountryKey:")
    for key, count in sorted(to_countries.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nRelated operational point-key counts:")
    for count, n_points in sorted(related_key_counts.items()):
        print(f"  {count} key(s): {n_points} point(s)")

    print("\nSample:")
    shown = 0
    for point_key, rows in results.items():
        if shown >= 10:
            break
        if not rows:
            print(f"  {point_key}: no interconnections rows")
            shown += 1
            continue
        related = related_operational_point_keys(point_key, rows)
        for row in rows[:2]:
            print(
                "  "
                f"{point_key}: {row.get('fromCountryKey')} {row.get('fromOperatorKey')} "
                f"{row.get('fromPointKey')} {row.get('fromDirectionKey')} "
                f"-> {row.get('toCountryKey')} {row.get('toOperatorKey')} "
                f"{row.get('toPointKey')} {row.get('toDirectionKey')} "
                f"relatedKeys={related}"
            )
            shown += 1
            if shown >= 10:
                break


def build_updates(
    results: Dict[str, List[Dict[str, Any]]],
    fetched_at: dt.datetime,
    include_empty: bool,
) -> List[UpdateOne]:
    operations: List[UpdateOne] = []
    for point_key, rows in results.items():
        if not rows and not include_empty:
            continue
        related_keys = related_operational_point_keys(point_key, rows)
        operations.append(
            UpdateOne(
                {"pointKey": point_key},
                {
                    "$set": {
                        "interconnections": rows,
                        "relatedOperationalPointKeys": related_keys,
                        "interconnectionsFetchedAt": fetched_at,
                        "interconnectionsSource": {
                            "provider": "ENTSOG",
                            "endpoint": ENTSOG_INTERCONNECTIONS_CSV,
                            "params": {"pointKey": point_key},
                        },
                    }
                },
            )
        )
    return operations


def write_results(
    collection_name: str,
    results: Dict[str, List[Dict[str, Any]]],
    fetched_at: dt.datetime,
    include_empty: bool,
) -> Dict[str, int]:
    db = get_database()
    collection = db[collection_name]
    collection.create_index("interconnections.fromOperatorKey")
    collection.create_index("interconnections.toOperatorKey")
    collection.create_index("relatedOperationalPointKeys")

    operations = build_updates(results, fetched_at=fetched_at, include_empty=include_empty)
    if not operations:
        return {"matched": 0, "modified": 0}

    result = collection.bulk_write(operations, ordered=False)
    return {"matched": result.matched_count, "modified": result.modified_count}


def fetch_all(point_keys: Iterable[str], limit: int) -> Dict[str, List[Dict[str, Any]]]:
    results: Dict[str, List[Dict[str, Any]]] = {}
    point_keys = list(point_keys)
    for idx, point_key in enumerate(point_keys, start=1):
        print(f"Fetching interconnections for {point_key} ({idx}/{len(point_keys)})...", flush=True)
        results[point_key] = fetch_interconnections(point_key, limit=limit)
        print(f"  fetched {len(results[point_key])} rows", flush=True)
        if idx < len(point_keys):
            time.sleep(MIN_REQUEST_INTERVAL_SEC)
    return results


def main() -> None:
    args = parse_args()
    dry_run = not args.write
    fetched_at = dt.datetime.now(dt.timezone.utc)

    if args.limit < 1:
        raise SystemExit("--limit must be >= 1")

    point_keys = load_point_keys(
        collection_name=args.collection,
        point_keys=args.point_keys,
        max_points=args.max_points,
    )
    if not point_keys:
        raise SystemExit("No pointKeys found to enrich.")

    results = fetch_all(point_keys, limit=args.limit)
    summarize(results)

    if dry_run:
        print("\nDry run only. No MongoDB writes performed.")
        return

    stats = write_results(
        collection_name=args.collection,
        results=results,
        fetched_at=fetched_at,
        include_empty=args.include_empty,
    )
    print(f"\nMongo update complete (matched={stats['matched']}, modified={stats['modified']}).")


if __name__ == "__main__":
    main()
