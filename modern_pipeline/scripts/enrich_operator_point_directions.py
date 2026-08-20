"""Attach ENTSOG operatorPointDirections metadata to connection points."""

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

ENTSOG_OPERATOR_POINT_DIRECTIONS_CSV = "https://transparency.entsog.eu/api/v1/operatorPointDirections.csv"
REQUEST_TIMEOUT = 45
MIN_REQUEST_INTERVAL_SEC = 10.5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich connection points with ENTSOG operatorPointDirections metadata."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and summarize point directions without writing to MongoDB. This is the default.",
    )
    mode.add_argument(
        "--write",
        action="store_true",
        help="Write fetched pointDirections arrays back to MongoDB.",
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
        help="Maximum operatorPointDirections rows requested per pointKey. Default: 100.",
    )
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="When writing, also mark points where ENTSOG returns no operatorPointDirections rows.",
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

    raise RuntimeError("Failed to fetch ENTSOG operatorPointDirections after retries.")


def parse_csv(csv_text: Optional[str]) -> List[Dict[str, str]]:
    if not csv_text:
        return []
    return list(csv.DictReader(io.StringIO(csv_text)))


def point_direction_id(operator_key: Optional[str], point_key: Optional[str], direction_key: Optional[str]) -> Optional[str]:
    if not operator_key or not point_key or not direction_key:
        return None
    return f"{operator_key}{point_key}{direction_key}".lower()


def normalize_direction(row: Dict[str, str]) -> Dict[str, Any]:
    operator_key = clean_str(row.get("operatorKey"))
    point_key = clean_str(row.get("pointKey"))
    direction_key = clean_str(row.get("directionKey"))

    return {
        "pointDirection": point_direction_id(operator_key, point_key, direction_key),
        "pointKey": point_key,
        "pointLabel": clean_str(row.get("pointLabel")),
        "operatorKey": operator_key,
        "operatorLabel": clean_str(row.get("operatorLabel")),
        "tsoEicCode": clean_str(row.get("tsoEicCode")),
        "directionKey": direction_key,
        "validFrom": clean_str(row.get("validFrom")),
        "validTo": clean_str(row.get("validTo")),
        "hasData": as_bool(row.get("hasData")),
        "isVirtualizedCommercially": as_bool(row.get("isVirtualizedCommercially")),
        "virtualizedCommerciallySince": clean_str(row.get("virtualizedCommerciallySince")),
        "isVirtualizedOperationally": as_bool(row.get("isVirtualizedOperationally")),
        "virtualizedOperationallySince": clean_str(row.get("virtualizedOperationallySince")),
        "isPipeInPipe": as_bool(row.get("isPipeInPipe")),
        "relatedOperators": clean_str(row.get("relatedOperators")),
        "relatedPoints": clean_str(row.get("relatedPoints")),
        "isDoubleReporting": as_bool(row.get("isDoubleReporting")),
        "doubleReportingWithTsoKey": clean_str(row.get("doubleReportingWithTsoKey")),
        "doubleReportingWithTsoLabel": clean_str(row.get("doubleReportingWithTsoLabel")),
        "tsoItemIdentifier": clean_str(row.get("tsoItemIdentifier")),
        "tpTsoItemLabel": clean_str(row.get("tpTsoItemLabel")),
        "tpTsoValidFrom": clean_str(row.get("tpTsoValidFrom")),
        "tpTsoValidTo": clean_str(row.get("tpTsoValidTo")),
        "tpTsoRemarks": clean_str(row.get("tpTsoRemarks")),
        "tpTsoConversionFactor": clean_str(row.get("tpTsoConversionFactor")),
        "tpTsoGCVMin": clean_str(row.get("tpTsoGCVMin")),
        "tpTsoGCVMax": clean_str(row.get("tpTsoGCVMax")),
        "tpTsoGCVUnit": clean_str(row.get("tpTsoGCVUnit")),
        "lastUpdateDateTime": clean_str(row.get("lastUpdateDateTime")),
        "isInvalid": as_bool(row.get("isInvalid")),
        "isCAMRelevant": as_bool(row.get("isCAMRelevant")),
        "isCMPRelevant": as_bool(row.get("isCMPRelevant")),
        "bookingPlatformKey": clean_str(row.get("bookingPlatformKey")),
        "virtualReverseFlow": clean_str(row.get("virtualReverseFlow")),
        "tsoCountry": clean_str(row.get("tSOCountry")),
        "tsoBalancingZone": clean_str(row.get("tSOBalancingZone")),
        "crossBorderPointType": clean_str(row.get("crossBorderPointType")),
        "euRelationship": clean_str(row.get("eURelationship")),
        "connectedOperators": clean_str(row.get("connectedOperators")),
        "adjacentTsoEic": clean_str(row.get("adjacentTsoEic")),
        "adjacentOperatorKey": clean_str(row.get("adjacentOperatorKey")),
        "adjacentCountry": clean_str(row.get("adjacentCountry")),
        "adjacentZones": clean_str(row.get("adjacentZones")),
        "entsogId": clean_str(row.get("id")),
        "dataSet": clean_str(row.get("dataSet")),
    }


def fetch_point_directions(point_key: str, limit: int) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "hasData": 1,
        "pointKey": point_key,
        "limit": limit,
    }
    csv_text = get_csv_with_retries(ENTSOG_OPERATOR_POINT_DIRECTIONS_CSV, params)
    rows = parse_csv(csv_text)
    if len(rows) == limit:
        print(
            f"  warning: {point_key} returned exactly --limit={limit} rows; "
            "operatorPointDirections may be truncated.",
            file=sys.stderr,
        )
    return [normalize_direction(row) for row in rows]


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
    total_directions = sum(len(directions) for directions in results.values())
    empty_points = [point_key for point_key, directions in results.items() if not directions]
    print(f"\nPointKeys checked: {len(results)}")
    print(f"operatorPointDirections rows: {total_directions}")
    print(f"PointKeys with no rows: {len(empty_points)}")

    by_direction: Dict[str, int] = {}
    by_tso_country: Dict[str, int] = {}
    for directions in results.values():
        for direction in directions:
            direction_key = direction.get("directionKey") or "Unknown"
            tso_country = direction.get("tsoCountry") or "Unknown"
            by_direction[direction_key] = by_direction.get(direction_key, 0) + 1
            by_tso_country[tso_country] = by_tso_country.get(tso_country, 0) + 1

    print("\nBy directionKey:")
    for key, count in sorted(by_direction.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nBy tsoCountry:")
    for key, count in sorted(by_tso_country.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nSample:")
    shown = 0
    for point_key, directions in results.items():
        if shown >= 10:
            break
        if not directions:
            print(f"  {point_key}: no operatorPointDirections rows")
            shown += 1
            continue
        for direction in directions[:2]:
            print(
                "  "
                f"{point_key}: {direction.get('pointDirection')} "
                f"{direction.get('operatorLabel')} {direction.get('directionKey')} "
                f"tsoCountry={direction.get('tsoCountry')} adjacent={direction.get('adjacentCountry')}"
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
    for point_key, directions in results.items():
        if not directions and not include_empty:
            continue
        operations.append(
            UpdateOne(
                {"pointKey": point_key},
                {
                    "$set": {
                        "pointDirections": directions,
                        "pointDirectionsFetchedAt": fetched_at,
                        "pointDirectionsSource": {
                            "provider": "ENTSOG",
                            "endpoint": ENTSOG_OPERATOR_POINT_DIRECTIONS_CSV,
                            "params": {"hasData": 1, "pointKey": point_key},
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
    collection.create_index("pointDirections.pointDirection")
    collection.create_index("pointDirections.operatorKey")
    collection.create_index("pointDirections.tsoCountry")

    operations = build_updates(results, fetched_at=fetched_at, include_empty=include_empty)
    if not operations:
        return {"matched": 0, "modified": 0}

    result = collection.bulk_write(operations, ordered=False)
    return {"matched": result.matched_count, "modified": result.modified_count}


def fetch_all(point_keys: Iterable[str], limit: int) -> Dict[str, List[Dict[str, Any]]]:
    results: Dict[str, List[Dict[str, Any]]] = {}
    point_keys = list(point_keys)
    for idx, point_key in enumerate(point_keys, start=1):
        print(f"Fetching operatorPointDirections for {point_key} ({idx}/{len(point_keys)})...", flush=True)
        results[point_key] = fetch_point_directions(point_key, limit=limit)
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
