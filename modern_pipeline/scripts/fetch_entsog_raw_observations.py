"""Fetch ENTSOG physical-flow observations into MongoDB.

The fetch universe is intentionally broad: modern manifest candidates plus
legacy ITP selections. The final publishable series is selected later by a
manifest; this collection stores what ENTSOG reported.
"""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import re
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from pymongo import UpdateOne
from pymongo.errors import PyMongoError
from requests import JSONDecodeError, RequestException

from modern_pipeline.db import get_database
from modern_pipeline.scripts.audit_manifest_candidates import derive_candidates

ENTSOG_OPERATIONAL_DATA_URL = "https://transparency.entsog.eu/api/v1/operationalData"
DEFAULT_LEGACY_LOCATIONS = "working data/locationsSAFE.xlsx"
DEFAULT_POINTS_SAFE = "working data/pointsSAFE_EU27.xlsx"
REQUEST_TIMEOUT = 60
MIN_REQUEST_INTERVAL_SEC = 10.5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
BACKFILL_START_DATE = dt.date(2016, 1, 1)
LAG_DAYS = 3
MAX_RESPONSE_PREVIEW_CHARS = 500
MONGO_WRITE_ATTEMPTS = 5


def format_duration(seconds: float) -> str:
    seconds = max(0, seconds)
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    hours = minutes / 60
    return f"{hours:.1f}h"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch broad ENTSOG raw physical-flow observations into MongoDB."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and summarize the fetch plan without writing. This is the default.",
    )
    mode.add_argument(
        "--write",
        action="store_true",
        help="Fetch and upsert observations to MongoDB.",
    )
    parser.add_argument(
        "--mode",
        choices=["daily", "backfill"],
        default="daily",
        help="daily fetches a rolling window; backfill fetches from 2016-01-01 by default.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Rolling-window length for daily mode. Default: 7.",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        default=None,
        help="Override start date, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        default=None,
        help="Override end date, YYYY-MM-DD. Default is today minus 3 days.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="pointDirections per ENTSOG request. Default: 10.",
    )
    parser.add_argument(
        "--chunk",
        choices=["month", "week"],
        default="month",
        help="Date chunk size. Default: month.",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Maximum request batches to execute. Useful for smoke tests.",
    )
    parser.add_argument(
        "--point-direction",
        action="append",
        dest="point_directions",
        help="Restrict fetch to specific pointDirection. Can be passed multiple times.",
    )
    parser.add_argument(
        "--collection",
        default="entsog_raw_observations",
        help="MongoDB raw-observations collection. Default: entsog_raw_observations.",
    )
    parser.add_argument(
        "--connection-points-collection",
        default="entsog_connection_points",
        help="MongoDB connection-points collection. Default: entsog_connection_points.",
    )
    parser.add_argument(
        "--legacy-locations",
        default=DEFAULT_LEGACY_LOCATIONS,
        help=f"Legacy locationsSAFE.xlsx path. Default: {DEFAULT_LEGACY_LOCATIONS}",
    )
    parser.add_argument(
        "--points-safe",
        default=DEFAULT_POINTS_SAFE,
        help=f"Legacy pointsSAFE_EU27.xlsx path. Default: {DEFAULT_POINTS_SAFE}",
    )
    parser.add_argument(
        "--skip-points-safe",
        action="store_true",
        help="Do not add extra ITP pointDirections from pointsSAFE_EU27 entry-only/UK sheets.",
    )
    return parser.parse_args()


def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def end_date_default() -> dt.date:
    return dt.date.today() - dt.timedelta(days=LAG_DAYS)


def resolve_date_window(args: argparse.Namespace) -> Tuple[dt.date, dt.date]:
    end = parse_date(args.to_date) if args.to_date else end_date_default()
    if args.from_date:
        start = parse_date(args.from_date)
    elif args.mode == "backfill":
        start = BACKFILL_START_DATE
    else:
        if args.days < 1:
            raise ValueError("--days must be >= 1")
        start = end - dt.timedelta(days=args.days - 1)
    if start > end:
        raise ValueError("Start date must be <= end date")
    return start, end


def split_semicolon(value: Any) -> List[str]:
    if value in (None, ""):
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    if isinstance(value, list):
        return [str(part).strip().lower() for part in value if str(part).strip()]
    return [part.strip().lower() for part in str(value).split(";") if part.strip()]


def point_direction_parts(point_direction: str) -> Dict[str, Optional[str]]:
    text = point_direction.strip().lower()
    match = re.match(r"^(?P<operator>[a-z]{2}-(?:tso|dso|lso)-\d{4})(?P<point>[a-z]+-\d+)(?P<direction>entry|exit)$", text)
    if not match:
        return {"operatorKey": None, "pointKey": None, "directionKey": None}
    return {
        "operatorKey": match.group("operator").upper(),
        "pointKey": match.group("point").upper(),
        "directionKey": match.group("direction"),
    }


def add_universe_item(
    universe: Dict[str, Dict[str, Any]],
    point_direction: str,
    source_set: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    point_direction = point_direction.strip().lower()
    if not point_direction:
        return
    parts = point_direction_parts(point_direction)
    item = universe.setdefault(
        point_direction,
        {
            "pointDirection": point_direction,
            "operatorKey": parts["operatorKey"],
            "pointKey": parts["pointKey"],
            "directionKey": parts["directionKey"],
            "sourceSets": set(),
            "metadata": {},
        },
    )
    item["sourceSets"].add(source_set)
    if metadata:
        item["metadata"].update({k: v for k, v in metadata.items() if v not in (None, "")})


def add_modern_universe(universe: Dict[str, Dict[str, Any]], connection_points_collection: str) -> None:
    db = get_database()
    docs = list(db[connection_points_collection].find({"catalogStatus": "candidate"}))
    for doc in docs:
        candidate = derive_candidates(doc)
        flags = split_semicolon(candidate.get("flags", []))
        metadata = {
            "catalogPointKey": candidate.get("catalogPointKey"),
            "pointLabel": candidate.get("pointLabel"),
            "modernSourceCountryKeys": ";".join(candidate.get("sourceCountryKeys") or []),
            "modernImportCountryKeys": ";".join(candidate.get("importCountryKeys") or []),
            "modernFlags": ";".join(candidate.get("flags") or []),
        }
        for pdid in candidate.get("recommendedSelectedPointDirections") or []:
            add_universe_item(universe, pdid, "modern_selected", metadata)
        for pdid in candidate.get("unmatchedSelectedPointDirections") or []:
            add_universe_item(universe, pdid, "modern_unmatched_selected", metadata)
        for pdid in candidate.get("fallbackCandidatePointDirections") or []:
            add_universe_item(universe, pdid, "modern_fallback_candidate", metadata)

        if set(flags) & {"grouped_point", "multiple_import_entries", "import_entry_not_in_opd", "bidirectional_point"}:
            for pdid in candidate.get("availablePointDirections") or []:
                add_universe_item(universe, pdid, "modern_available_flagged", metadata)


def add_legacy_locations_universe(universe: Dict[str, Dict[str, Any]], legacy_locations: str) -> None:
    sheets = [0, "UK"]
    for sheet in sheets:
        try:
            df = pd.read_excel(legacy_locations, sheet_name=sheet).fillna("")
        except ValueError:
            continue
        required = {"operator", "location", "direction"}
        if not required.issubset(df.columns):
            continue
        for _, row in df.iterrows():
            location = str(row.get("location", "")).strip()
            aggregation = str(row.get("aggregation", "")).strip()
            if not location.startswith("ITP-"):
                continue
            if aggregation == "LNG":
                continue
            point_direction = f"{str(row.get('operator', '')).strip()}{location}{str(row.get('direction', '')).strip()}".lower()
            add_universe_item(
                universe,
                point_direction,
                "legacy_locations",
                {
                    "legacyLabel": str(row.get("label", "")).strip(),
                    "legacyExportCountry": str(row.get("exportcountry", "")).strip(),
                    "legacyImportCountry": str(row.get("importcountry", "")).strip(),
                    "legacyAggregation": aggregation,
                    "legacyRouteGroup": str(row.get("aggregation2", "")).strip(),
                    "legacyAggregation3": str(row.get("aggregation3", "")).strip(),
                },
            )


def extract_point_directions_from_url(value: str) -> List[str]:
    match = re.search(r"pointDirection=([^&]+)", value)
    if not match:
        return []
    return [part.strip().lower() for part in match.group(1).split(",") if part.strip().lower()]


def add_points_safe_universe(universe: Dict[str, Dict[str, Any]], points_safe: str) -> None:
    for sheet in ("entry only", "UK"):
        try:
            df = pd.read_excel(points_safe, sheet_name=sheet)
        except ValueError:
            continue
        if df.empty:
            continue
        col = df.columns[0]
        for value in df[col].dropna().astype(str):
            for point_direction in extract_point_directions_from_url(value):
                parts = point_direction_parts(point_direction)
                if not (parts["pointKey"] or "").startswith("ITP-"):
                    continue
                if point_direction in universe:
                    continue
                add_universe_item(
                    universe,
                    point_direction,
                    f"legacy_points_safe_{sheet.replace(' ', '_')}",
                )


def build_universe(args: argparse.Namespace) -> List[Dict[str, Any]]:
    universe: Dict[str, Dict[str, Any]] = {}
    add_modern_universe(universe, args.connection_points_collection)
    add_legacy_locations_universe(universe, args.legacy_locations)
    if not args.skip_points_safe:
        add_points_safe_universe(universe, args.points_safe)

    if args.point_directions:
        wanted = {pdid.lower() for pdid in args.point_directions}
        universe = {pdid: item for pdid, item in universe.items() if pdid in wanted}

    rows = []
    for item in universe.values():
        item = dict(item)
        item["sourceSets"] = sorted(item["sourceSets"])
        rows.append(item)
    return sorted(rows, key=lambda item: item["pointDirection"])


def month_chunks(start: dt.date, end: dt.date) -> Iterable[Tuple[dt.date, dt.date]]:
    cursor = start
    while cursor <= end:
        last_day = calendar.monthrange(cursor.year, cursor.month)[1]
        chunk_end = min(end, dt.date(cursor.year, cursor.month, last_day))
        yield cursor, chunk_end
        cursor = chunk_end + dt.timedelta(days=1)


def week_chunks(start: dt.date, end: dt.date) -> Iterable[Tuple[dt.date, dt.date]]:
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + dt.timedelta(days=6))
        yield cursor, chunk_end
        cursor = chunk_end + dt.timedelta(days=1)


def date_chunks(start: dt.date, end: dt.date, chunk: str) -> List[Tuple[dt.date, dt.date]]:
    return list(month_chunks(start, end) if chunk == "month" else week_chunks(start, end))


def batches(items: Sequence[Dict[str, Any]], batch_size: int) -> Iterable[List[Dict[str, Any]]]:
    if batch_size < 1:
        raise ValueError("--batch-size must be >= 1")
    for idx in range(0, len(items), batch_size):
        yield list(items[idx : idx + batch_size])


def response_preview(response: requests.Response) -> str:
    text = response.text.strip().replace("\n", " ")
    if len(text) > MAX_RESPONSE_PREVIEW_CHARS:
        return text[:MAX_RESPONSE_PREVIEW_CHARS] + "..."
    return text


def get_json_with_retries(params: Dict[str, Any]) -> Dict[str, Any]:
    session = requests.Session()
    for attempt in range(1, 6):
        if attempt > 1:
            time.sleep(MIN_REQUEST_INTERVAL_SEC)
        try:
            response = session.get(ENTSOG_OPERATIONAL_DATA_URL, params=params, timeout=REQUEST_TIMEOUT)
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
        if response.status_code in {204, 404}:
            return {"operationalData": []}
        response.raise_for_status()
        try:
            payload = response.json()
        except JSONDecodeError as exc:
            content_type = response.headers.get("Content-Type", "unknown")
            preview = response_preview(response)
            if attempt < 5:
                print(
                    "ENTSOG returned non-JSON response "
                    f"(status={response.status_code}, content_type={content_type}); "
                    f"waiting {MIN_REQUEST_INTERVAL_SEC:.1f}s (attempt {attempt}/5). "
                    f"Preview: {preview!r}",
                    file=sys.stderr,
                )
                time.sleep(MIN_REQUEST_INTERVAL_SEC)
                continue
            raise RuntimeError(
                "ENTSOG returned a non-JSON response after retries "
                f"(status={response.status_code}, content_type={content_type}). "
                f"Preview: {preview!r}. Params: {params}"
            ) from exc
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected non-dict ENTSOG response.")
        message = payload.get("message")
        if isinstance(message, str) and "archived" in message.lower():
            raise RuntimeError(
                "ENTSOG live API reports this date range is archived. "
                "Use a date within the rolling live window or ingest TP Archives separately. "
                f"Message: {message}"
            )
        return payload
    raise RuntimeError("Failed to fetch ENTSOG data after retries.")


def parse_entsog_datetime(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            return dt.datetime.strptime(value, "%Y-%m-%d %H:%M")
        except ValueError:
            return None


def row_point_direction(row: Dict[str, Any]) -> Optional[str]:
    operator = row.get("operatorKey")
    point = row.get("pointKey")
    direction = row.get("directionKey")
    if operator and point and direction:
        return f"{operator}{point}{direction}".lower()
    return None


def normalize_observation(row: Dict[str, Any], universe_by_pd: Dict[str, Dict[str, Any]], fetched_at: dt.datetime) -> Optional[Dict[str, Any]]:
    point_direction = row_point_direction(row)
    if not point_direction:
        return None
    period_from = parse_entsog_datetime(row.get("periodFrom"))
    period_to = parse_entsog_datetime(row.get("periodTo"))
    if not period_from:
        return None
    gas_day = dt.datetime.combine(period_from.date(), dt.time.min)

    value = row.get("value")
    try:
        numeric_value = None if value in (None, "") else float(value)
    except (TypeError, ValueError):
        numeric_value = None

    universe_item = universe_by_pd.get(point_direction, {})
    parts = point_direction_parts(point_direction)
    return {
        "provider": "ENTSOG",
        "pointDirection": point_direction,
        "operatorKey": row.get("operatorKey") or parts["operatorKey"],
        "pointKey": row.get("pointKey") or parts["pointKey"],
        "directionKey": row.get("directionKey") or parts["directionKey"],
        "indicator": row.get("indicator"),
        "periodType": row.get("periodType"),
        "periodFrom": period_from,
        "periodTo": period_to,
        "gasDay": gas_day,
        "unit": row.get("unit"),
        "value": numeric_value,
        "flowStatus": row.get("flowStatus"),
        "tsoEicCode": row.get("tsoEicCode"),
        "operatorLabel": row.get("operatorLabel"),
        "pointLabel": row.get("pointLabel"),
        "tsoItemIdentifier": row.get("tsoItemIdentifier"),
        "lastUpdateDateTime": parse_entsog_datetime(row.get("lastUpdateDateTime")),
        "isNA": row.get("isNA"),
        "isArchived": row.get("isArchived"),
        "sourceSets": universe_item.get("sourceSets", []),
        "sourceMetadata": universe_item.get("metadata", {}),
        "fetchedAt": fetched_at,
    }


def ensure_indexes(collection_name: str) -> None:
    db = get_database()
    collection = db[collection_name]
    collection.create_index(
        [("pointDirection", 1), ("periodFrom", 1), ("indicator", 1), ("periodType", 1)],
        unique=True,
    )
    collection.create_index([("gasDay", 1), ("pointDirection", 1)])
    collection.create_index([("pointKey", 1), ("gasDay", 1)])


def upsert_observations(collection_name: str, observations: List[Dict[str, Any]]) -> Dict[str, int]:
    if not observations:
        return {"matched": 0, "modified": 0, "upserted": 0}
    db = get_database()
    collection = db[collection_name]
    operations = []
    for obs in observations:
        operations.append(
            UpdateOne(
                {
                    "pointDirection": obs["pointDirection"],
                    "periodFrom": obs["periodFrom"],
                    "indicator": obs["indicator"],
                    "periodType": obs["periodType"],
                },
                {"$set": obs},
                upsert=True,
            )
        )
    result = None
    for attempt in range(1, MONGO_WRITE_ATTEMPTS + 1):
        try:
            result = collection.bulk_write(operations, ordered=False)
            break
        except PyMongoError as exc:
            if attempt >= MONGO_WRITE_ATTEMPTS:
                raise RuntimeError(f"Mongo bulk_write failed after retries: {exc}") from exc
            print(
                f"Mongo bulk_write failed: {exc}. Waiting {MIN_REQUEST_INTERVAL_SEC:.1f}s "
                f"(attempt {attempt}/{MONGO_WRITE_ATTEMPTS}).",
                file=sys.stderr,
            )
            time.sleep(MIN_REQUEST_INTERVAL_SEC)

    if result is None:
        raise RuntimeError("Mongo bulk_write failed without returning a result.")
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "upserted": len(result.upserted_ids),
    }


def summarize_plan(universe: List[Dict[str, Any]], chunks: List[Tuple[dt.date, dt.date]], batch_size: int, max_batches: Optional[int]) -> None:
    print(f"Universe pointDirections: {len(universe)}")
    source_counts: Dict[str, int] = {}
    for item in universe:
        for source_set in item["sourceSets"]:
            source_counts[source_set] = source_counts.get(source_set, 0) + 1
    print("Universe sourceSets:")
    for key, count in sorted(source_counts.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")
    planned_batches = len(chunks) * ((len(universe) + batch_size - 1) // batch_size)
    if max_batches is not None:
        planned_batches = min(planned_batches, max_batches)
    print(f"Date chunks: {len(chunks)}")
    print(f"Planned ENTSOG requests: {planned_batches}")
    print(f"Estimated minimum runtime from pacing: {planned_batches * MIN_REQUEST_INTERVAL_SEC / 60:.1f} minutes")
    print("Sample pointDirections:")
    for item in universe[:15]:
        print(f"  {item['pointDirection']} [{','.join(item['sourceSets'])}]")


def planned_request_count(n_universe: int, n_chunks: int, batch_size: int, max_batches: Optional[int]) -> int:
    count = n_chunks * ((n_universe + batch_size - 1) // batch_size)
    if max_batches is not None:
        return min(count, max_batches)
    return count


def fetch_batch(batch: List[Dict[str, Any]], start: dt.date, end: dt.date) -> Dict[str, Any]:
    point_directions = ",".join(item["pointDirection"] for item in batch)
    params = {
        "forceDownload": "true",
        "pointDirection": point_directions,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "indicator": "Physical Flow",
        "periodType": "day",
        "timezone": "CET",
        "limit": -1,
        "dataset": 1,
        "directDownload": "true",
    }
    return get_json_with_retries(params)


def main() -> None:
    args = parse_args()
    dry_run = not args.write
    start, end = resolve_date_window(args)
    universe = build_universe(args)
    chunks = date_chunks(start, end, args.chunk)
    summarize_plan(universe, chunks, batch_size=args.batch_size, max_batches=args.max_batches)

    if dry_run:
        print("\nDry run only. No ENTSOG observation requests made and no MongoDB writes performed.")
        return

    ensure_indexes(args.collection)
    universe_by_pd = {item["pointDirection"]: item for item in universe}
    total_planned_requests = planned_request_count(
        n_universe=len(universe),
        n_chunks=len(chunks),
        batch_size=args.batch_size,
        max_batches=args.max_batches,
    )
    request_count = 0
    total_rows = 0
    total_upserted = 0
    total_modified = 0
    started_at = time.monotonic()

    for chunk_start, chunk_end in chunks:
        for batch in batches(universe, args.batch_size):
            if args.max_batches is not None and request_count >= args.max_batches:
                print("Reached --max-batches; stopping.")
                elapsed = time.monotonic() - started_at
                print(
                    f"Rows seen={total_rows}, modified={total_modified}, upserted={total_upserted}, "
                    f"elapsed={format_duration(elapsed)}"
                )
                return
            request_count += 1
            request_started_at = time.monotonic()
            print(
                f"Request {request_count}/{total_planned_requests}: {chunk_start} to {chunk_end}, "
                f"{len(batch)} pointDirections...",
                flush=True,
            )
            fetched_at = dt.datetime.now(dt.timezone.utc)
            payload = fetch_batch(batch, chunk_start, chunk_end)
            rows = payload.get("operationalData") or []
            observations = [
                obs
                for row in rows
                if (obs := normalize_observation(row, universe_by_pd=universe_by_pd, fetched_at=fetched_at))
            ]
            stats = upsert_observations(args.collection, observations)
            request_elapsed = time.monotonic() - request_started_at
            total_elapsed = time.monotonic() - started_at
            avg_elapsed = total_elapsed / request_count
            remaining = max(total_planned_requests - request_count, 0) * avg_elapsed
            total_rows += len(observations)
            total_modified += stats["modified"]
            total_upserted += stats["upserted"]
            print(
                f"  rows={len(observations)} modified={stats['modified']} upserted={stats['upserted']} "
                f"request_time={format_duration(request_elapsed)} "
                f"elapsed={format_duration(total_elapsed)} eta={format_duration(remaining)}",
                flush=True,
            )
            time.sleep(MIN_REQUEST_INTERVAL_SEC)

    elapsed = time.monotonic() - started_at
    print(
        f"\nComplete. Requests={request_count}, rows seen={total_rows}, "
        f"modified={total_modified}, upserted={total_upserted}, elapsed={format_duration(elapsed)}"
    )


if __name__ == "__main__":
    main()
