"""Fetch GIE ALSI daily LNG terminal observations into MongoDB."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from dotenv import load_dotenv
from pymongo import UpdateOne
from pymongo.errors import PyMongoError
from requests import JSONDecodeError, RequestException

from modern_pipeline.db import get_database

GIE_ALSI_API_URL = "https://alsi.gie.eu/api"
DEFAULT_MANIFEST_VERSION = "gie_alsi_lng_terminals_v1"
DEFAULT_LNG_GWH_PER_MCM = 11.58
MAX_PAGE_SIZE = 300
REQUEST_TIMEOUT = 45
MIN_REQUEST_INTERVAL_SEC = 1.1
MAX_RETRIES = 5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
BACKFILL_START_DATE = dt.date(2016, 1, 1)
LAG_DAYS = 3
MONGO_WRITE_ATTEMPTS = 5
MAX_RESPONSE_PREVIEW_CHARS = 500


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
        description="Fetch selected GIE ALSI LNG terminal daily observations into MongoDB."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and summarize the fetch plan without requesting daily ALSI data. This is the default.",
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
        "--manifest-version",
        default=DEFAULT_MANIFEST_VERSION,
        help=f"GIE LNG terminal manifest version. Default: {DEFAULT_MANIFEST_VERSION}.",
    )
    parser.add_argument(
        "--manifest-collection",
        default="gie_lng_terminal_manifest",
        help="MongoDB manifest collection. Default: gie_lng_terminal_manifest.",
    )
    parser.add_argument(
        "--collection",
        default="gie_lng_raw_observations",
        help="MongoDB raw-observations collection. Default: gie_lng_raw_observations.",
    )
    parser.add_argument(
        "--country",
        action="append",
        dest="countries",
        help="Restrict fetch to ISO2 country code. Can be passed multiple times.",
    )
    parser.add_argument(
        "--facility-eic",
        action="append",
        dest="facility_eics",
        help="Restrict fetch to a specific ALSI facility EIC. Can be passed multiple times.",
    )
    parser.add_argument(
        "--include-unselected",
        action="store_true",
        help="Also fetch manifest entries where selected is false.",
    )
    parser.add_argument(
        "--max-facilities",
        type=int,
        default=None,
        help="Maximum facilities to fetch. Useful for smoke tests.",
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


def get_api_key() -> str:
    load_dotenv()
    for name in ("ALSI_API_KEY", "GIE_API_KEY", "AGSI_API_KEY"):
        value = os.getenv(name, "").strip()
        if value:
            return value
    raise RuntimeError("Missing ALSI_API_KEY, GIE_API_KEY, or AGSI_API_KEY in environment or .env file.")


def load_manifest_entries(args: argparse.Namespace) -> List[Dict[str, Any]]:
    db = get_database()
    manifest = db[args.manifest_collection].find_one({"version": args.manifest_version})
    if not manifest:
        raise RuntimeError(
            f"Manifest version {args.manifest_version!r} not found in {args.manifest_collection!r}. "
            "Run build_gie_lng_terminal_manifest first."
        )

    countries = {country.upper() for country in args.countries or []}
    facility_eics = {eic.strip() for eic in args.facility_eics or []}
    entries = []
    for entry in manifest.get("entries") or []:
        if not isinstance(entry, dict):
            continue
        if not args.include_unselected and not entry.get("selected", True):
            continue
        country_iso2 = str(entry.get("countryIso2", "")).upper()
        country_raw = str(entry.get("countryCodeRaw", "")).upper()
        if countries and country_iso2 not in countries and country_raw not in countries:
            continue
        if facility_eics and str(entry.get("facilityEic", "")).strip() not in facility_eics:
            continue
        if not entry.get("facilityEic"):
            continue
        entries.append(entry)

    entries = sorted(entries, key=lambda item: (item.get("countryIso2", ""), item.get("facilityName", "")))
    if args.max_facilities is not None:
        entries = entries[: args.max_facilities]
    return entries


def summarize_plan(entries: Sequence[Dict[str, Any]], start: dt.date, end: dt.date) -> None:
    by_country: Dict[str, int] = {}
    by_status: Dict[str, int] = {}
    for entry in entries:
        country = str(entry.get("countryIso2", "")).upper() or "Unknown"
        status = str(entry.get("facilityStatus", "")) or "unknown"
        by_country[country] = by_country.get(country, 0) + 1
        by_status[status] = by_status.get(status, 0) + 1

    days = (end - start).days + 1
    minimum_pages = len(entries)
    approximate_pages = sum(max(1, (days + MAX_PAGE_SIZE - 1) // MAX_PAGE_SIZE) for _ in entries)
    print(f"Manifest facilities selected: {len(entries)}")
    print(f"Date window: {start} to {end} ({days} days)")
    print(f"Planned ALSI requests: roughly {approximate_pages} pages, minimum {minimum_pages}")
    print(f"Estimated minimum runtime from pacing: {approximate_pages * MIN_REQUEST_INTERVAL_SEC / 60:.1f} minutes")

    print("\nBy countryIso2:")
    for key, count in sorted(by_country.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nBy facilityStatus:")
    for key, count in sorted(by_status.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nSample facilities:")
    for entry in entries[:20]:
        print(f"  {entry.get('countryIso2')} | {entry.get('facilityName')} | {entry.get('facilityEic')}")


def last_page_from_payload(payload: Dict[str, Any]) -> Optional[int]:
    for key in ("last_page", "lastPage", "lastpage", "pages", "total_pages"):
        value = payload.get(key)
        if isinstance(value, int) and value >= 0:
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def response_preview(response: requests.Response) -> str:
    text = response.text.strip().replace("\n", " ")
    if len(text) > MAX_RESPONSE_PREVIEW_CHARS:
        return text[:MAX_RESPONSE_PREVIEW_CHARS] + "..."
    return text


def redact_secret(value: Any, secret: Optional[str] = None) -> str:
    text = "" if value is None else str(value)
    if secret:
        text = text.replace(secret, "[redacted]")
    return text


def get_json_with_retries(session: requests.Session, params: Dict[str, Any]) -> Dict[str, Any]:
    api_key = session.headers.get("x-key")
    for attempt in range(1, MAX_RETRIES + 1):
        if attempt > 1:
            time.sleep(MIN_REQUEST_INTERVAL_SEC)
        try:
            response = session.get(GIE_ALSI_API_URL, params=params, timeout=REQUEST_TIMEOUT)
        except RequestException as exc:
            if attempt >= MAX_RETRIES:
                raise RuntimeError(f"GIE ALSI request failed after retries: {exc}") from exc
            print(
                f"GIE ALSI request failed: {exc}. Waiting {MIN_REQUEST_INTERVAL_SEC:.1f}s "
                f"(attempt {attempt}/{MAX_RETRIES}).",
                file=sys.stderr,
            )
            continue

        if response.status_code in RETRY_STATUS_CODES and attempt < MAX_RETRIES:
            retry_after = response.headers.get("Retry-After")
            wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else MIN_REQUEST_INTERVAL_SEC
            print(
                f"Retryable GIE ALSI response {response.status_code}; waiting {wait_seconds:.1f}s "
                f"(attempt {attempt}/{MAX_RETRIES}).",
                file=sys.stderr,
            )
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        try:
            payload = response.json()
        except JSONDecodeError as exc:
            preview = response_preview(response)
            if attempt < MAX_RETRIES:
                print(
                    "GIE ALSI returned non-JSON response "
                    f"(status={response.status_code}); waiting {MIN_REQUEST_INTERVAL_SEC:.1f}s "
                    f"(attempt {attempt}/{MAX_RETRIES}). Preview: {preview!r}",
                    file=sys.stderr,
                )
                continue
            raise RuntimeError(
                "GIE ALSI returned a non-JSON response after retries "
                f"(status={response.status_code}). Preview: {preview!r}. Params: {params}"
            ) from exc

        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected non-dict GIE ALSI payload for params={params}")
        if payload.get("error") and not payload.get("data"):
            message = redact_secret(payload.get("message") or payload.get("error"), api_key)
            if attempt < MAX_RETRIES:
                print(
                    "GIE ALSI returned an API-error payload "
                    f"for params={params}; waiting {MIN_REQUEST_INTERVAL_SEC:.1f}s "
                    f"(attempt {attempt}/{MAX_RETRIES}). Message: {message}",
                    file=sys.stderr,
                )
                time.sleep(MIN_REQUEST_INTERVAL_SEC)
                continue
            raise RuntimeError(f"GIE ALSI API error after retries for params={params}: {message}")
        return payload

    raise RuntimeError("Failed to fetch GIE ALSI data after retries.")


def clean_float(value: Any) -> Optional[float]:
    if value in (None, "", "-"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_gas_day(value: Any) -> Optional[dt.datetime]:
    if not value:
        return None
    text = str(value)
    try:
        return dt.datetime.fromisoformat(text[:10])
    except ValueError:
        return None


def normalize_observation(row: Dict[str, Any], entry: Dict[str, Any], fetched_at: dt.datetime) -> Optional[Dict[str, Any]]:
    gas_day = parse_gas_day(row.get("gasDayStart") or row.get("gasDay"))
    if not gas_day:
        return None

    send_out_gwh = clean_float(row.get("sendOut"))
    return {
        "provider": "GIE_ALSI",
        "facilityEic": entry.get("facilityEic"),
        "facilityName": entry.get("facilityName"),
        "facilityType": entry.get("facilityType"),
        "countryIso2": entry.get("countryIso2"),
        "countryCodeRaw": entry.get("countryCodeRaw"),
        "operatorEic": entry.get("operatorEic"),
        "operatorName": entry.get("operatorName"),
        "operatorShortName": entry.get("operatorShortName"),
        "gasDay": gas_day,
        "gasDayStart": row.get("gasDayStart"),
        "sendOutGwhPerDay": send_out_gwh,
        "valueGwh": send_out_gwh,
        "valueMcm": None if send_out_gwh is None else send_out_gwh / DEFAULT_LNG_GWH_PER_MCM,
        "converterGwhPerMcm": DEFAULT_LNG_GWH_PER_MCM,
        "inventoryThousandM3Lng": clean_float(row.get("inventory")),
        "technicalStorageCapacityThousandM3Lng": clean_float(row.get("dtmi")),
        "technicalSendoutCapacityGwhPerDay": clean_float(row.get("dtrs")),
        "full": clean_float(row.get("full")),
        "status": row.get("status"),
        "url": entry.get("apiUrl"),
        "sourceManifestVersion": entry.get("sourceManifestVersion"),
        "sourceManifestEntryId": entry.get("entryId"),
        "raw": row,
        "fetchedAt": fetched_at,
    }


def fetch_facility_rows(
    session: requests.Session,
    entry: Dict[str, Any],
    start: dt.date,
    end: dt.date,
) -> Tuple[List[Dict[str, Any]], int]:
    rows: List[Dict[str, Any]] = []
    page = 1
    request_count = 0
    params_base = dict(entry.get("apiParams") or {})
    if not params_base:
        params_base = {
            "country": entry.get("countryCodeRaw") or entry.get("countryIso2"),
            "company": entry.get("operatorEic"),
            "facility": entry.get("facilityEic"),
        }

    while True:
        params = {
            **params_base,
            "from": start.isoformat(),
            "to": end.isoformat(),
            "page": page,
            "size": MAX_PAGE_SIZE,
        }
        payload = get_json_with_retries(session, params)
        request_count += 1
        page_rows = payload.get("data") or []
        if not isinstance(page_rows, list):
            raise RuntimeError(f"Unexpected ALSI data payload for facility={entry.get('facilityEic')}, page={page}.")
        rows.extend([row for row in page_rows if isinstance(row, dict)])

        last_page = last_page_from_payload(payload)
        if last_page is not None:
            if page >= last_page:
                break
            page += 1
            time.sleep(MIN_REQUEST_INTERVAL_SEC)
            continue

        if not page_rows or len(page_rows) < MAX_PAGE_SIZE:
            break
        page += 1
        time.sleep(MIN_REQUEST_INTERVAL_SEC)

    return rows, request_count


def ensure_indexes(collection_name: str) -> None:
    db = get_database()
    collection = db[collection_name]
    collection.create_index([("facilityEic", 1), ("gasDay", 1)], unique=True)
    collection.create_index([("gasDay", 1), ("countryIso2", 1)])
    collection.create_index([("countryIso2", 1), ("facilityEic", 1)])


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
                    "facilityEic": obs["facilityEic"],
                    "gasDay": obs["gasDay"],
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


def attach_manifest_version(entries: Iterable[Dict[str, Any]], version: str) -> List[Dict[str, Any]]:
    rows = []
    for entry in entries:
        item = dict(entry)
        item["sourceManifestVersion"] = version
        rows.append(item)
    return rows


def main() -> None:
    args = parse_args()
    dry_run = not args.write
    start, end = resolve_date_window(args)
    entries = attach_manifest_version(load_manifest_entries(args), args.manifest_version)
    summarize_plan(entries, start, end)

    if dry_run:
        print("\nDry run only. No ALSI observation requests made and no MongoDB writes performed.")
        return

    api_key = get_api_key()
    ensure_indexes(args.collection)

    total_rows = 0
    total_modified = 0
    total_upserted = 0
    total_requests = 0
    started_at = time.monotonic()

    with requests.Session() as session:
        session.headers.update({"x-key": api_key})
        for idx, entry in enumerate(entries, start=1):
            request_started_at = time.monotonic()
            print(
                f"Facility {idx}/{len(entries)}: {entry.get('countryIso2')} | "
                f"{entry.get('facilityName')} | {entry.get('facilityEic')}...",
                flush=True,
            )
            fetched_at = dt.datetime.now(dt.timezone.utc)
            rows, request_count = fetch_facility_rows(session, entry, start, end)
            observations = [
                obs
                for row in rows
                if (obs := normalize_observation(row, entry=entry, fetched_at=fetched_at))
            ]
            stats = upsert_observations(args.collection, observations)
            total_rows += len(observations)
            total_modified += stats["modified"]
            total_upserted += stats["upserted"]
            total_requests += request_count

            request_elapsed = time.monotonic() - request_started_at
            total_elapsed = time.monotonic() - started_at
            avg_elapsed = total_elapsed / idx
            remaining = max(len(entries) - idx, 0) * avg_elapsed
            print(
                f"  pages={request_count} rows={len(observations)} "
                f"modified={stats['modified']} upserted={stats['upserted']} "
                f"facility_time={format_duration(request_elapsed)} "
                f"elapsed={format_duration(total_elapsed)} eta={format_duration(remaining)}",
                flush=True,
            )
            time.sleep(MIN_REQUEST_INTERVAL_SEC)

    elapsed = time.monotonic() - started_at
    print(
        f"\nComplete. Requests={total_requests}, rows seen={total_rows}, "
        f"modified={total_modified}, upserted={total_upserted}, elapsed={format_duration(elapsed)}"
    )


if __name__ == "__main__":
    main()
