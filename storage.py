#!/usr/bin/env python3
"""Sync AGSI storage data to MongoDB (backfill or rolling window)."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import time
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import requests
from dotenv import load_dotenv
from pymongo import UpdateOne

from mongo_client import get_database, test_mongo_connection

BASE_URL = "https://agsi.gie.eu/api"
ABOUT_URL = "https://agsi.gie.eu/api/about?showlisting"
MAX_PAGE_SIZE = 300
REQUEST_TIMEOUT = 30
BACKFILL_START_DATE = dt.date(2016, 1, 1)
MIN_REQUEST_INTERVAL_SEC = 1.1
MAX_RETRIES = 5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


def _extract_country_codes(payload: Any) -> List[str]:
    """Extract 2-letter country codes from AGSI listing payload."""
    codes: Set[str] = set()
    code_pattern = re.compile(r"\bcountry=([A-Z]{2})\b", re.IGNORECASE)

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, str):
                    if key.lower() in {"country", "code"} and re.fullmatch(r"[A-Za-z]{2}", value):
                        codes.add(value.upper())
                    for match in code_pattern.findall(value):
                        codes.add(match.upper())
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, str):
            for match in code_pattern.findall(node):
                codes.add(match.upper())

    walk(payload)
    return sorted(codes)


def get_country_codes(session: requests.Session) -> List[str]:
    """Get all AGSI country codes from listing endpoint."""
    response = session.get(ABOUT_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    codes = _extract_country_codes(payload)

    if not codes:
        raise RuntimeError(
            "Could not parse country codes from AGSI listing endpoint. "
            "Check API response structure at /api/about?showlisting."
        )
    return codes


def _last_page_from_payload(payload: Dict[str, Any]) -> Optional[int]:
    """Read last-page info from varied API pagination key names."""
    candidates = ("last_page", "lastPage", "lastpage", "pages", "total_pages")
    for key in candidates:
        value = payload.get(key)
        if isinstance(value, int) and value >= 1:
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _get_json_with_retries(session: requests.Session, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """GET JSON with gentle rate-limiting and retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(MIN_REQUEST_INTERVAL_SEC)
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ConnectionError,
        ) as exc:
            if attempt >= MAX_RETRIES:
                raise RuntimeError(
                    f"Network timeout/connection error after retries for params={params}: {exc}"
                ) from exc
            wait_seconds = min(30, 2 ** (attempt - 1))
            print(
                f"Network timeout/connection error for params={params}: {exc}. "
                f"Retrying in {wait_seconds}s (attempt {attempt}/{MAX_RETRIES})."
            )
            time.sleep(wait_seconds)
            continue

        if response.status_code in RETRY_STATUS_CODES and attempt < MAX_RETRIES:
            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                wait_seconds = int(retry_after)
            else:
                wait_seconds = min(30, 2 ** (attempt - 1))
            print(
                f"Retryable AGSI response {response.status_code} for params={params}. "
                f"Retrying in {wait_seconds}s (attempt {attempt}/{MAX_RETRIES})."
            )
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected non-dict payload for params={params}")
        return payload

    raise RuntimeError(f"Failed to fetch AGSI data after retries for params={params}")


def fetch_country_data(
    session: requests.Session,
    country_code: str,
    start_date: str,
    end_date: str,
    query_key: str = "country",
) -> List[Dict[str, Any]]:
    """Fetch paginated daily storage data for one country/type query."""
    rows: List[Dict[str, Any]] = []
    page = 1

    while True:
        params = {
            query_key: country_code,
            "from": start_date,
            "to": end_date,
            "page": page,
            "size": MAX_PAGE_SIZE,
        }
        payload = _get_json_with_retries(session, BASE_URL, params)
        page_rows = payload.get("data", [])

        if not isinstance(page_rows, list):
            raise RuntimeError(f"Unexpected data payload for country={country_code}, page={page}.")

        for item in page_rows:
            if not isinstance(item, dict):
                continue
            item["country_query"] = country_code
            rows.append(item)

        last_page = _last_page_from_payload(payload)
        if last_page is not None:
            if page >= last_page:
                break
            page += 1
            continue

        if not page_rows or len(page_rows) < MAX_PAGE_SIZE:
            break
        page += 1

    return rows


def fetch_storage_dataframe(api_key: str, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """Return AGSI daily storage data between start_date and end_date."""
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")

    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()

    today = dt.date.today()
    if end_date > today:
        end_date_str = today.isoformat()

    with requests.Session() as session:
        session.headers.update({"x-key": api_key})
        country_codes = get_country_codes(session)

        all_rows: List[Dict[str, Any]] = []
        print(f"Starting AGSI fetch from {start_date_str} to {end_date_str}...")
        for code in country_codes:
            print(f"Fetching country={code}...")
            country_rows = fetch_country_data(session, code, start_date_str, end_date_str)
            all_rows.extend(country_rows)
        print("Fetching type=EU...")
        all_rows.extend(fetch_country_data(session, "EU", start_date_str, end_date_str, query_key="type"))

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    keep_columns = [
        "gasDayStart",
        "code",
        "gasInStorage",
        "full",
    ]
    available_columns = [col for col in keep_columns if col in df.columns]
    df = df[available_columns].copy()
    if "code" in df.columns:
        df["code"] = df["code"].fillna("EU")

    if "gasDayStart" in df.columns:
        df["gasDayStart"] = pd.to_datetime(df["gasDayStart"], errors="coerce").dt.date

    numeric_cols = [
        "gasInStorage",
        "full",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    sort_cols = [c for c in ("code", "gasDayStart") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync AGSI daily storage data to MongoDB."
    )
    parser.add_argument(
        "--mode",
        choices=["backfill", "daily"],
        default="daily",
        help="Run full backfill (since 2016-01-01) or rolling daily sync window.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to sync in daily mode (inclusive of today).",
    )
    return parser.parse_args()


def sync_gas_storage_to_mongo(df: pd.DataFrame, collection_name: str = "gas_storage") -> Dict[str, int]:
    """Upsert country-day storage rows into MongoDB."""
    if df.empty:
        return {"matched": 0, "modified": 0, "upserted": 0}

    db = get_database()
    collection = db[collection_name]
    collection.create_index([("day", 1), ("code", 1)], unique=True)

    operations: List[UpdateOne] = []
    for row in df.itertuples(index=False):
        day_value = pd.Timestamp(row.gasDayStart).to_pydatetime().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        code = str(row.code)
        gas_in_storage = None if pd.isna(row.gasInStorage) else float(row.gasInStorage)
        full = None if pd.isna(row.full) else float(row.full)

        operations.append(
            UpdateOne(
                {"day": day_value, "code": code},
                {"$set": {"day": day_value, "code": code, "gasInStorage": gas_in_storage, "full": full}},
                upsert=True,
            )
        )

    result = collection.bulk_write(operations, ordered=False)
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "upserted": len(result.upserted_ids),
    }


def main() -> None:
    args = parse_args()
    load_dotenv()
    api_key = os.getenv("AGSI_API_KEY", "").strip()

    if not api_key:
        raise SystemExit("Missing AGSI_API_KEY in environment or .env file.")
    if args.days < 1:
        raise SystemExit("--days must be >= 1")

    test_mongo_connection()
    end_date = dt.date.today()
    if args.mode == "backfill":
        start_date = BACKFILL_START_DATE
    else:
        start_date = end_date - dt.timedelta(days=args.days - 1)

    df = fetch_storage_dataframe(api_key, start_date=start_date, end_date=end_date)
    if df.empty:
        print("No rows returned.")
        return

    print(
        f"Fetched {len(df)} rows for {df['code'].nunique() if 'code' in df.columns else 'n/a'} countries."
    )
    sync_stats = sync_gas_storage_to_mongo(df, collection_name="gas_storage")
    print(
        "Mongo sync complete "
        f"(matched={sync_stats['matched']}, modified={sync_stats['modified']}, upserted={sync_stats['upserted']})."
    )


if __name__ == "__main__":
    main()
