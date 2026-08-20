"""Stream ENTSOG archive Physical Flow CSVs into raw observations."""

from __future__ import annotations

import argparse
import datetime as dt
import io
import sys
import time
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from pymongo import UpdateOne
from pymongo.errors import PyMongoError
from requests import JSONDecodeError, RequestException

from modern_pipeline.db import get_database
from modern_pipeline.scripts.fetch_entsog_raw_observations import (
    add_legacy_locations_universe,
    add_modern_universe,
    add_points_safe_universe,
    point_direction_parts,
)

ARCHIVE_LIST_URL = "https://transparency.entsog.eu/api/v1/archives"
ARCHIVE_DOWNLOAD_BASE_URL = "https://transparency.entsog.eu/api/archiveDirectories/16"
ARCHIVE_TYPE_ALL_TSO_YEARLY = 16
DEFAULT_YEARS = (2016, 2017, 2018, 2019, 2020)
DEFAULT_LEGACY_LOCATIONS = "working data/locationsSAFE.xlsx"
DEFAULT_POINTS_SAFE = "working data/pointsSAFE_EU27.xlsx"
REQUEST_TIMEOUT = 90
MIN_REQUEST_INTERVAL_SEC = 1.1
MAX_RETRIES = 5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
MONGO_WRITE_ATTEMPTS = 5
CHUNKSIZE = 100_000
ARCHIVE_EIC_ALIASES = {
    # ENTSOG archive files for 2016-2019 report Irish Moffat entry rows with
    # Point EIC "?" rather than the modern Moffat point EIC. Keep this alias
    # deliberately narrow so it cannot catch unrelated GNI rows.
    "ie-tso-0002itp-00495entry": [
        {
            "operatorEic": "47X0000000000576",
            "pointEic": "?",
            "direction": "entry",
            "reason": "Archive-era Moffat (IE) rows use Point EIC '?'",
        }
    ],
    # ENTSOG archive files for 2016-2019 report Dornum / NETRA (GUD)
    # with Point EIC "?" rather than the modern Dornum operational item
    # identifier. Require the exact point name because the same German operator
    # publishes many unrelated archive rows with Point EIC "?".
    "de-tso-0005itp-00188entry": [
        {
            "operatorEic": "21X-DE-D-A0A0A-K",
            "pointEic": "?",
            "pointName": "Dornum / NETRA (GUD)",
            "direction": "entry",
            "reason": "Archive-era Dornum / NETRA (GUD) rows use Point EIC '?'",
        }
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream ENTSOG annual archive CSVs and upsert matching Physical Flow observations."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Stream archive CSVs and count matching rows without writing. This is the default.",
    )
    mode.add_argument(
        "--write",
        action="store_true",
        help="Stream archive CSVs and upsert matching rows to MongoDB.",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="List archive files and mapping size without streaming CSV contents.",
    )
    parser.add_argument(
        "--year",
        type=int,
        action="append",
        help="Archive calendar year to ingest. Can be passed multiple times. Defaults to 2016-2020.",
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
    parser.add_argument(
        "--point-direction",
        action="append",
        dest="point_directions",
        help="Restrict archive ingest to a specific pointDirection. Can be passed multiple times.",
    )
    parser.add_argument(
        "--limit-rows",
        type=int,
        default=None,
        help="Stop after reading this many CSV rows per archive file. Useful for smoke tests.",
    )
    parser.add_argument(
        "--max-chunks",
        type=int,
        default=None,
        help="Stop after this many chunks per archive file. Useful for smoke tests.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=CHUNKSIZE,
        help=f"CSV rows per streaming chunk. Default: {CHUNKSIZE}.",
    )
    return parser.parse_args()


def format_duration(seconds: float) -> str:
    seconds = max(0, seconds)
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    return f"{minutes / 60:.1f}h"


def build_universe(args: argparse.Namespace) -> List[Dict[str, Any]]:
    universe: Dict[str, Dict[str, Any]] = {}
    add_modern_universe(universe, args.connection_points_collection)
    add_legacy_locations_universe(universe, args.legacy_locations)
    if not args.skip_points_safe:
        add_points_safe_universe(universe, args.points_safe)

    if args.point_directions:
        wanted = {item.lower() for item in args.point_directions}
        universe = {pdid: item for pdid, item in universe.items() if pdid in wanted}

    rows = []
    for item in universe.values():
        row = dict(item)
        row["sourceSets"] = sorted(row["sourceSets"])
        rows.append(row)
    return sorted(rows, key=lambda item: item["pointDirection"])


def archive_listing() -> Dict[int, str]:
    payload = get_json_with_retries(
        ARCHIVE_LIST_URL,
        params={"archive_type": ARCHIVE_TYPE_ALL_TSO_YEARLY},
    )
    files: Dict[int, str] = {}
    for archive in payload.get("archives") or []:
        if not isinstance(archive, dict):
            continue
        for link in archive.get("links") or []:
            text = str(link)
            for year in DEFAULT_YEARS:
                if str(year) in text and "PhysicalFlow" in text:
                    files[year] = text
    return files


def get_json_with_retries(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    session = requests.Session()
    for attempt in range(1, MAX_RETRIES + 1):
        if attempt > 1:
            time.sleep(MIN_REQUEST_INTERVAL_SEC)
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except RequestException as exc:
            if attempt >= MAX_RETRIES:
                raise RuntimeError(f"ENTSOG archive-list request failed after retries: {exc}") from exc
            print(
                f"ENTSOG archive-list request failed: {exc}. Waiting {MIN_REQUEST_INTERVAL_SEC:.1f}s "
                f"(attempt {attempt}/{MAX_RETRIES}).",
                file=sys.stderr,
            )
            continue

        if response.status_code in RETRY_STATUS_CODES and attempt < MAX_RETRIES:
            retry_after = response.headers.get("Retry-After")
            wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else MIN_REQUEST_INTERVAL_SEC
            print(
                f"Retryable ENTSOG archive-list response {response.status_code}; waiting {wait_seconds:.1f}s "
                f"(attempt {attempt}/{MAX_RETRIES}).",
                file=sys.stderr,
            )
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        try:
            payload = response.json()
        except JSONDecodeError as exc:
            raise RuntimeError("ENTSOG archive-list endpoint returned non-JSON response.") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected non-dict ENTSOG archive-list response.")
        return payload
    raise RuntimeError("Failed to fetch ENTSOG archive listing after retries.")


def archive_download_url(filename: str) -> str:
    return f"{ARCHIVE_DOWNLOAD_BASE_URL}/{urllib.parse.quote(filename)}"


def normalize_eic(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip().upper()


def normalize_direction(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_point_name(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return " ".join(str(value).strip().casefold().split())


def add_lookup_item(
    lookup: Dict[Tuple[str, str, str], Dict[str, Any]],
    point_direction: str,
    operator_eic: Any,
    point_eic: Any,
    direction: Any,
    metadata: Dict[str, Any],
) -> None:
    operator_eic_norm = normalize_eic(operator_eic)
    point_eic_norm = normalize_eic(point_eic)
    direction_norm = normalize_direction(direction)
    if not operator_eic_norm or not point_eic_norm or direction_norm not in {"entry", "exit"}:
        return
    key = (operator_eic_norm, point_eic_norm, direction_norm)
    lookup.setdefault(key, {"pointDirection": point_direction, "metadata": metadata})


def add_named_alias_item(
    lookup: Dict[Tuple[str, str, str, str], Dict[str, Any]],
    point_direction: str,
    operator_eic: Any,
    point_eic: Any,
    point_name: Any,
    direction: Any,
    metadata: Dict[str, Any],
) -> None:
    operator_eic_norm = normalize_eic(operator_eic)
    point_eic_norm = normalize_eic(point_eic)
    point_name_norm = normalize_point_name(point_name)
    direction_norm = normalize_direction(direction)
    if (
        not operator_eic_norm
        or not point_eic_norm
        or not point_name_norm
        or direction_norm not in {"entry", "exit"}
    ):
        return
    key = (operator_eic_norm, point_eic_norm, direction_norm, point_name_norm)
    lookup.setdefault(key, {"pointDirection": point_direction, "metadata": metadata})


def build_eic_lookup(universe: Sequence[Dict[str, Any]], collection_name: str) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    wanted = {item["pointDirection"] for item in universe}
    universe_by_pd = {item["pointDirection"]: item for item in universe}
    lookup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    db = get_database()

    for doc in db["entsog_connection_points"].find(
        {"pointDirections.pointDirection": {"$in": list(wanted)}},
        {"pointDirections": 1},
    ):
        for opd in doc.get("pointDirections") or []:
            point_direction = str(opd.get("pointDirection") or "").lower()
            if point_direction not in wanted:
                continue
            add_lookup_item(
                lookup,
                point_direction,
                opd.get("tsoEicCode"),
                opd.get("tsoItemIdentifier"),
                opd.get("directionKey"),
                universe_by_pd.get(point_direction, {}),
            )

    for raw in db[collection_name].find(
        {
            "pointDirection": {"$in": list(wanted)},
            "tsoEicCode": {"$exists": True},
            "tsoItemIdentifier": {"$exists": True},
        },
        {"pointDirection": 1, "tsoEicCode": 1, "tsoItemIdentifier": 1, "directionKey": 1},
    ):
        point_direction = str(raw.get("pointDirection") or "").lower()
        if point_direction not in wanted:
            continue
        add_lookup_item(
            lookup,
            point_direction,
            raw.get("tsoEicCode"),
            raw.get("tsoItemIdentifier"),
            raw.get("directionKey"),
            universe_by_pd.get(point_direction, {}),
        )

    for point_direction, aliases in ARCHIVE_EIC_ALIASES.items():
        if point_direction not in wanted:
            continue
        metadata = dict(universe_by_pd.get(point_direction, {}))
        for alias in aliases:
            if alias.get("pointName"):
                continue
            alias_metadata = dict(metadata)
            alias_metadata["metadata"] = {
                **(metadata.get("metadata") or {}),
                "archiveAliasReason": alias.get("reason"),
            }
            add_lookup_item(
                lookup,
                point_direction,
                alias.get("operatorEic"),
                alias.get("pointEic"),
                alias.get("direction"),
                alias_metadata,
            )

    return lookup


def build_named_alias_lookup(universe: Sequence[Dict[str, Any]]) -> Dict[Tuple[str, str, str, str], Dict[str, Any]]:
    wanted = {item["pointDirection"] for item in universe}
    universe_by_pd = {item["pointDirection"]: item for item in universe}
    lookup: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for point_direction, aliases in ARCHIVE_EIC_ALIASES.items():
        if point_direction not in wanted:
            continue
        metadata = dict(universe_by_pd.get(point_direction, {}))
        for alias in aliases:
            if not alias.get("pointName"):
                continue
            alias_metadata = dict(metadata)
            alias_metadata["metadata"] = {
                **(metadata.get("metadata") or {}),
                "archiveAliasReason": alias.get("reason"),
                "archiveAliasPointName": alias.get("pointName"),
            }
            add_named_alias_item(
                lookup,
                point_direction,
                alias.get("operatorEic"),
                alias.get("pointEic"),
                alias.get("pointName"),
                alias.get("direction"),
                alias_metadata,
            )
    return lookup


def parse_archive_datetime(value: Any) -> Optional[dt.datetime]:
    if value in (None, ""):
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    text = str(value).strip()
    if not text:
        return None
    date_text = text[:10]
    try:
        date_value = dt.date.fromisoformat(date_text)
    except ValueError:
        return None
    return dt.datetime.combine(date_value, dt.time.min)


def clean_float(value: Any) -> Optional[float]:
    if value in (None, "", "-", "NULL"):
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    text = str(value).strip()
    if text.count(".") > 1 and "," not in text:
        compact = text.replace(".", "")
        if compact.isdigit():
            return float(compact) / 1_000_000.0
    try:
        return float(text.replace(",", "."))
    except (TypeError, ValueError):
        return None


def normalize_archive_observation(
    row: Dict[str, Any],
    lookup_item: Dict[str, Any],
    fetched_at: dt.datetime,
    archive_year: int,
    archive_file: str,
) -> Optional[Dict[str, Any]]:
    point_direction = lookup_item["pointDirection"]
    parts = point_direction_parts(point_direction)
    period_from = parse_archive_datetime(row.get("Period From"))
    period_to = parse_archive_datetime(row.get("Period To"))
    if not period_from:
        return None
    value = clean_float(row.get("value", row.get("Value")))
    metadata = lookup_item.get("metadata") or {}
    return {
        "provider": "ENTSOG",
        "sourceTransport": "archive_csv",
        "archiveYear": archive_year,
        "archiveFile": archive_file,
        "pointDirection": point_direction,
        "operatorKey": parts.get("operatorKey"),
        "pointKey": parts.get("pointKey"),
        "directionKey": parts.get("directionKey"),
        "indicator": "Physical Flow",
        "periodType": "day",
        "periodFrom": period_from,
        "periodTo": period_to,
        "gasDay": period_from,
        "unit": row.get("Unit"),
        "value": value,
        "flowStatus": None,
        "tsoEicCode": normalize_eic(row.get("Operator EIC")),
        "operatorLabel": row.get("Operator Name"),
        "pointLabel": row.get("Point name"),
        "tsoItemIdentifier": normalize_eic(row.get("Point EIC")),
        "lastUpdateDateTime": None,
        "isNA": None,
        "isArchived": True,
        "sourceSets": metadata.get("sourceSets", []),
        "sourceMetadata": metadata.get("metadata", {}),
        "raw": row,
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
    operations = [
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
        for obs in observations
    ]
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


def download_archive_bytes(url: str) -> bytes:
    for attempt in range(1, MAX_RETRIES + 1):
        if attempt > 1:
            time.sleep(MIN_REQUEST_INTERVAL_SEC)
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
        except RequestException as exc:
            if attempt >= MAX_RETRIES:
                raise RuntimeError(f"ENTSOG archive download failed after retries: {exc}") from exc
            print(
                f"ENTSOG archive download failed: {exc}. Waiting {MIN_REQUEST_INTERVAL_SEC:.1f}s "
                f"(attempt {attempt}/{MAX_RETRIES}).",
                file=sys.stderr,
            )
            continue

        if response.status_code in RETRY_STATUS_CODES and attempt < MAX_RETRIES:
            retry_after = response.headers.get("Retry-After")
            wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else MIN_REQUEST_INTERVAL_SEC
            print(
                f"Retryable ENTSOG archive download response {response.status_code}; "
                f"waiting {wait_seconds:.1f}s (attempt {attempt}/{MAX_RETRIES}).",
                file=sys.stderr,
            )
            time.sleep(wait_seconds)
            continue
        response.raise_for_status()
        return response.content

    raise RuntimeError("Failed to download ENTSOG archive after retries.")


def read_archive_chunks_from_bytes(archive_bytes: bytes, chunksize: int, encoding: str) -> Iterable[pd.DataFrame]:
    text_stream = io.TextIOWrapper(io.BytesIO(archive_bytes), encoding=encoding, newline="")
    yield from pd.read_csv(
        text_stream,
        sep=";",
        chunksize=chunksize,
        dtype=str,
        engine="python",
        on_bad_lines="warn",
    )


def stream_archive_chunks(url: str, chunksize: int) -> Iterable[pd.DataFrame]:
    archive_bytes = download_archive_bytes(url)
    try:
        yield from read_archive_chunks_from_bytes(archive_bytes, chunksize, "utf-8-sig")
    except UnicodeDecodeError:
        print("  UTF-8 decoding failed; retrying archive parse with cp1252.", file=sys.stderr)
        yield from read_archive_chunks_from_bytes(archive_bytes, chunksize, "cp1252")


def archive_row_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        normalize_eic(row.get("Operator EIC")),
        normalize_eic(row.get("Point EIC")),
        normalize_direction(row.get("Direction")),
    )


def archive_named_alias_row_key(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        normalize_eic(row.get("Operator EIC")),
        normalize_eic(row.get("Point EIC")),
        normalize_direction(row.get("Direction")),
        normalize_point_name(row.get("Point name")),
    )


def normalize_indicator(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return "".join(str(value).strip().lower().split())


def is_physical_flow_chunk(chunk: pd.DataFrame) -> pd.Series:
    indicator = None
    if "Indicator" in chunk.columns:
        indicator = chunk["Indicator"].map(normalize_indicator)
    if "NmIndicatorType" in chunk.columns:
        nm_indicator = chunk["NmIndicatorType"].map(normalize_indicator)
        indicator = nm_indicator if indicator is None else indicator.combine_first(nm_indicator)
    if indicator is None:
        return pd.Series([True] * len(chunk), index=chunk.index)
    return indicator.eq("physicalflow")


def process_archive_file(
    year: int,
    filename: str,
    lookup: Dict[Tuple[str, str, str], Dict[str, Any]],
    named_alias_lookup: Dict[Tuple[str, str, str, str], Dict[str, Any]],
    collection_name: str,
    chunksize: int,
    limit_rows: Optional[int],
    max_chunks: Optional[int],
    write: bool,
) -> Dict[str, int]:
    url = archive_download_url(filename)
    fetched_at = dt.datetime.now(dt.timezone.utc)
    total_read = 0
    total_matched = 0
    total_modified = 0
    total_upserted = 0
    chunk_count = 0
    started_at = time.monotonic()
    print(f"\nStreaming {year}: {filename}")
    print(f"  URL: {url}", flush=True)

    for chunk in stream_archive_chunks(url, chunksize):
        chunk_count += 1
        rows_remaining = None if limit_rows is None else max(limit_rows - total_read, 0)
        if rows_remaining == 0:
            break
        if rows_remaining is not None and len(chunk) > rows_remaining:
            chunk = chunk.head(rows_remaining)
        total_read += len(chunk)

        chunk = chunk[is_physical_flow_chunk(chunk)]

        observations = []
        for row in chunk.to_dict("records"):
            lookup_item = lookup.get(archive_row_key(row))
            if not lookup_item:
                lookup_item = named_alias_lookup.get(archive_named_alias_row_key(row))
            if not lookup_item:
                continue
            obs = normalize_archive_observation(row, lookup_item, fetched_at, year, filename)
            if obs:
                observations.append(obs)

        total_matched += len(observations)
        stats = upsert_observations(collection_name, observations) if write else {"modified": 0, "upserted": 0}
        total_modified += stats["modified"]
        total_upserted += stats["upserted"]
        elapsed = time.monotonic() - started_at
        print(
            f"  chunk={chunk_count} rows_read={total_read} matched={total_matched} "
            f"modified={total_modified} upserted={total_upserted} elapsed={format_duration(elapsed)}",
            flush=True,
        )

        if max_chunks is not None and chunk_count >= max_chunks:
            print("  reached --max-chunks; stopping this file.")
            break
        if limit_rows is not None and total_read >= limit_rows:
            print("  reached --limit-rows; stopping this file.")
            break

    return {
        "rows_read": total_read,
        "matched": total_matched,
        "modified": total_modified,
        "upserted": total_upserted,
        "chunks": chunk_count,
    }


def summarize_plan(
    years: Sequence[int],
    files: Dict[int, str],
    universe: Sequence[Dict[str, Any]],
    lookup: Dict[Tuple[str, str, str], Dict[str, Any]],
    named_alias_lookup: Dict[Tuple[str, str, str, str], Dict[str, Any]],
) -> None:
    print(f"Archive years requested: {', '.join(str(year) for year in years)}")
    print(f"Universe pointDirections: {len(universe)}")
    print(f"EIC lookup keys: {len(lookup)}")
    print(f"Named archive alias keys: {len(named_alias_lookup)}")
    print("\nArchive files:")
    for year in years:
        filename = files.get(year)
        print(f"  {year}: {filename or 'MISSING'}")

    missing_pds = sorted(
        {item["pointDirection"] for item in universe}
        - {item["pointDirection"] for item in lookup.values()}
        - {item["pointDirection"] for item in named_alias_lookup.values()}
    )
    if missing_pds:
        print(f"\nPointDirections without EIC lookup: {len(missing_pds)}")
        for point_direction in missing_pds[:30]:
            print(f"  {point_direction}")


def main() -> None:
    args = parse_args()
    dry_run = not args.write
    years = sorted(set(args.year or DEFAULT_YEARS))
    files = archive_listing()
    missing_years = [year for year in years if year not in files]
    if missing_years:
        raise RuntimeError(f"No archive file found for year(s): {missing_years}")

    universe = build_universe(args)
    if not universe:
        raise RuntimeError("Archive universe is empty.")
    lookup = build_eic_lookup(universe, args.collection)
    named_alias_lookup = build_named_alias_lookup(universe)
    summarize_plan(years, files, universe, lookup, named_alias_lookup)

    if args.plan_only:
        print("\nPlan only. No archive CSVs streamed and no MongoDB writes performed.")
        return

    if args.chunksize < 1:
        raise RuntimeError("--chunksize must be >= 1")
    if args.write:
        ensure_indexes(args.collection)
    else:
        print("\nDry run: archive CSVs will be streamed and matched, but MongoDB writes are disabled.")

    total = {"rows_read": 0, "matched": 0, "modified": 0, "upserted": 0, "chunks": 0}
    started_at = time.monotonic()
    for year in years:
        stats = process_archive_file(
            year=year,
            filename=files[year],
            lookup=lookup,
            named_alias_lookup=named_alias_lookup,
            collection_name=args.collection,
            chunksize=args.chunksize,
            limit_rows=args.limit_rows,
            max_chunks=args.max_chunks,
            write=args.write,
        )
        for key, value in stats.items():
            total[key] += value

    elapsed = time.monotonic() - started_at
    print(
        "\nComplete. "
        f"rows_read={total['rows_read']}, matched={total['matched']}, "
        f"modified={total['modified']}, upserted={total['upserted']}, "
        f"chunks={total['chunks']}, elapsed={format_duration(elapsed)}"
    )


if __name__ == "__main__":
    main()
