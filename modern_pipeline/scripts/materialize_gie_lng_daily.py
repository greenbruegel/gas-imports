"""Materialize curated daily LNG import series from raw GIE ALSI observations."""

from __future__ import annotations

import argparse
import datetime as dt
import time
from collections import defaultdict
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Sequence, Tuple

from pymongo import UpdateOne

from modern_pipeline.db import get_database

DEFAULT_MANIFEST_VERSION = "gie_alsi_lng_terminals_v1"
DEFAULT_CONVERTER_GWH_PER_MCM = 11.58


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build LNG rows in gas_import_daily from gie_lng_raw_observations and a GIE ALSI manifest."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Summarize the materialisation without writing. This is the default.",
    )
    mode.add_argument(
        "--write",
        action="store_true",
        help="Upsert curated daily LNG observations into MongoDB.",
    )
    parser.add_argument(
        "--manifest-version",
        default=DEFAULT_MANIFEST_VERSION,
        help=f"LNG terminal manifest version to materialize. Default: {DEFAULT_MANIFEST_VERSION}.",
    )
    parser.add_argument(
        "--manifest-collection",
        default="gie_lng_terminal_manifest",
        help="MongoDB LNG manifest collection. Default: gie_lng_terminal_manifest.",
    )
    parser.add_argument(
        "--raw-collection",
        default="gie_lng_raw_observations",
        help="MongoDB raw LNG observations collection. Default: gie_lng_raw_observations.",
    )
    parser.add_argument(
        "--output-collection",
        default="gas_import_daily",
        help="MongoDB curated output collection. Default: gas_import_daily.",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        default=None,
        help="Optional start date, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        default=None,
        help="Optional end date, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--country",
        action="append",
        dest="countries",
        help="Restrict to one country ISO2 code. Can be passed multiple times.",
    )
    parser.add_argument(
        "--include-facilities",
        action="store_true",
        help="Also write daily facility-level rows for audit/debugging.",
    )
    parser.add_argument(
        "--delete-existing-window",
        action="store_true",
        help=(
            "Before writing, delete existing rows for this manifest version and date window. "
            "Useful when manifest entries changed."
        ),
    )
    parser.add_argument(
        "--converter-gwh-per-mcm",
        type=float,
        default=None,
        help=(
            "Compatibility conversion from GWh to million cubic metres. "
            f"Default: manifest scope value if present, otherwise {DEFAULT_CONVERTER_GWH_PER_MCM}."
        ),
    )
    return parser.parse_args()


def parse_date(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    return dt.datetime.combine(dt.date.fromisoformat(value), dt.time.min)


def format_duration(seconds: float) -> str:
    seconds = max(0, seconds)
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    return f"{minutes / 60:.1f}h"


def load_manifest(collection_name: str, version: str) -> Dict[str, Any]:
    db = get_database()
    manifest = db[collection_name].find_one({"version": version})
    if not manifest:
        raise RuntimeError(f"LNG terminal manifest version not found: {version}")
    return manifest


def resolve_converter(manifest: Dict[str, Any], cli_converter: Optional[float]) -> float:
    if cli_converter is not None:
        return cli_converter
    value = (manifest.get("scope") or {}).get("converterGwhPerMcm")
    try:
        converter = float(value)
    except (TypeError, ValueError):
        converter = DEFAULT_CONVERTER_GWH_PER_MCM
    return converter if converter > 0 else DEFAULT_CONVERTER_GWH_PER_MCM


def selected_entries(manifest: Dict[str, Any], countries: Optional[Sequence[str]]) -> List[Dict[str, Any]]:
    wanted = {country.upper() for country in countries or []}
    entries = []
    seen = set()
    for entry in manifest.get("entries") or []:
        if not isinstance(entry, dict):
            continue
        if not entry.get("selected", True):
            continue
        country_iso2 = str(entry.get("countryIso2", "")).upper()
        country_raw = str(entry.get("countryCodeRaw", "")).upper()
        if wanted and country_iso2 not in wanted and country_raw not in wanted:
            continue
        facility_eic = str(entry.get("facilityEic") or "").strip()
        if not facility_eic or facility_eic in seen:
            continue
        seen.add(facility_eic)
        entries.append({**entry, "facilityEic": facility_eic})
    return sorted(entries, key=lambda item: (item.get("countryIso2", ""), item.get("facilityName", "")))


def date_query(from_date: Optional[dt.datetime], to_date: Optional[dt.datetime]) -> Dict[str, Any]:
    query: Dict[str, Any] = {}
    if from_date:
        query["$gte"] = from_date
    if to_date:
        query["$lte"] = to_date
    return query


def fetch_raw_daily(
    raw_collection_name: str,
    facility_eics: Sequence[str],
    from_date: Optional[dt.datetime],
    to_date: Optional[dt.datetime],
) -> List[Dict[str, Any]]:
    db = get_database()
    query: Dict[str, Any] = {"facilityEic": {"$in": list(facility_eics)}}
    gas_day_query = date_query(from_date, to_date)
    if gas_day_query:
        query["gasDay"] = gas_day_query

    projection = {
        "_id": 0,
        "facilityEic": 1,
        "gasDay": 1,
        "sendOutGwhPerDay": 1,
        "valueGwh": 1,
        "valueMcm": 1,
        "countryIso2": 1,
        "facilityName": 1,
    }
    return list(db[raw_collection_name].find(query, projection).sort([("gasDay", 1), ("facilityEic", 1)]))


def day_key(value: dt.datetime) -> dt.datetime:
    return dt.datetime.combine(value.date(), dt.time.min)


def iso_calendar_fields(day: dt.datetime) -> Dict[str, int]:
    iso = day.date().isocalendar()
    return {"isoYear": int(iso.year), "isoWeek": int(iso.week), "isoWeekday": int(iso.weekday)}


def init_aggregate(
    manifest_version: str,
    day: dt.datetime,
    aggregation_level: str,
    series_key: str,
    series_label: str,
    converter: float,
) -> Dict[str, Any]:
    return {
        "manifestVersion": manifest_version,
        "provider": "GIE_ALSI",
        "date": day,
        "granularity": "day",
        "aggregationLevel": aggregation_level,
        "seriesKey": series_key,
        "seriesLabel": series_label,
        "flowConcept": "gross_import",
        "valueKwh": 0.0,
        "valueGwh": 0.0,
        "valueTwh": 0.0,
        "valueMcm": 0.0,
        "converterGwhPerMcm": converter,
        "sourceFacilities": set(),
        "sourceGroups": {"LNG"},
        "updatedAt": dt.datetime.now(dt.timezone.utc),
        **iso_calendar_fields(day),
    }


def add_value(target: Dict[str, Any], value_gwh: float, facility_eic: str) -> None:
    target["valueGwh"] += value_gwh
    target["sourceFacilities"].add(facility_eic)


def materialize_rows(
    manifest_version: str,
    entries: List[Dict[str, Any]],
    raw_rows: Iterable[Dict[str, Any]],
    converter: float,
    include_facilities: bool,
) -> List[Dict[str, Any]]:
    entry_by_facility = {entry["facilityEic"]: entry for entry in entries}
    aggregates: Dict[Tuple[dt.datetime, str, str], Dict[str, Any]] = {}

    for raw in raw_rows:
        facility_eic = raw.get("facilityEic")
        entry = entry_by_facility.get(facility_eic)
        gas_day = raw.get("gasDay")
        if not entry or not isinstance(gas_day, dt.datetime):
            continue
        value = raw.get("valueGwh")
        if value in (None, ""):
            value = raw.get("sendOutGwhPerDay")
        if value in (None, ""):
            continue
        try:
            value_gwh = float(value) * float(entry.get("sign", 1))
        except (TypeError, ValueError):
            continue

        day = day_key(gas_day)
        country_iso2 = entry.get("countryIso2") or raw.get("countryIso2") or "Unknown"
        keys = [
            ("source_group", "LNG", "LNG"),
            ("source_group_country", f"LNG:{country_iso2}", f"LNG {country_iso2}"),
        ]
        if include_facilities:
            label = entry.get("facilityName") or facility_eic
            keys.append(("facility", f"LNG:{facility_eic}", label))

        for aggregation_level, series_key, series_label in keys:
            agg_key = (day, aggregation_level, series_key)
            target = aggregates.get(agg_key)
            if not target:
                target = init_aggregate(
                    manifest_version,
                    day,
                    aggregation_level,
                    series_key,
                    series_label,
                    converter,
                )
                aggregates[agg_key] = target
            add_value(target, value_gwh, facility_eic)

    rows = []
    for row in aggregates.values():
        row["valueKwh"] = row["valueGwh"] * 1_000_000.0
        row["valueTwh"] = row["valueGwh"] / 1_000.0
        row["valueMcm"] = row["valueGwh"] / converter
        row["sourceFacilities"] = sorted(v for v in row["sourceFacilities"] if v)
        row["sourceGroups"] = sorted(v for v in row["sourceGroups"] if v)
        rows.append(row)

    return sorted(rows, key=lambda item: (item["date"], item["aggregationLevel"], item["seriesKey"]))


def ensure_indexes(collection_name: str) -> None:
    db = get_database()
    collection = db[collection_name]
    old_key = {
        "manifestVersion": 1,
        "date": 1,
        "aggregationLevel": 1,
        "seriesKey": 1,
    }
    for index in collection.list_indexes():
        if index.get("unique") and dict(index.get("key", {})) == old_key:
            collection.drop_index(index["name"])
    collection.create_index(
        [("manifestVersion", 1), ("date", 1), ("aggregationLevel", 1), ("seriesKey", 1), ("flowConcept", 1)],
        unique=True,
    )
    collection.create_index([("manifestVersion", 1), ("aggregationLevel", 1), ("seriesKey", 1), ("flowConcept", 1), ("date", 1)])
    collection.create_index([("manifestVersion", 1), ("isoYear", 1), ("isoWeek", 1)])


def delete_existing_window(
    collection_name: str,
    manifest_version: str,
    from_date: Optional[dt.datetime],
    to_date: Optional[dt.datetime],
) -> int:
    db = get_database()
    query: Dict[str, Any] = {"manifestVersion": manifest_version}
    date_filter = date_query(from_date, to_date)
    if date_filter:
        query["date"] = date_filter
    result = db[collection_name].delete_many(query)
    return result.deleted_count


def upsert_rows(collection_name: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    if not rows:
        return {"matched": 0, "modified": 0, "upserted": 0}

    operations = []
    for row in rows:
        operations.append(
            UpdateOne(
                {
                    "manifestVersion": row["manifestVersion"],
                    "date": row["date"],
                    "aggregationLevel": row["aggregationLevel"],
                    "seriesKey": row["seriesKey"],
                    "flowConcept": row["flowConcept"],
                },
                {"$set": row},
                upsert=True,
            )
        )

    db = get_database()
    result = db[collection_name].bulk_write(operations, ordered=False)
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "upserted": len(result.upserted_ids),
    }


def summarize(entries: List[Dict[str, Any]], raw_rows: List[Dict[str, Any]], materialized: List[Dict[str, Any]]) -> None:
    by_country: DefaultDict[str, int] = defaultdict(int)
    by_status: DefaultDict[str, int] = defaultdict(int)
    for entry in entries:
        by_country[entry.get("countryIso2") or "Unknown"] += 1
        by_status[entry.get("facilityStatus") or "unknown"] += 1

    date_values = [row["date"] for row in materialized]
    print(f"Manifest facilities selected: {len(entries)}")
    print(f"Raw daily observations found: {len(raw_rows)}")
    print(f"Curated daily rows built: {len(materialized)}")
    if date_values:
        print(f"Date coverage: {min(date_values).date()} to {max(date_values).date()}")

    print("\nManifest facilities by countryIso2:")
    for key, count in sorted(by_country.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nManifest facilities by facilityStatus:")
    for key, count in sorted(by_status.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    latest_source_rows = [
        row
        for row in materialized
        if row["aggregationLevel"] == "source_group"
        and date_values
        and row["date"] == max(date_values)
    ]
    latest_country_rows = [
        row
        for row in materialized
        if row["aggregationLevel"] == "source_group_country"
        and date_values
        and row["date"] == max(date_values)
    ]
    if latest_source_rows:
        print("\nLatest LNG total:")
        for row in sorted(latest_source_rows, key=lambda item: item["seriesKey"]):
            print(f"  {row['date'].date()} {row['seriesKey']}: {row['valueMcm']:.1f} mcm")
    if latest_country_rows:
        print("\nLatest LNG country values:")
        for row in sorted(latest_country_rows, key=lambda item: item["seriesKey"]):
            print(f"  {row['date'].date()} {row['seriesKey']}: {row['valueMcm']:.1f} mcm")


def main() -> None:
    args = parse_args()
    started_at = time.monotonic()
    from_date = parse_date(args.from_date)
    to_date = parse_date(args.to_date)

    manifest = load_manifest(args.manifest_collection, args.manifest_version)
    converter = resolve_converter(manifest, args.converter_gwh_per_mcm)
    entries = selected_entries(manifest, args.countries)
    if not entries:
        raise RuntimeError(f"No selected LNG manifest entries for version {args.manifest_version}")

    raw_rows = fetch_raw_daily(
        args.raw_collection,
        [entry["facilityEic"] for entry in entries],
        from_date,
        to_date,
    )
    materialized = materialize_rows(
        args.manifest_version,
        entries,
        raw_rows,
        converter,
        args.include_facilities,
    )
    summarize(entries, raw_rows, materialized)

    if not args.write:
        print("\nDry run only. No MongoDB writes performed.")
        return

    ensure_indexes(args.output_collection)
    deleted = 0
    if args.delete_existing_window:
        deleted = delete_existing_window(args.output_collection, args.manifest_version, from_date, to_date)
    stats = upsert_rows(args.output_collection, materialized)
    elapsed = time.monotonic() - started_at
    print(
        "\nMongo materialization complete "
        f"(deleted={deleted}, matched={stats['matched']}, modified={stats['modified']}, "
        f"upserted={stats['upserted']}, elapsed={format_duration(elapsed)})."
    )


if __name__ == "__main__":
    main()
