"""Materialize curated daily gas import series from raw ENTSOG observations."""

from __future__ import annotations

import argparse
import datetime as dt
import time
from collections import defaultdict
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Sequence, Tuple

from pymongo import UpdateOne

from modern_pipeline.db import get_database

DEFAULT_MANIFEST_VERSION = "legacy_excel_v1"
DEFAULT_CONVERTER_KWH_PER_MCM = 10_300_000.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build gas_import_daily from entsog_raw_observations and a versioned manifest."
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
        help="Upsert curated daily observations into MongoDB.",
    )
    parser.add_argument(
        "--manifest-version",
        default=DEFAULT_MANIFEST_VERSION,
        help=f"Manifest version to materialize. Default: {DEFAULT_MANIFEST_VERSION}.",
    )
    parser.add_argument(
        "--manifest-collection",
        default="entsog_import_manifest",
        help="MongoDB manifest collection. Default: entsog_import_manifest.",
    )
    parser.add_argument(
        "--raw-collection",
        default="entsog_raw_observations",
        help="MongoDB raw-observations collection. Default: entsog_raw_observations.",
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
        "--source-group",
        action="append",
        dest="source_groups",
        help="Restrict to one sourceGroup. Can be passed multiple times.",
    )
    parser.add_argument(
        "--include-route-groups",
        action="store_true",
        help="Also write daily route_group series, useful for Russian route debugging.",
    )
    parser.add_argument(
        "--include-point-directions",
        action="store_true",
        help="Also write daily point_direction contribution rows for audit/debugging.",
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
        "--converter-kwh-per-mcm",
        type=float,
        default=DEFAULT_CONVERTER_KWH_PER_MCM,
        help=(
            "Compatibility conversion from kWh to million cubic metres. "
            f"Default: {DEFAULT_CONVERTER_KWH_PER_MCM:,.0f}."
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
        raise RuntimeError(f"Manifest version not found: {version}")
    return manifest


def selected_entries(manifest: Dict[str, Any], source_groups: Optional[Sequence[str]]) -> List[Dict[str, Any]]:
    wanted = set(source_groups or [])
    entries = []
    seen = set()
    for entry in manifest.get("entries") or []:
        if not entry.get("selected", True):
            continue
        if wanted and entry.get("sourceGroup") not in wanted:
            continue
        point_direction = str(entry.get("pointDirection") or "").strip().lower()
        if not point_direction or point_direction in seen:
            continue
        seen.add(point_direction)
        entries.append({**entry, "pointDirection": point_direction, "flowConcept": "gross_import"})
    return entries


def selected_counterflow_entries(manifest: Dict[str, Any], source_groups: Optional[Sequence[str]]) -> List[Dict[str, Any]]:
    wanted = set(source_groups or [])
    entries = []
    seen = set()
    for entry in manifest.get("counterflowEntries") or []:
        if not entry.get("selected", True):
            continue
        source_group = entry.get("counterflowForSourceGroup") or entry.get("sourceGroup")
        if wanted and source_group not in wanted:
            continue
        point_direction = str(entry.get("pointDirection") or "").strip().lower()
        if not point_direction or point_direction in seen:
            continue
        seen.add(point_direction)
        entries.append(
            {
                **entry,
                "pointDirection": point_direction,
                "sourceGroup": source_group,
                "flowConcept": entry.get("flowConcept") or "counterflow_export",
            }
        )
    return entries


def parse_entry_date(value: Any) -> Optional[dt.datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, dt.datetime):
        return day_key(value)
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time.min)
    try:
        return dt.datetime.combine(dt.date.fromisoformat(str(value)[:10]), dt.time.min)
    except ValueError:
        return None


def entry_valid_for_day(entry: Dict[str, Any], day: dt.datetime) -> bool:
    valid_from = parse_entry_date(entry.get("validFrom"))
    valid_to = parse_entry_date(entry.get("validTo"))
    if valid_from and day < valid_from:
        return False
    if valid_to and day > valid_to:
        return False
    return True


def date_query(from_date: Optional[dt.datetime], to_date: Optional[dt.datetime]) -> Dict[str, Any]:
    query: Dict[str, Any] = {}
    if from_date:
        query["$gte"] = from_date
    if to_date:
        query["$lte"] = to_date
    return query


def fetch_raw_daily(
    raw_collection_name: str,
    point_directions: Sequence[str],
    from_date: Optional[dt.datetime],
    to_date: Optional[dt.datetime],
) -> List[Dict[str, Any]]:
    db = get_database()
    query: Dict[str, Any] = {
        "pointDirection": {"$in": list(point_directions)},
        "indicator": "Physical Flow",
        "periodType": "day",
    }
    gas_day_query = date_query(from_date, to_date)
    if gas_day_query:
        query["gasDay"] = gas_day_query

    projection = {
        "_id": 0,
        "pointDirection": 1,
        "gasDay": 1,
        "value": 1,
        "unit": 1,
        "flowStatus": 1,
    }
    return list(db[raw_collection_name].find(query, projection).sort([("gasDay", 1), ("pointDirection", 1)]))


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
    flow_concept: str,
) -> Dict[str, Any]:
    return {
        "manifestVersion": manifest_version,
        "date": day,
        "granularity": "day",
        "aggregationLevel": aggregation_level,
        "seriesKey": series_key,
        "seriesLabel": series_label,
        "flowConcept": flow_concept,
        "valueKwh": 0.0,
        "valueGwh": 0.0,
        "valueTwh": 0.0,
        "valueMcm": 0.0,
        "defaultConverterKwhPerMcm": converter,
        "conversionFactorsBySource": {},
        "sourcePointDirections": set(),
        "counterflowPointDirections": set(),
        "sourceGroups": set(),
        "routeGroups": set(),
        "updatedAt": dt.datetime.now(dt.timezone.utc),
        **iso_calendar_fields(day),
    }


def entry_converter(entry: Dict[str, Any], default_converter: float) -> float:
    value = entry.get("converterKwhPerMcm")
    try:
        converter = float(value)
    except (TypeError, ValueError):
        converter = default_converter
    return converter if converter > 0 else default_converter


def add_value(
    target: Dict[str, Any],
    value_kwh: float,
    point_direction: str,
    entry: Dict[str, Any],
    default_converter: float,
) -> None:
    converter = entry_converter(entry, default_converter)
    source_group = entry.get("sourceGroup")
    target["valueKwh"] += value_kwh
    target["valueMcm"] += value_kwh / converter
    if target.get("flowConcept") == "counterflow_export":
        target["counterflowPointDirections"].add(point_direction)
    else:
        target["sourcePointDirections"].add(point_direction)
    target["sourceGroups"].add(source_group)
    if source_group:
        target["conversionFactorsBySource"][source_group] = converter
    if entry.get("routeGroup"):
        target["routeGroups"].add(entry.get("routeGroup"))


def materialize_rows(
    manifest_version: str,
    entries: List[Dict[str, Any]],
    counterflow_entries: List[Dict[str, Any]],
    raw_rows: Iterable[Dict[str, Any]],
    converter: float,
    include_route_groups: bool,
    include_point_directions: bool,
) -> List[Dict[str, Any]]:
    entry_by_pd = {entry["pointDirection"]: entry for entry in entries}
    counterflow_by_pd = {entry["pointDirection"]: entry for entry in counterflow_entries}
    aggregates: Dict[Tuple[dt.datetime, str, str, str], Dict[str, Any]] = {}

    for raw in raw_rows:
        point_direction = raw.get("pointDirection")
        entry = entry_by_pd.get(point_direction)
        is_counterflow = False
        if not entry:
            entry = counterflow_by_pd.get(point_direction)
            is_counterflow = entry is not None
        gas_day = raw.get("gasDay")
        if not entry or not isinstance(gas_day, dt.datetime):
            continue
        value = raw.get("value")
        if value in (None, ""):
            continue
        try:
            signed_value = float(value) * float(entry.get("sign", 1))
        except (TypeError, ValueError):
            continue

        day = day_key(gas_day)
        if not entry_valid_for_day(entry, day):
            continue
        source_group = entry.get("sourceGroup") or "Unknown"
        flow_concept = entry.get("flowConcept") or "gross_import"

        if is_counterflow:
            keys = [("source_group_counterflow", source_group, source_group, "counterflow_export")]
        else:
            keys = [
                ("source_group", source_group, source_group, flow_concept),
                ("total", "Total", "Total", flow_concept),
            ]
        if not is_counterflow and include_route_groups and entry.get("routeGroup") and entry.get("routeGroup") != "ignore":
            route_group = entry["routeGroup"]
            keys.append(("route_group", route_group, route_group, flow_concept))
        if include_point_directions:
            label = entry.get("pointLabelLegacy") or point_direction
            level = "point_direction_counterflow" if is_counterflow else "point_direction"
            keys.append((level, point_direction, label, flow_concept))

        for aggregation_level, series_key, series_label, key_flow_concept in keys:
            agg_key = (day, aggregation_level, series_key, key_flow_concept)
            target = aggregates.get(agg_key)
            if not target:
                target = init_aggregate(
                    manifest_version,
                    day,
                    aggregation_level,
                    series_key,
                    series_label,
                    converter,
                    key_flow_concept,
                )
                aggregates[agg_key] = target
            add_value(target, signed_value, point_direction, entry, converter)

    add_net_flow_rows(manifest_version, aggregates, converter)
    rows = []
    for row in aggregates.values():
        row["valueGwh"] = row["valueKwh"] / 1_000_000.0
        row["valueTwh"] = row["valueKwh"] / 1_000_000_000.0
        converters = sorted(set(row["conversionFactorsBySource"].values()))
        row["converterKwhPerMcm"] = converters[0] if len(converters) == 1 else None
        row["sourcePointDirections"] = sorted(v for v in row["sourcePointDirections"] if v)
        row["counterflowPointDirections"] = sorted(v for v in row["counterflowPointDirections"] if v)
        row["sourceGroups"] = sorted(v for v in row["sourceGroups"] if v)
        row["routeGroups"] = sorted(v for v in row["routeGroups"] if v)
        rows.append(row)

    return sorted(rows, key=lambda item: (item["date"], item["aggregationLevel"], item["seriesKey"], item["flowConcept"]))


def clone_net_row(
    manifest_version: str,
    day: dt.datetime,
    gross: Optional[Dict[str, Any]],
    counterflow: Optional[Dict[str, Any]],
    converter: float,
) -> Dict[str, Any]:
    row = init_aggregate(
        manifest_version,
        day,
        "source_group",
        "UK",
        "UK",
        converter,
        "net_import",
    )
    row["valueKwh"] = (gross or {}).get("valueKwh", 0.0) - (counterflow or {}).get("valueKwh", 0.0)
    row["valueMcm"] = (gross or {}).get("valueMcm", 0.0) - (counterflow or {}).get("valueMcm", 0.0)
    row["sourceGroups"].add("UK")
    for key in ("sourcePointDirections", "counterflowPointDirections"):
        for value in (gross or {}).get(key, set()):
            row[key].add(value)
        for value in (counterflow or {}).get(key, set()):
            row[key].add(value)
    row["conversionFactorsBySource"] = {
        **((gross or {}).get("conversionFactorsBySource") or {}),
        **((counterflow or {}).get("conversionFactorsBySource") or {}),
    }
    row["derivation"] = "gross_import_minus_counterflow_export"
    return row


def add_net_flow_rows(
    manifest_version: str,
    aggregates: Dict[Tuple[dt.datetime, str, str, str], Dict[str, Any]],
    converter: float,
) -> None:
    days = {
        key[0]
        for key in aggregates
        if key[1] in {"source_group", "source_group_counterflow"} and key[2] == "UK"
    }
    for day in days:
        gross_key = (day, "source_group", "UK", "gross_import")
        counter_key = (day, "source_group_counterflow", "UK", "counterflow_export")
        net_key = (day, "source_group", "UK", "net_import")
        aggregates[net_key] = clone_net_row(
            manifest_version,
            day,
            aggregates.get(gross_key),
            aggregates.get(counter_key),
            converter,
        )


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


def summarize(
    entries: List[Dict[str, Any]],
    counterflow_entries: List[Dict[str, Any]],
    raw_rows: List[Dict[str, Any]],
    materialized: List[Dict[str, Any]],
) -> None:
    by_source: DefaultDict[str, int] = defaultdict(int)
    for entry in entries:
        by_source[entry.get("sourceGroup") or "Unknown"] += 1

    date_values = [row["date"] for row in materialized]
    print(f"Manifest entries selected: {len(entries)}")
    print(f"Counterflow entries selected: {len(counterflow_entries)}")
    print(f"Raw daily observations found: {len(raw_rows)}")
    print(f"Curated daily rows built: {len(materialized)}")
    if date_values:
        print(f"Date coverage: {min(date_values).date()} to {max(date_values).date()}")

    print("\nManifest pointDirections by sourceGroup:")
    for key, count in sorted(by_source.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    latest_source_rows = [
        row
        for row in materialized
        if row["aggregationLevel"] == "source_group"
        and date_values
        and row["date"] == max(date_values)
    ]
    if latest_source_rows:
        print("\nLatest source_group values:")
        for row in sorted(latest_source_rows, key=lambda item: (item["seriesKey"], item["flowConcept"])):
            print(
                f"  {row['date'].date()} {row['seriesKey']} "
                f"[{row['flowConcept']}]: {row['valueMcm']:.1f} mcm"
            )


def main() -> None:
    args = parse_args()
    started_at = time.monotonic()
    from_date = parse_date(args.from_date)
    to_date = parse_date(args.to_date)

    manifest = load_manifest(args.manifest_collection, args.manifest_version)
    entries = selected_entries(manifest, args.source_groups)
    counterflow_entries = selected_counterflow_entries(manifest, args.source_groups)
    if not entries and not counterflow_entries:
        raise RuntimeError(f"No selected manifest entries for version {args.manifest_version}")

    point_directions = sorted({entry["pointDirection"] for entry in entries + counterflow_entries})
    raw_rows = fetch_raw_daily(
        args.raw_collection,
        point_directions,
        from_date,
        to_date,
    )
    materialized = materialize_rows(
        args.manifest_version,
        entries,
        counterflow_entries,
        raw_rows,
        args.converter_kwh_per_mcm,
        args.include_route_groups,
        args.include_point_directions,
    )
    summarize(entries, counterflow_entries, raw_rows, materialized)

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
