"""Build a versioned import manifest from the legacy locations Excel file."""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd
from pymongo import UpdateOne

from modern_pipeline.db import get_database

DEFAULT_LEGACY_LOCATIONS = Path("working data/locationsSAFE.xlsx")
DEFAULT_VERSION = "legacy_excel_v1"
DEFAULT_SOURCE_GROUPS = ("Russia", "Norway", "UK", "Azerbaijan", "Algeria")
DEFAULT_CONVERTER_KWH_PER_MCM = 10_300_000.0
LEGACY_V4_NORWAY_CONVERTER_KWH_PER_MCM = 11_000_000.0
SOURCE_GCV_KWH_PER_M3 = {
    # ENTSOG Gas Quality Outlook 2024 input-data charts, kWh/m3 (25/0 C).
    # These are source-average GCV values read from the import-source chart
    # except UK, which is read from the European production chart.
    "Russia": 11.40,
    "Norway": 11.58,
    "UK": 11.55,
    "Azerbaijan": 11.72,
    "Algeria": 11.65,
    "LNG": 11.58,
}
SOURCE_GCV_REFERENCE = {
    "name": "ENTSOG Gas Quality Outlook 2024",
    "url": "https://www.entsog.eu/sites/default/files/2026-03/entsog_GQO_2024_260327.pdf",
    "unit": "kWh/m3 (25/0 C)",
    "notes": (
        "Values are approximate source-average GCV points read from the report's "
        "input-data charts and converted to kWh/MCM by multiplying by 1,000,000."
    ),
}
LEGACY_V4_CONVERSION_REFERENCE = {
    "name": "legacy_10.3_with_gassco_norway_11.0",
    "unit": "kWh/m3",
    "notes": (
        "Compatibility volume convention: non-Norway sources keep the legacy "
        "10.3 kWh/m3 conversion; Norway uses 11.0 kWh/m3, approximately implied "
        "by Gassco official BCM/TWh delivery statistics."
    ),
}
LEGACY_V2_EXCLUDED_POINT_DIRECTIONS = {
    # Azerbaijan: the legacy Figure 1 workbook uses Kipoi, not both TAP landings.
    "it-tso-0001itp-00008entry": "legacy_excel_v2 excludes Melendugno to avoid double-counting TAP with Kipoi",
    # Norway: duplicate or legacy landing-point variants relative to the Figure 1 workbook.
    "be-tso-0001itp-00519entry": "legacy_excel_v2 excludes Dunkerque DKB duplicate/legacy variant",
    "de-tso-0005itp-00081entry": "legacy_excel_v2 excludes Emden GUD duplicate/legacy variant",
    "de-tso-0009itp-00126entry": "legacy_excel_v2 excludes Dornum OGE duplicate/legacy variant",
    "de-tso-0009itp-00525entry": "legacy_excel_v2 excludes Dornum GASPOOL duplicate/legacy variant",
    "de-tso-0013itp-00211entry": "legacy_excel_v2 excludes Dornum jordgas legacy variant",
    "dk-tso-0001itp-00097entry": "legacy_excel_v2 excludes Nybro, replaced by newer Norwegian routing",
    "nl-tso-0001itp-00161entry": "legacy_excel_v2 excludes Emden NPT legacy variant",
    # Russia: current API exposes duplicate/legacy variants that are not in the Figure 1 workbook totals.
    "de-tso-0017itp-00247entry": "legacy_excel_v2 excludes one Greifswald duplicate/legacy variant",
    "de-tso-0018itp-00297entry": "legacy_excel_v2 excludes one Greifswald duplicate/legacy variant",
    "lt-tso-0001itp-00085entry": "legacy_excel_v2 excludes Kotlovka duplicate/legacy variant",
    "lt-tso-0001itp-00050exit": "legacy_excel_v2 excludes Sakiai because it is Lithuania exit to Russia/Kaliningrad",
    "ro-tso-0001itp-00154exit": "legacy_excel_v2 excludes Ungheni reverse/legacy variant",
}
LEGACY_V3_EXCLUDED_POINT_DIRECTIONS = {
    key: value
    for key, value in LEGACY_V2_EXCLUDED_POINT_DIRECTIONS.items()
    if key != "de-tso-0005itp-00081entry"
}
LEGACY_V3_TIME_BOUND_POINT_DIRECTIONS = {
    "de-tso-0005itp-00081entry": {
        "validTo": "2020-12-31",
        "notes": (
            "legacy_excel_v3 includes Emden EPT1 GUD only through 2020 as the "
            "archive-era counterpart for missing pre-2021 Emden sub-series."
        ),
    },
    "de-tso-0002itp-00105entry": {
        "validFrom": "2021-01-01",
        "notes": "legacy_excel_v3 uses Emden EPT1 Thyssengas from 2021 onward.",
    },
}
LEGACY_V3_UK_NET_FLOW_RULE = {
    "seriesKey": "UK",
    "grossFlowConcept": "gross_import",
    "counterflowConcept": "counterflow_export",
    "netFlowConcept": "net_import",
    "basis": (
        "Legacy Figure 1 UK panel used net physical flows: UK-to-EU gross "
        "imports minus EU-to-UK counterflows from Bacton BBL and Zeebrugge IZT."
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert locationsSAFE.xlsx into a versioned ENTSOG import manifest."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and summarize the manifest without writing. This is the default.",
    )
    mode.add_argument(
        "--write",
        action="store_true",
        help="Upsert the manifest into MongoDB.",
    )
    parser.add_argument(
        "--legacy-locations",
        type=Path,
        default=DEFAULT_LEGACY_LOCATIONS,
        help=f"Path to locationsSAFE.xlsx. Default: {DEFAULT_LEGACY_LOCATIONS}",
    )
    parser.add_argument(
        "--version",
        default=DEFAULT_VERSION,
        help=(
            f"Manifest version id. Default: {DEFAULT_VERSION}. "
            "Use legacy_excel_v2 for Figure 1 workbook-compatible point selection."
        ),
    )
    parser.add_argument(
        "--collection",
        default="entsog_import_manifest",
        help="MongoDB manifest collection. Default: entsog_import_manifest.",
    )
    parser.add_argument(
        "--source-group",
        action="append",
        dest="source_groups",
        help=(
            "Legacy aggregation bucket to include. Can be passed multiple times. "
            f"Defaults: {', '.join(DEFAULT_SOURCE_GROUPS)}."
        ),
    )
    parser.add_argument(
        "--include-libya",
        action="store_true",
        help="Also include legacy Libya pipeline rows.",
    )
    parser.add_argument(
        "--include-all-pipeline",
        action="store_true",
        help="Include every non-LNG ITP row from locationsSAFE.xlsx.",
    )
    parser.add_argument(
        "--include-uk-sheet",
        action="store_true",
        help=(
            "Also read the workbook's UK sheet. Off by default because locationsSAFE "
            "already contains the UK-to-EU rows needed for Figure 1."
        ),
    )
    parser.add_argument(
        "--include-imports-to-uk",
        action="store_true",
        help="Keep rows where the legacy import country is United Kingdom. Off by default for EU Figure 1.",
    )
    return parser.parse_args()


def clean_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def clean_optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def point_direction_id(operator_key: str, point_key: str, direction_key: str) -> str:
    return f"{operator_key}{point_key}{direction_key}".strip().lower()


def read_legacy_rows(path: Path, include_uk_sheet: bool) -> pd.DataFrame:
    sheets: List[Any] = ["locationsSAFE"]
    if include_uk_sheet:
        sheets.append("UK")

    frames = []
    for sheet in sheets:
        try:
            df = pd.read_excel(path, sheet_name=sheet).fillna("")
        except ValueError:
            if sheet == "locationsSAFE":
                df = pd.read_excel(path, sheet_name=0).fillna("")
            else:
                continue
        df["legacySheet"] = str(sheet)
        frames.append(df)

    if not frames:
        raise RuntimeError(f"No readable legacy sheets found in {path}")

    df = pd.concat(frames, ignore_index=True)
    required = {"direction", "operator", "location", "aggregation"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"{path} is missing required columns: {sorted(missing)}")
    return df


def read_legacy_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        return pd.read_excel(path, sheet_name=sheet_name).fillna("")
    except ValueError:
        return pd.DataFrame()


def source_groups_from_args(args: argparse.Namespace) -> List[str]:
    groups = list(args.source_groups or DEFAULT_SOURCE_GROUPS)
    if args.include_libya and "Libya" not in groups:
        groups.append("Libya")
    return groups


def should_include_row(
    row: pd.Series,
    source_groups: Sequence[str],
    include_all_pipeline: bool,
    include_imports_to_uk: bool,
) -> bool:
    location = clean_text(row.get("location"))
    aggregation = clean_text(row.get("aggregation"))
    import_country = clean_text(row.get("importcountry"))
    if not location.startswith("ITP-"):
        return False
    if aggregation == "LNG":
        return False
    if import_country == "United Kingdom" and not include_imports_to_uk:
        return False
    if include_all_pipeline:
        return True
    return aggregation in set(source_groups)


def make_entry(row: pd.Series, ordinal: int) -> Dict[str, Any]:
    operator_key = clean_text(row.get("operator")).upper()
    point_key = clean_text(row.get("location")).upper()
    direction_key = clean_text(row.get("direction")).lower()
    source_group = clean_text(row.get("aggregation"))
    route_group = clean_text(row.get("aggregation2"))
    route_subgroup = clean_text(row.get("aggregation3"))

    return {
        "entryId": f"legacy_excel_v1:{ordinal:04d}",
        "pointDirection": point_direction_id(operator_key, point_key, direction_key),
        "operatorKey": operator_key,
        "pointKey": point_key,
        "directionKey": direction_key,
        "selected": True,
        "sign": 1,
        "sourceGroup": source_group,
        "figure1Group": source_group,
        "routeGroup": route_group,
        "routeSubgroup": route_subgroup,
        "exportCountryLegacy": clean_text(row.get("exportcountry")),
        "importCountryLegacy": clean_text(row.get("importcountry")),
        "importCountryIso2Legacy": clean_text(row.get("import2digit")).upper(),
        "pointLabelLegacy": clean_text(row.get("label")),
        "ieaNameLegacy": clean_text(row.get("IEAname")),
        "gcvLegacy": clean_optional_float(row.get("GCV")),
        "legacySheet": clean_text(row.get("legacySheet")),
        "legacyRowNumber": int(ordinal),
        "selectionSource": "locationsSAFE.xlsx",
        "notes": "",
    }


def uses_v3_point_selection(version: str) -> bool:
    return version in {"legacy_excel_v3", "legacy_excel_v4"}


def apply_version_compatibility(entry: Dict[str, Any], version: str) -> Dict[str, Any]:
    entry = dict(entry)
    point_direction = entry["pointDirection"]
    if version == "legacy_excel_v2" and point_direction in LEGACY_V2_EXCLUDED_POINT_DIRECTIONS:
        entry["selected"] = False
        entry["compatibilityExclusionReason"] = LEGACY_V2_EXCLUDED_POINT_DIRECTIONS[point_direction]
        entry["notes"] = LEGACY_V2_EXCLUDED_POINT_DIRECTIONS[point_direction]
    if uses_v3_point_selection(version):
        if point_direction in LEGACY_V3_EXCLUDED_POINT_DIRECTIONS:
            entry["selected"] = False
            entry["compatibilityExclusionReason"] = LEGACY_V3_EXCLUDED_POINT_DIRECTIONS[point_direction]
            entry["notes"] = LEGACY_V3_EXCLUDED_POINT_DIRECTIONS[point_direction]
        time_rule = LEGACY_V3_TIME_BOUND_POINT_DIRECTIONS.get(point_direction)
        if time_rule:
            entry.update({key: value for key, value in time_rule.items() if key != "notes"})
            entry["notes"] = time_rule["notes"]
    if version == "legacy_excel_v3":
        entry = apply_source_gcv_conversion(entry)
    if version == "legacy_excel_v4":
        entry = apply_legacy_v4_conversion(entry)
    return entry


def apply_default_conversion(entry: Dict[str, Any]) -> Dict[str, Any]:
    entry = dict(entry)
    entry["converterKwhPerMcm"] = DEFAULT_CONVERTER_KWH_PER_MCM
    entry["conversionSource"] = "legacy_default_10.3_kwh_per_m3"
    return entry


def apply_source_gcv_conversion(entry: Dict[str, Any]) -> Dict[str, Any]:
    entry = dict(entry)
    source_group = entry.get("sourceGroup")
    gcv = SOURCE_GCV_KWH_PER_M3.get(source_group)
    if gcv is None:
        return apply_default_conversion(entry)
    entry["sourceGcvKwhPerM3"] = gcv
    entry["converterKwhPerMcm"] = gcv * 1_000_000.0
    entry["conversionSource"] = SOURCE_GCV_REFERENCE["name"]
    entry["conversionReferenceUrl"] = SOURCE_GCV_REFERENCE["url"]
    return entry


def apply_legacy_v4_conversion(entry: Dict[str, Any]) -> Dict[str, Any]:
    entry = apply_default_conversion(entry)
    if entry.get("sourceGroup") == "Norway":
        entry["sourceGcvKwhPerM3"] = LEGACY_V4_NORWAY_CONVERTER_KWH_PER_MCM / 1_000_000.0
        entry["converterKwhPerMcm"] = LEGACY_V4_NORWAY_CONVERTER_KWH_PER_MCM
        entry["conversionSource"] = LEGACY_V4_CONVERSION_REFERENCE["name"]
        entry["conversionReferenceNotes"] = LEGACY_V4_CONVERSION_REFERENCE["notes"]
    return entry


def apply_version_conversion(entry: Dict[str, Any], version: str) -> Dict[str, Any]:
    if version == "legacy_excel_v3":
        return apply_source_gcv_conversion(entry)
    if version == "legacy_excel_v4":
        return apply_legacy_v4_conversion(entry)
    return entry


def build_counterflow_entries(path: Path, version: str) -> List[Dict[str, Any]]:
    df = read_legacy_sheet(path, "UK")
    if df.empty:
        return []
    entries = []
    seen = set()
    for idx, row in df.iterrows():
        if clean_text(row.get("aggregation")) != "EU":
            continue
        location = clean_text(row.get("location")).upper()
        if not location.startswith("ITP-"):
            continue
        entry = make_entry(row, idx + 2)
        entry.update(
            {
                "entryId": f"{version}:counterflow:{idx + 2:04d}",
                "sourceGroup": "UK",
                "figure1Group": "UK",
                "flowConcept": "counterflow_export",
                "counterflowForSourceGroup": "UK",
                "selectionSource": "locationsSAFE.xlsx:UK",
                "notes": "EU-to-UK counterflow used to derive UK net imports.",
            }
        )
        entry = apply_version_conversion(entry, version)
        point_direction = entry["pointDirection"]
        if point_direction in seen:
            continue
        seen.add(point_direction)
        entries.append(entry)
    return sorted(entries, key=lambda item: (item["pointKey"], item["pointDirection"]))


def build_manifest(args: argparse.Namespace) -> Dict[str, Any]:
    source_groups = source_groups_from_args(args)
    df = read_legacy_rows(args.legacy_locations, args.include_uk_sheet)

    entries = []
    seen = set()
    for idx, row in df.iterrows():
        if not should_include_row(
            row,
            source_groups,
            args.include_all_pipeline,
            args.include_imports_to_uk,
        ):
            continue
        entry = make_entry(row, idx + 2)
        entry = apply_version_compatibility(entry, args.version)
        point_direction = entry["pointDirection"]
        if point_direction in seen:
            continue
        seen.add(point_direction)
        entries.append(entry)

    counterflow_entries = build_counterflow_entries(args.legacy_locations, args.version) if uses_v3_point_selection(args.version) else []
    now = dt.datetime.now(dt.timezone.utc)
    return {
        "version": args.version,
        "kind": "entsog_import_manifest",
        "status": "draft",
        "description": (
            "Legacy-compatible manifest converted from locationsSAFE.xlsx for "
            "Figure 1 pipeline import reproduction, excluding LNG."
        ),
        "source": {
            "type": "legacy_excel",
            "path": str(args.legacy_locations),
            "sheets": ["locationsSAFE"] + (["UK"] if args.include_uk_sheet else []),
        },
        "scope": {
            "includedSourceGroups": sorted({entry["sourceGroup"] for entry in entries}),
            "excluded": ["LNG"] if not args.include_all_pipeline else ["LNG"],
            "figure": "figure1",
            "notes": "Total is intended to be the sum of included selected entries.",
        },
        "compatibilityRules": (
            {
                "basis": "legacy_country_data.xlsx Figure 1 comparison plus notebook cleanup behavior",
                "excludedPointDirections": LEGACY_V2_EXCLUDED_POINT_DIRECTIONS,
            }
            if args.version == "legacy_excel_v2"
            else {
                "basis": (
                    "legacy_excel_v2 point selection with source-GCV conversion and "
                    "time-bounded Norway archive alias handling"
                ),
                "excludedPointDirections": LEGACY_V3_EXCLUDED_POINT_DIRECTIONS,
                "timeBoundPointDirections": LEGACY_V3_TIME_BOUND_POINT_DIRECTIONS,
                "netFlowRules": {"UK": LEGACY_V3_UK_NET_FLOW_RULE},
                "sourceGcvKwhPerM3": SOURCE_GCV_KWH_PER_M3,
                "conversionReference": SOURCE_GCV_REFERENCE,
            }
            if args.version == "legacy_excel_v3"
            else {
                "basis": (
                    "legacy_excel_v3 point selection, UK net-flow handling, and "
                    "compatibility volume conversion: Norway at 11.0 kWh/m3, "
                    "all other pipeline sources at legacy 10.3 kWh/m3."
                ),
                "excludedPointDirections": LEGACY_V3_EXCLUDED_POINT_DIRECTIONS,
                "timeBoundPointDirections": LEGACY_V3_TIME_BOUND_POINT_DIRECTIONS,
                "netFlowRules": {"UK": LEGACY_V3_UK_NET_FLOW_RULE},
                "defaultConverterKwhPerMcm": DEFAULT_CONVERTER_KWH_PER_MCM,
                "sourceOverrideConverterKwhPerMcm": {
                    "Norway": LEGACY_V4_NORWAY_CONVERTER_KWH_PER_MCM,
                },
                "conversionReference": LEGACY_V4_CONVERSION_REFERENCE,
            }
            if args.version == "legacy_excel_v4"
            else {}
        ),
        "entries": sorted(entries, key=lambda item: (item["sourceGroup"], item["pointKey"], item["pointDirection"])),
        "counterflowEntries": counterflow_entries,
        "entryCount": len(entries),
        "selectedEntryCount": sum(1 for entry in entries if entry.get("selected", True)),
        "counterflowEntryCount": len(counterflow_entries),
        "createdAt": now,
        "updatedAt": now,
    }


def summarize_entries(entries: Iterable[Dict[str, Any]]) -> None:
    rows = list(entries)
    selected_rows = [row for row in rows if row.get("selected", True)]
    excluded_rows = [row for row in rows if not row.get("selected", True)]
    by_source: Dict[str, int] = {}
    by_direction: Dict[str, int] = {}
    for row in selected_rows:
        by_source[row["sourceGroup"]] = by_source.get(row["sourceGroup"], 0) + 1
        by_direction[row["directionKey"]] = by_direction.get(row["directionKey"], 0) + 1

    print(f"Manifest entries: {len(rows)}")
    print(f"Selected entries: {len(selected_rows)}")
    print(f"Compatibility-excluded entries: {len(excluded_rows)}")
    print("\nBy sourceGroup:")
    for key, count in sorted(by_source.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nBy directionKey:")
    for key, count in sorted(by_direction.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nSample:")
    for row in selected_rows[:20]:
        print(
            "  "
            f"{row['pointDirection']} | {row['sourceGroup']} | "
            f"{row['pointLabelLegacy']} | {row['exportCountryLegacy']} -> {row['importCountryLegacy']}"
        )
    if excluded_rows:
        print("\nExcluded sample:")
        for row in excluded_rows[:20]:
            print(
                "  "
                f"{row['pointDirection']} | {row['sourceGroup']} | "
                f"{row['pointLabelLegacy']} | {row.get('compatibilityExclusionReason', '')}"
            )


def summarize_counterflow_entries(entries: Iterable[Dict[str, Any]]) -> None:
    rows = list(entries)
    if not rows:
        return
    print(f"\nCounterflow entries: {len(rows)}")
    for row in rows:
        print(
            "  "
            f"{row['pointDirection']} | {row.get('counterflowForSourceGroup')} | "
            f"{row['pointLabelLegacy']} | {row['exportCountryLegacy']} -> {row['importCountryLegacy']}"
        )


def ensure_indexes(collection_name: str) -> None:
    db = get_database()
    collection = db[collection_name]
    collection.create_index("version", unique=True)
    collection.create_index([("status", 1), ("version", 1)])


def write_manifest(collection_name: str, manifest: Dict[str, Any]) -> Dict[str, int]:
    ensure_indexes(collection_name)
    db = get_database()
    collection = db[collection_name]
    manifest_update = dict(manifest)
    created_at = manifest_update.pop("createdAt")
    result = collection.bulk_write(
        [
            UpdateOne(
                {"version": manifest["version"]},
                {
                    "$set": manifest_update,
                    "$setOnInsert": {"createdAt": created_at},
                },
                upsert=True,
            )
        ]
    )
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "upserted": len(result.upserted_ids),
    }


def main() -> None:
    args = parse_args()
    manifest = build_manifest(args)
    summarize_entries(manifest["entries"])
    summarize_counterflow_entries(manifest.get("counterflowEntries") or [])

    if not args.write:
        print("\nDry run only. No MongoDB writes performed.")
        return

    result = write_manifest(args.collection, manifest)
    print(
        "\nMongo upsert complete "
        f"(matched={result['matched']}, modified={result['modified']}, upserted={result['upserted']})."
    )


if __name__ == "__main__":
    main()
