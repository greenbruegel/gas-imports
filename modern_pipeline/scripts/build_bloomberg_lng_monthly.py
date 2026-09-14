"""Build monthly EU27 LNG origin flows from Bloomberg terminal Excel dumps."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from pymongo import UpdateOne

from modern_pipeline.db import get_database

DEFAULT_INPUT_DIR = Path("modern_pipeline/input/bloomberg_lng")
DEFAULT_OUTPUT_DIR = Path("modern_pipeline/output/bloomberg_lng")
DEFAULT_SNAPSHOT_DIR = Path("modern_pipeline/snapshots")
DEFAULT_LEGACY_PLOT_DIR = Path("modern_pipeline/input/bloomberg_lng_legacy")
DEFAULT_MCM_PER_TONNE = 0.00138
DEFAULT_SERIES_START = dt.date(2017, 1, 1)
DEFAULT_FRESH_FROM = dt.date(2022, 1, 1)
DEFAULT_COLLECTION = "bloomberg_lng_monthly"
SKIP_INPUT_DIR_NAMES = {"export", "exports"}

# Overlapping month/origin/destination cells keep the highest priority file.
# Discharge extracts (Origin_Berthcountry, typically negative tonnes) match the
# old Europe LNG files and beat load-port "flows_*" dumps. Filename prefixes
# still apply when mixed dumps sit in the same folder.
LAYOUT_PRIORITY = {
    "discharge_port": 100,
    "load_port": 10,
}
SOURCE_FILE_PRIORITY = {
    "imports": 100,
    "flows": 10,
}
DEFAULT_SOURCE_FILE_PRIORITY = 50

# True EU27 allowlist. The legacy notebook only dropped UK/Turkey/Russia because
# those Excel dumps were already Europe-only; these terminal extracts are global.
EU27_DESTINATIONS = {
    "Austria",
    "Belgium",
    "Bulgaria",
    "Croatia",
    "Cyprus",
    "Czech Republic",
    "Czechia",
    "Denmark",
    "Estonia",
    "Finland",
    "France",
    "Germany",
    "Greece",
    "Hungary",
    "Ireland",
    "Italy",
    "Latvia",
    "Lithuania",
    "Luxembourg",
    "Malta",
    "Netherlands",
    "Poland",
    "Portugal",
    "Romania",
    "Slovakia",
    "Slovenia",
    "Spain",
    "Sweden",
}

# Legacy notebook dropped these as "European re-exports". They do not appear as
# load-port origins in the current global extracts, but keep the filter.
EU_REEXPORT_ORIGINS = {
    "Belgium",
    "Finland",
    "France",
    "Germany",
    "Lithuania",
    "Netherlands",
    "Spain",
}

ORIGIN_DISPLAY_NAMES = {
    "United States of America": "United States",
}

NAMED_ORIGINS = [
    "Algeria",
    "Egypt",
    "Nigeria",
    "Norway",
    "Qatar",
    "Russia",
    "Trinidad & Tobago",
    "United States",
]

# Same regional buckets as the published LNG plot data workbook.
REGION_COLUMNS = ["America", "Africa", "Middle East", "Russia", "Other"]
REGION_BY_ORIGIN = {
    "United States": "America",
    "Trinidad & Tobago": "America",
    "Dominican Republic": "America",
    "Algeria": "Africa",
    "Angola": "Africa",
    "Nigeria": "Africa",
    "Egypt": "Africa",
    "Cameroon": "Africa",
    "Equatorial Guinea": "Africa",
    "Mozambique": "Africa",
    "Qatar": "Middle East",
    "Oman": "Middle East",
    "United Arab Emirates": "Middle East",
    "Russia": "Russia",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read Bloomberg LNG Excel dumps and write monthly EU27 origin flows. "
            "Does not modify the legacy LNG EU flows notebook."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Directory of Bloomberg .xlsx dumps. Default: {DEFAULT_INPUT_DIR}.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for published CSV/Excel outputs. Default: {DEFAULT_OUTPUT_DIR}.",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=DEFAULT_SNAPSHOT_DIR,
        help=f"Directory for debug pairs/JSON. Default: {DEFAULT_SNAPSHOT_DIR}.",
    )
    parser.add_argument(
        "--legacy-region-workbook",
        type=Path,
        default=None,
        help=(
            "Optional LNG plot data workbook used for 2019-2021 region history. "
            f"Default: latest 'LNG plot data *.xlsx' in {DEFAULT_LEGACY_PLOT_DIR}."
        ),
    )
    parser.add_argument(
        "--series-start",
        default=DEFAULT_SERIES_START.isoformat(),
        help=(
            "Drop Bloomberg months before this date. "
            f"Default: {DEFAULT_SERIES_START.isoformat()} (ignores the lone December 2016 extract)."
        ),
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help=(
            "Treat this calendar date as today when deciding the last complete month. "
            "Default: the machine's current date. The current calendar month is never written."
        ),
    )
    parser.add_argument(
        "--fresh-from",
        default=None,
        help=(
            "If set, splice LNG plot data for months before this date onto the "
            "Bloomberg dumps. Default: use Bloomberg dumps for every available month."
        ),
    )
    parser.add_argument(
        "--converter-mcm-per-tonne",
        type=float,
        default=DEFAULT_MCM_PER_TONNE,
        help=(
            "Metric tonnes to million cubic metres. "
            f"Default: {DEFAULT_MCM_PER_TONNE}, the absolute value of the legacy notebook converter."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and summarize the monthly series without writing files or MongoDB.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Upsert the spliced regional monthly series into MongoDB.",
    )
    parser.add_argument(
        "--collection",
        default=DEFAULT_COLLECTION,
        help=f"MongoDB collection in GAS_IMPORTS_DB. Default: {DEFAULT_COLLECTION}.",
    )
    return parser.parse_args()


def is_month_column(value: Any) -> bool:
    return isinstance(value, (dt.datetime, dt.date, pd.Timestamp))


def month_end(value: Any) -> dt.date:
    stamp = pd.Timestamp(value)
    return stamp.date()


def last_complete_month_end(as_of: dt.date) -> dt.date:
    """Last day of the most recent calendar month that has fully finished."""
    return as_of.replace(day=1) - dt.timedelta(days=1)


def list_bloomberg_input_files(input_dir: Path) -> List[Path]:
    files: List[Path] = []
    for path in sorted(input_dir.glob("*.xlsx")):
        if path.name.startswith("~$"):
            continue
        if path.parent.name.lower() in SKIP_INPUT_DIR_NAMES:
            continue
        files.append(path)
    return files


def source_file_priority(name: str, layout: str) -> int:
    layout_score = LAYOUT_PRIORITY.get(layout, DEFAULT_SOURCE_FILE_PRIORITY)
    stem = Path(name).name.lower()
    name_score = DEFAULT_SOURCE_FILE_PRIORITY
    for prefix, priority in SOURCE_FILE_PRIORITY.items():
        if stem.startswith(prefix):
            name_score = priority
            break
    return max(layout_score, name_score)


def detect_header_row(path: Path) -> int:
    raw = pd.read_excel(path, header=None)
    for index, row in raw.iterrows():
        values = [str(cell) for cell in row.tolist() if pd.notna(cell)]
        if "Port_Country" in values and any("Berthcountry" in value for value in values):
            return int(index)
    raise ValueError(f"Could not find Bloomberg header row in {path}")


def classify_id_columns(columns: Sequence[Any]) -> Tuple[str, str, str]:
    names = [str(column) for column in columns if not is_month_column(column)]
    if "Destination_Berthcountry" in names and "Port_Country" in names:
        return "Port_Country", "Destination_Berthcountry", "load_port"
    if "Origin_Berthcountry" in names and "Port_Country" in names:
        return "Origin_Berthcountry", "Port_Country", "discharge_port"
    raise ValueError(
        "Expected Port_Country plus Destination_Berthcountry or Origin_Berthcountry. "
        f"Found: {names}"
    )


def read_bloomberg_excel(path: Path) -> pd.DataFrame:
    header_row = detect_header_row(path)
    wide = pd.read_excel(path, header=header_row)
    origin_col, destination_col, layout = classify_id_columns(wide.columns)
    month_cols = [column for column in wide.columns if is_month_column(column)]
    if not month_cols:
        raise ValueError(f"No month columns found in {path}")

    id_cols = [column for column in wide.columns if not is_month_column(column)]
    if len(id_cols) > 2:
        wide = wide.groupby([origin_col, destination_col], dropna=False)[month_cols].sum(min_count=1)

    long = wide.melt(
        id_vars=[origin_col, destination_col],
        value_vars=month_cols,
        var_name="monthEnd",
        value_name="valueTonnes",
    )
    long = long.rename(columns={origin_col: "originRaw", destination_col: "destination"})
    long["valueTonnes"] = pd.to_numeric(long["valueTonnes"], errors="coerce")
    long = long.dropna(subset=["originRaw", "destination", "valueTonnes"])
    long = long.loc[long["valueTonnes"] != 0].copy()
    # Discharge extracts store tonnes as negatives; load-port dumps are positive.
    # Use absolute tonnes, then the positive mcm converter (the legacy notebook
    # multiplied by -0.00138).
    long["valueTonnes"] = long["valueTonnes"].abs()
    long["month"] = long["monthEnd"].map(month_end)
    long["sourceFile"] = path.name
    long["sourceLayout"] = layout
    long["sourcePriority"] = source_file_priority(path.name, layout)
    long["sourceMaxMonth"] = long["month"].max()
    return long[
        [
            "month",
            "originRaw",
            "destination",
            "valueTonnes",
            "sourceFile",
            "sourceLayout",
            "sourcePriority",
            "sourceMaxMonth",
        ]
    ]


def combine_files(frames: Sequence[pd.DataFrame]) -> Tuple[pd.DataFrame, int]:
    if not frames:
        raise RuntimeError("No Bloomberg LNG rows were read.")
    combined = pd.concat(frames, ignore_index=True)
    before = len(combined)
    if "sourceMaxMonth" not in combined.columns:
        combined["sourceMaxMonth"] = combined.groupby("sourceFile")["month"].transform("max")
    combined = combined.sort_values(
        [
            "month",
            "originRaw",
            "destination",
            "sourcePriority",
            "sourceMaxMonth",
            "sourceFile",
        ]
    )
    combined = combined.drop_duplicates(
        subset=["month", "originRaw", "destination"],
        keep="last",
    )
    return combined.reset_index(drop=True), before - len(combined)


def canonical_origin(name: str) -> str:
    return ORIGIN_DISPLAY_NAMES.get(name, name)


def origin_group(name: str) -> str:
    return name if name in NAMED_ORIGINS else "Other"


def origin_region(name: str) -> str:
    return REGION_BY_ORIGIN.get(name, "Other")


def build_tables(raw: pd.DataFrame, mcm_per_tonne: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    eu = raw.loc[raw["destination"].isin(EU27_DESTINATIONS)].copy()
    eu = eu.loc[~eu["originRaw"].isin(EU_REEXPORT_ORIGINS)].copy()
    eu["origin"] = eu["originRaw"].map(canonical_origin)
    eu["originGroup"] = eu["origin"].map(origin_group)
    eu["region"] = eu["origin"].map(origin_region)
    eu["valueMcm"] = eu["valueTonnes"] * mcm_per_tonne

    pairs = (
        eu.groupby(["month", "origin", "originGroup", "region", "destination"], as_index=False)
        .agg(valueTonnes=("valueTonnes", "sum"), valueMcm=("valueMcm", "sum"))
        .sort_values(["month", "origin", "destination"])
        .reset_index(drop=True)
    )
    by_origin = (
        eu.groupby(["month", "origin", "originGroup", "region"], as_index=False)
        .agg(valueTonnes=("valueTonnes", "sum"), valueMcm=("valueMcm", "sum"))
        .sort_values(["month", "origin"])
        .reset_index(drop=True)
    )
    return pairs, by_origin


def region_wide_from_origins(by_origin: pd.DataFrame) -> pd.DataFrame:
    wide = (
        by_origin.groupby(["month", "region"], as_index=False)["valueMcm"]
        .sum()
        .pivot(index="month", columns="region", values="valueMcm")
        .reindex(columns=REGION_COLUMNS)
        .fillna(0.0)
        .reset_index()
    )
    wide["source"] = "bloomberg_terminal"
    return wide


def find_legacy_region_workbook(explicit: Optional[Path], search_dir: Path) -> Optional[Path]:
    if explicit is not None:
        if not explicit.exists():
            raise FileNotFoundError(f"Legacy region workbook not found: {explicit}")
        return explicit
    matches = [
        path
        for path in search_dir.glob("LNG plot data*.xlsx")
        if not path.name.startswith("~$")
    ]
    if not matches:
        return None
    return sorted(matches, key=lambda path: path.stat().st_mtime)[-1]


def load_legacy_region_wide(path: Path, fresh_from: dt.date) -> pd.DataFrame:
    wide = pd.read_excel(path, index_col=0)
    wide.index = pd.to_datetime(wide.index)
    missing = [column for column in REGION_COLUMNS if column not in wide.columns]
    if missing:
        raise RuntimeError(f"{path} is missing region columns: {', '.join(missing)}")
    wide = wide[REGION_COLUMNS].copy()
    wide = wide.loc[wide.index < pd.Timestamp(fresh_from)].copy()
    if wide.empty:
        raise RuntimeError(f"{path} has no months before {fresh_from.isoformat()}")
    index_name = wide.index.name or "index"
    wide = wide.reset_index().rename(columns={index_name: "month"})
    wide["month"] = wide["month"].map(lambda value: pd.Timestamp(value).date())
    wide["source"] = "legacy_plot"
    return wide[["month", *REGION_COLUMNS, "source"]]


def splice_region_wide(legacy: pd.DataFrame, fresh: pd.DataFrame, fresh_from: dt.date) -> pd.DataFrame:
    fresh = fresh.loc[fresh["month"] >= fresh_from].copy()
    spliced = pd.concat([legacy, fresh], ignore_index=True)
    spliced = spliced.sort_values("month").drop_duplicates(subset=["month"], keep="last")
    return spliced.reset_index(drop=True)


def month_datetime(value: object) -> dt.datetime:
    if isinstance(value, dt.datetime):
        return dt.datetime(value.year, value.month, value.day)
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day)
    stamp = pd.Timestamp(value)
    return dt.datetime(stamp.year, stamp.month, stamp.day)


def region_mongo_documents(region_wide: pd.DataFrame) -> List[Dict[str, Any]]:
    updated_at = dt.datetime.now(dt.timezone.utc)
    docs: List[Dict[str, Any]] = []
    for record in region_wide.to_dict(orient="records"):
        month = month_datetime(record["month"])
        values = {region: float(record[region]) for region in REGION_COLUMNS}
        docs.append(
            {
                "month": month,
                "year": month.year,
                "monthNumber": month.month,
                "reporter": "EU27",
                "unit": "mcm",
                "granularity": "month",
                "values": values,
                "totalMcm": float(sum(values.values())),
                "source": str(record["source"]),
                "updatedAt": updated_at,
            }
        )
    return docs


def ensure_region_indexes(collection_name: str) -> None:
    collection = get_database()[collection_name]
    collection.create_index("month", unique=True)
    collection.create_index([("year", 1), ("monthNumber", 1)])
    collection.create_index("source")


def upsert_region_months(collection_name: str, docs: List[Dict[str, Any]]) -> Dict[str, int]:
    if not docs:
        return {"matched": 0, "modified": 0, "upserted": 0, "deleted": 0}

    ensure_region_indexes(collection_name)
    collection = get_database()[collection_name]
    months = [doc["month"] for doc in docs]
    deleted = collection.delete_many({"month": {"$nin": months}}).deleted_count
    operations = [
        UpdateOne({"month": doc["month"]}, {"$set": doc}, upsert=True)
        for doc in docs
    ]
    result = collection.bulk_write(operations, ordered=False)
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "upserted": len(result.upserted_ids),
        "deleted": deleted,
    }


def summarize(
    pairs: pd.DataFrame,
    by_origin: pd.DataFrame,
    region_wide: pd.DataFrame,
    combined: pd.DataFrame,
    duplicate_rows: int,
    legacy_workbook: Optional[Path],
) -> None:
    months = sorted(by_origin["month"].unique())
    print(f"EU27 pair rows: {len(pairs)}")
    print(f"EU27 origin-month rows: {len(by_origin)}")
    print(f"Duplicate month/origin/destination cells overridden: {duplicate_rows}")
    print("Rows kept after dedupe, by source file:")
    for name, count in combined.groupby("sourceFile").size().sort_values(ascending=False).items():
        layout = combined.loc[combined["sourceFile"] == name, "sourceLayout"].iloc[0]
        priority = int(combined.loc[combined["sourceFile"] == name, "sourcePriority"].iloc[0])
        print(f"  {name}: {count} ({layout}, priority={priority})")
    if months:
        print(f"Fresh origin months: {months[0].isoformat()} to {months[-1].isoformat()}")
    print(f"Origins: {', '.join(sorted(by_origin['origin'].unique()))}")
    if legacy_workbook is not None:
        print(f"Legacy region history: {legacy_workbook}")
    if not region_wide.empty:
        print(
            "Region months: "
            f"{region_wide['month'].min().isoformat()} to {region_wide['month'].max().isoformat()} "
            f"({len(region_wide)} months)"
        )
        print("Region-month sources:")
        for source, count in region_wide["source"].value_counts().sort_index().items():
            print(f"  {source}: {count}")
    latest = months[-1] if months else None
    if latest is None:
        return
    print(f"\nLatest month totals ({latest.isoformat()}, mcm):")
    latest_rows = by_origin.loc[by_origin["month"] == latest].sort_values("valueMcm", ascending=False)
    for row in latest_rows.itertuples(index=False):
        print(f"  {row.origin}: {row.valueMcm:.1f}")
    print(f"  TOTAL: {latest_rows['valueMcm'].sum():.1f}")


def main() -> None:
    args = parse_args()
    fresh_from = dt.date.fromisoformat(args.fresh_from) if args.fresh_from else None
    files = list_bloomberg_input_files(args.input_dir)
    if not files:
        raise FileNotFoundError(f"No .xlsx files found in {args.input_dir}")

    series_start = dt.date.fromisoformat(args.series_start)
    as_of = dt.date.fromisoformat(args.as_of) if args.as_of else dt.date.today()
    series_end = last_complete_month_end(as_of)
    frames = [read_bloomberg_excel(path) for path in files]
    combined, duplicate_rows = combine_files(frames)
    combined = combined.loc[
        (combined["month"] >= series_start) & (combined["month"] <= series_end)
    ].copy()
    if combined.empty:
        raise RuntimeError(
            f"No Bloomberg LNG rows between {series_start.isoformat()} and {series_end.isoformat()}"
        )
    pairs, by_origin = build_tables(combined, args.converter_mcm_per_tonne)
    fresh_regions = region_wide_from_origins(by_origin)

    legacy_workbook: Optional[Path] = None
    if fresh_from is None:
        region_wide = fresh_regions
    else:
        legacy_workbook = find_legacy_region_workbook(args.legacy_region_workbook, DEFAULT_LEGACY_PLOT_DIR)
        if legacy_workbook is None:
            raise FileNotFoundError(
                "No LNG plot data workbook found for pre-Bloomberg history. "
                f"Place one in {DEFAULT_LEGACY_PLOT_DIR} or pass --legacy-region-workbook."
            )
        legacy_regions = load_legacy_region_wide(legacy_workbook, fresh_from)
        region_wide = splice_region_wide(legacy_regions, fresh_regions, fresh_from)

    metadata = {
        "source": "Bloomberg LNG terminal Excel dumps",
        "inputDir": str(args.input_dir),
        "inputFiles": [path.name for path in files],
        "sourceFilePriority": SOURCE_FILE_PRIORITY,
        "layoutPriority": LAYOUT_PRIORITY,
        "legacyRegionWorkbook": str(legacy_workbook) if legacy_workbook else None,
        "freshFrom": fresh_from.isoformat() if fresh_from else None,
        "seriesStart": series_start.isoformat(),
        "asOf": as_of.isoformat(),
        "seriesEnd": series_end.isoformat(),
        "converterMcmPerTonne": args.converter_mcm_per_tonne,
        "eu27Destinations": sorted(EU27_DESTINATIONS),
        "droppedReexportOrigins": sorted(EU_REEXPORT_ORIGINS),
        "namedOrigins": NAMED_ORIGINS,
        "regionColumns": REGION_COLUMNS,
        "regionByOrigin": REGION_BY_ORIGIN,
        "notes": [
            "Long country and wide region files use the discharge Bloomberg dumps (year-split xlsx).",
            "Negative discharge tonnes are taken as absolute values, then converted at 0.00138 mcm/tonne.",
            "Load-port dumps in export/ or exports/ are ignored. Discharge layout beats load-port on overlaps.",
            "Later-ending files win when the same layout overlaps (e.g. 2024-26 over 2023-24).",
            "Unlisted origins, including Norway, are mapped to Other to match the published regional chart.",
            "Pass --fresh-from to splice LNG plot data before that date; default is Bloomberg-only.",
            f"Months before {DEFAULT_SERIES_START.isoformat()} are dropped (the 2015-16 extract is December 2016 only).",
            "The current calendar month and any later months are dropped until that month has fully completed.",
        ],
        "builtAt": dt.datetime.now(dt.timezone.utc).isoformat(),
        "originMonths": {
            "from": by_origin["month"].min().isoformat() if len(by_origin) else None,
            "to": by_origin["month"].max().isoformat() if len(by_origin) else None,
        },
        "regionMonths": {
            "from": region_wide["month"].min().isoformat() if len(region_wide) else None,
            "to": region_wide["month"].max().isoformat() if len(region_wide) else None,
        },
        "duplicateCellsOverridden": duplicate_rows,
    }

    summarize(pairs, by_origin, region_wide, combined, duplicate_rows, legacy_workbook)
    print(f"As of {as_of.isoformat()}: last complete month is {series_end.isoformat()}")
    if args.dry_run:
        print("\nDry run: no files written.")
        return

    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.snapshot_dir.mkdir(parents=True, exist_ok=True)

    origin_csv = args.output_dir / "eu27_monthly_by_origin.csv"
    region_csv = args.output_dir / "eu27_monthly_by_region.csv"
    region_xlsx = args.output_dir / "eu27_monthly_by_region.xlsx"
    pairs_csv = args.snapshot_dir / "bloomberg_lng_eu27_monthly_pairs.csv"
    metadata_json = args.snapshot_dir / "bloomberg_lng_build.json"

    long_out = by_origin[["month", "origin", "originGroup", "region", "valueTonnes", "valueMcm"]]
    long_out.to_csv(origin_csv, index=False)
    region_wide.to_csv(region_csv, index=False)

    excel_wide = region_wide.set_index("month")[REGION_COLUMNS].copy()
    excel_wide.index = pd.to_datetime(excel_wide.index)
    excel_wide.to_excel(region_xlsx, index=True)

    pairs.to_csv(pairs_csv, index=False)
    metadata_json.write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"\nLong country CSV: {origin_csv}")
    print(f"Wide region CSV: {region_csv}")
    print(f"Wide region Excel: {region_xlsx}")
    print(f"Debug pairs CSV: {pairs_csv}")
    print(f"Debug JSON: {metadata_json}")

    if args.write:
        mongo_docs = region_mongo_documents(region_wide)
        result = upsert_region_months(args.collection, mongo_docs)
        print(
            f"\nMongoDB {args.collection}: "
            f"upserted={result['upserted']}, matched={result['matched']}, "
            f"modified={result['modified']}, deleted={result['deleted']}"
        )


if __name__ == "__main__":
    main()
