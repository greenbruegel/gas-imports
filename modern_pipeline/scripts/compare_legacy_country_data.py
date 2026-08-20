"""Compare curated gas_import_daily weekly values with legacy country workbook."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd

from modern_pipeline.db import get_database

DEFAULT_LEGACY_WORKBOOK = Path("legacy_country_data.xlsx")
DEFAULT_OUTPUT = Path("modern_pipeline/snapshots/legacy_country_data_comparison.csv")
DEFAULT_MANIFEST_VERSION = "legacy_excel_v3"
DEFAULT_LNG_MANIFEST_VERSION = "gie_alsi_lng_terminals_v2"
DEFAULT_SERIES = ("Russia", "Norway", "UK", "Azerbaijan", "Algeria", "LNG", "Total")


def figure1_flow_concept(series_key: str) -> str:
    return "net_import" if series_key == "UK" else "gross_import"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare weekly gas_import_daily source series with legacy_country_data.xlsx."
    )
    parser.add_argument(
        "--legacy-workbook",
        type=Path,
        default=DEFAULT_LEGACY_WORKBOOK,
        help=f"Path to legacy country workbook. Default: {DEFAULT_LEGACY_WORKBOOK}.",
    )
    parser.add_argument(
        "--manifest-version",
        default=DEFAULT_MANIFEST_VERSION,
        help=f"Pipeline manifest version in gas_import_daily. Default: {DEFAULT_MANIFEST_VERSION}.",
    )
    parser.add_argument(
        "--lng-manifest-version",
        default=None,
        help=(
            "Optional LNG manifest version in gas_import_daily. "
            f"Use {DEFAULT_LNG_MANIFEST_VERSION} to compare pipeline plus LNG."
        ),
    )
    parser.add_argument(
        "--collection",
        default="gas_import_daily",
        help="MongoDB curated collection. Default: gas_import_daily.",
    )
    parser.add_argument(
        "--sheet",
        default="EU27 gross imports ",
        help="Legacy workbook sheet to compare. Default: 'EU27 gross imports '.",
    )
    parser.add_argument(
        "--series",
        action="append",
        help=(
            "Series/source group to compare. Can be passed multiple times. "
            f"Defaults: {', '.join(DEFAULT_SERIES)}."
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        action="append",
        help="Calendar/ISO year to compare. Can be passed multiple times. Defaults to all overlapping years.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output comparison CSV. Default: {DEFAULT_OUTPUT}.",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=20,
        help="Number of largest absolute differences to print. Default: 20.",
    )
    return parser.parse_args()


def clean_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_legacy(path: Path, sheet: str, series: Sequence[str], years: Optional[Sequence[int]]) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet)
    if "week" not in df.columns:
        raise RuntimeError(f"Legacy sheet {sheet!r} does not have a week column")

    rows: List[Dict[str, Any]] = []
    wanted_years = set(years or [])
    for _, row in df.iterrows():
        week = clean_float(row.get("week"))
        if week is None:
            continue
        week_int = int(week)
        if week_int < 1:
            continue

        for series_key in series:
            prefix = "EU" if series_key == "Total" else series_key
            for col in df.columns:
                col_text = str(col)
                expected_prefix = f"{prefix}_"
                if not col_text.startswith(expected_prefix):
                    continue
                suffix = col_text.removeprefix(expected_prefix)
                if not suffix.isdigit():
                    continue
                year = int(suffix)
                if wanted_years and year not in wanted_years:
                    continue
                value = clean_float(row.get(col))
                if value is None:
                    continue
                rows.append(
                    {
                        "seriesKey": series_key,
                        "isoYear": year,
                        "isoWeek": week_int,
                        "legacyMcm": value,
                    }
                )
    return pd.DataFrame(rows)


def query_modern_rows(
    collection_name: str,
    manifest_version: str,
    series: Sequence[str],
    years: Optional[Sequence[int]],
) -> pd.DataFrame:
    if not series:
        return pd.DataFrame(columns=["seriesKey", "isoYear", "isoWeek", "modernMcm"])
    db = get_database()
    query: Dict[str, Any] = {
        "manifestVersion": manifest_version,
        "granularity": "day",
        "aggregationLevel": "source_group",
        "seriesKey": {"$in": list(series)},
    }
    if years:
        query["isoYear"] = {"$in": list(years)}

    projection = {
        "_id": 0,
        "seriesKey": 1,
        "isoYear": 1,
        "isoWeek": 1,
        "flowConcept": 1,
        "valueMcm": 1,
    }
    rows = list(db[collection_name].find(query, projection))
    if not rows:
        return pd.DataFrame(columns=["seriesKey", "isoYear", "isoWeek", "modernMcm"])
    df = pd.DataFrame(rows)
    if "flowConcept" not in df.columns:
        df["flowConcept"] = pd.NA
    has_flow_concept = df["flowConcept"].notna()
    if has_flow_concept.any():
        expected = df["seriesKey"].map(figure1_flow_concept)
        df = df[(~has_flow_concept) | (df["flowConcept"] == expected)].copy()
    df["valueMcm"] = pd.to_numeric(df["valueMcm"], errors="coerce").fillna(0)
    return (
        df.groupby(["seriesKey", "isoYear", "isoWeek"], as_index=False)["valueMcm"]
        .sum()
        .rename(columns={"valueMcm": "modernMcm"})
    )


def load_modern(
    collection_name: str,
    manifest_version: str,
    lng_manifest_version: Optional[str],
    series: Sequence[str],
    years: Optional[Sequence[int]],
) -> pd.DataFrame:
    component_series = [item for item in series if item != "Total"]
    pipeline_series = [item for item in component_series if item != "LNG"]
    frames = [query_modern_rows(collection_name, manifest_version, pipeline_series, years)]

    if "LNG" in component_series:
        if not lng_manifest_version:
            print(
                "Warning: LNG requested but --lng-manifest-version was not provided; "
                "LNG will be modern_only missing.",
            )
        else:
            frames.append(query_modern_rows(collection_name, lng_manifest_version, ["LNG"], years))

    modern = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)
    if modern.empty:
        raise RuntimeError(f"No gas_import_daily source_group rows found for manifestVersion={manifest_version}")

    if "Total" in series:
        total = (
            modern[modern["seriesKey"].isin(component_series)]
            .groupby(["isoYear", "isoWeek"], as_index=False)["modernMcm"]
            .sum()
        )
        total["seriesKey"] = "Total"
        modern = pd.concat([modern, total[["seriesKey", "isoYear", "isoWeek", "modernMcm"]]], ignore_index=True)

    return modern


def compare(legacy: pd.DataFrame, modern: pd.DataFrame) -> pd.DataFrame:
    merged = legacy.merge(modern, on=["seriesKey", "isoYear", "isoWeek"], how="outer")
    merged["legacyMcm"] = pd.to_numeric(merged["legacyMcm"], errors="coerce")
    merged["modernMcm"] = pd.to_numeric(merged["modernMcm"], errors="coerce")
    merged["diffMcm"] = merged["modernMcm"] - merged["legacyMcm"]
    merged["absDiffMcm"] = merged["diffMcm"].abs()
    merged["pctDiff"] = merged["diffMcm"] / merged["legacyMcm"].replace({0: pd.NA}) * 100
    merged["status"] = "matched"
    merged.loc[merged["legacyMcm"].isna(), "status"] = "modern_only"
    merged.loc[merged["modernMcm"].isna(), "status"] = "legacy_only"
    return merged.sort_values(["seriesKey", "isoYear", "isoWeek"]).reset_index(drop=True)


def summarize(comparison: pd.DataFrame, show: int) -> None:
    print(f"Comparison rows: {len(comparison)}")
    print("\nStatus counts:")
    for status, count in comparison["status"].value_counts().sort_index().items():
        print(f"  {status}: {count}")

    matched = comparison[comparison["status"] == "matched"].copy()
    if matched.empty:
        print("\nNo overlapping rows to summarize.")
        return

    print("\nMean absolute difference by series/year:")
    summary = (
        matched.groupby(["seriesKey", "isoYear"], as_index=False)
        .agg(
            weeks=("isoWeek", "count"),
            meanAbsDiffMcm=("absDiffMcm", "mean"),
            maxAbsDiffMcm=("absDiffMcm", "max"),
            meanPctDiff=("pctDiff", "mean"),
        )
        .sort_values(["seriesKey", "isoYear"])
    )
    for _, row in summary.iterrows():
        print(
            "  "
            f"{row['seriesKey']} {int(row['isoYear'])}: weeks={int(row['weeks'])}, "
            f"mean_abs={row['meanAbsDiffMcm']:.1f} mcm, "
            f"max_abs={row['maxAbsDiffMcm']:.1f} mcm, "
            f"mean_pct={row['meanPctDiff']:.2f}%"
        )

    print(f"\nLargest {show} absolute differences:")
    largest = matched.sort_values("absDiffMcm", ascending=False).head(show)
    for _, row in largest.iterrows():
        print(
            "  "
            f"{row['seriesKey']} {int(row['isoYear'])}-W{int(row['isoWeek']):02d}: "
            f"legacy={row['legacyMcm']:.1f}, modern={row['modernMcm']:.1f}, "
            f"diff={row['diffMcm']:.1f} mcm"
        )


def main() -> None:
    args = parse_args()
    series = list(args.series or DEFAULT_SERIES)
    legacy = load_legacy(args.legacy_workbook, args.sheet, series, args.year)
    modern = load_modern(args.collection, args.manifest_version, args.lng_manifest_version, series, args.year)
    comparison = compare(legacy, modern)
    args.csv.parent.mkdir(parents=True, exist_ok=True)
    comparison.to_csv(args.csv, index=False)
    summarize(comparison, args.show)
    print(f"\nCSV written: {args.csv}")


if __name__ == "__main__":
    main()
