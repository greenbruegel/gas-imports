"""Compare daily curated gas imports with legacy route/source daily CSV."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from modern_pipeline.db import get_database

DEFAULT_LEGACY_CSV = Path("legacy_route_data_daily.csv")
DEFAULT_OUTPUT = Path("modern_pipeline/snapshots/legacy_route_data_daily_comparison.csv")
DEFAULT_MANIFEST_VERSION = "legacy_excel_v2"
DEFAULT_SERIES = ("Russia", "Nord Stream", "Ukraine Gas Transit", "Yamal (BY,PL)", "Turkstream")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare daily gas_import_daily values with legacy_route_data_daily.csv."
    )
    parser.add_argument(
        "--legacy-csv",
        type=Path,
        default=DEFAULT_LEGACY_CSV,
        help=f"Path to legacy daily CSV. Default: {DEFAULT_LEGACY_CSV}.",
    )
    parser.add_argument(
        "--manifest-version",
        default=DEFAULT_MANIFEST_VERSION,
        help=f"Manifest version in gas_import_daily. Default: {DEFAULT_MANIFEST_VERSION}.",
    )
    parser.add_argument(
        "--collection",
        default="gas_import_daily",
        help="MongoDB curated collection. Default: gas_import_daily.",
    )
    parser.add_argument(
        "--series",
        action="append",
        help=(
            "Series to compare. Can be passed multiple times. "
            f"Defaults: {', '.join(DEFAULT_SERIES)}."
        ),
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        default="2022-01-01",
        help="Start date, YYYY-MM-DD. Default: 2022-01-01.",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        default="2024-12-31",
        help="End date, YYYY-MM-DD. Default: 2024-12-31.",
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
        default=30,
        help="Number of largest absolute differences to print. Default: 30.",
    )
    return parser.parse_args()


def parse_date(value: str) -> pd.Timestamp:
    return pd.Timestamp(value)


def load_legacy(path: Path, series: Sequence[str], start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "dates" not in df.columns:
        raise RuntimeError(f"{path} does not have a dates column")

    missing = [name for name in series if name not in df.columns]
    if missing:
        raise RuntimeError(f"{path} is missing requested series columns: {missing}")

    # Rows in the current-year tail can be like "24-Jun"; for this comparison we
    # only use explicitly year-stamped legacy rows.
    dated = df[df["dates"].astype(str).str.contains(r"\d{4}", regex=True, na=False)].copy()
    dated["date"] = pd.to_datetime(dated["dates"], dayfirst=True, errors="coerce")
    dated = dated.dropna(subset=["date"])
    dated = dated[(dated["date"] >= start) & (dated["date"] <= end)]

    long = dated.melt(
        id_vars=["date"],
        value_vars=list(series),
        var_name="seriesKey",
        value_name="legacyMcm",
    )
    long["legacyMcm"] = pd.to_numeric(long["legacyMcm"], errors="coerce")
    return long.dropna(subset=["legacyMcm"]).reset_index(drop=True)


def load_modern(
    collection_name: str,
    manifest_version: str,
    series: Sequence[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    db = get_database()
    query: Dict[str, Any] = {
        "manifestVersion": manifest_version,
        "granularity": "day",
        "seriesKey": {"$in": list(series)},
        "date": {
            "$gte": start.to_pydatetime(),
            "$lte": end.to_pydatetime(),
        },
    }
    projection = {
        "_id": 0,
        "date": 1,
        "seriesKey": 1,
        "valueMcm": 1,
        "sourcePointDirections": 1,
    }
    rows = list(db[collection_name].find(query, projection).sort([("date", 1), ("seriesKey", 1)]))
    if not rows:
        raise RuntimeError(
            f"No rows found in {collection_name} for manifestVersion={manifest_version}"
        )
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["modernMcm"] = pd.to_numeric(df["valueMcm"], errors="coerce")
    return df[["date", "seriesKey", "modernMcm", "sourcePointDirections"]]


def compare(legacy: pd.DataFrame, modern: pd.DataFrame) -> pd.DataFrame:
    merged = legacy.merge(modern, on=["date", "seriesKey"], how="outer")
    merged["diffMcm"] = merged["modernMcm"] - merged["legacyMcm"]
    merged["absDiffMcm"] = merged["diffMcm"].abs()
    merged["pctDiff"] = merged["diffMcm"] / merged["legacyMcm"].replace({0: pd.NA}) * 100
    merged["status"] = "matched"
    merged.loc[merged["legacyMcm"].isna(), "status"] = "modern_only"
    merged.loc[merged["modernMcm"].isna(), "status"] = "legacy_only"
    return merged.sort_values(["seriesKey", "date"]).reset_index(drop=True)


def summarize(comparison: pd.DataFrame, show: int) -> None:
    print(f"Comparison rows: {len(comparison)}")
    print("\nStatus counts:")
    for status, count in comparison["status"].value_counts().sort_index().items():
        print(f"  {status}: {count}")

    matched = comparison[comparison["status"] == "matched"].copy()
    if matched.empty:
        print("\nNo overlapping rows to summarize.")
        return

    print("\nDaily difference by series:")
    summary = (
        matched.groupby("seriesKey", as_index=False)
        .agg(
            days=("date", "count"),
            legacySumMcm=("legacyMcm", "sum"),
            modernSumMcm=("modernMcm", "sum"),
            meanAbsDiffMcm=("absDiffMcm", "mean"),
            maxAbsDiffMcm=("absDiffMcm", "max"),
            meanDiffMcm=("diffMcm", "mean"),
        )
        .sort_values("meanAbsDiffMcm", ascending=False)
    )
    for _, row in summary.iterrows():
        print(
            "  "
            f"{row['seriesKey']}: days={int(row['days'])}, "
            f"legacy_sum={row['legacySumMcm']:.1f}, modern_sum={row['modernSumMcm']:.1f}, "
            f"mean_diff={row['meanDiffMcm']:.2f}, "
            f"mean_abs={row['meanAbsDiffMcm']:.2f}, max_abs={row['maxAbsDiffMcm']:.2f}"
        )

    print(f"\nLargest {show} absolute differences:")
    largest = matched.sort_values("absDiffMcm", ascending=False).head(show)
    for _, row in largest.iterrows():
        print(
            "  "
            f"{row['date'].date()} {row['seriesKey']}: "
            f"legacy={row['legacyMcm']:.1f}, modern={row['modernMcm']:.1f}, "
            f"diff={row['diffMcm']:.1f}"
        )


def main() -> None:
    args = parse_args()
    series = list(args.series or DEFAULT_SERIES)
    start = parse_date(args.from_date)
    end = parse_date(args.to_date)

    legacy = load_legacy(args.legacy_csv, series, start, end)
    modern = load_modern(args.collection, args.manifest_version, series, start, end)
    comparison = compare(legacy, modern)

    args.csv.parent.mkdir(parents=True, exist_ok=True)
    comparison.to_csv(args.csv, index=False)
    summarize(comparison, args.show)
    print(f"\nCSV written: {args.csv}")


if __name__ == "__main__":
    main()
