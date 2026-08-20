"""Compare annual curated Norway imports with official Norwegian delivery data."""

from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
from typing import Any, Dict, Optional

os.environ.setdefault("MPLCONFIGDIR", "modern_pipeline/snapshots/.matplotlib")

import matplotlib.pyplot as plt
import pandas as pd

from modern_pipeline.db import get_database

DEFAULT_OFFICIAL_CSV = Path("modern_pipeline/snapshots/norway_official_delivery_point_comparison.csv")
DEFAULT_OUTPUT_DIR = Path("modern_pipeline/snapshots")
DEFAULT_MANIFEST_VERSION = "legacy_excel_v3"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare annual gas_import_daily Norway values with official Norwegian delivery totals."
    )
    parser.add_argument(
        "--official-csv",
        type=Path,
        default=DEFAULT_OFFICIAL_CSV,
        help=f"Official Norway delivery-point comparison CSV. Default: {DEFAULT_OFFICIAL_CSV}.",
    )
    parser.add_argument(
        "--manifest-version",
        default=DEFAULT_MANIFEST_VERSION,
        help=f"Pipeline manifest version. Default: {DEFAULT_MANIFEST_VERSION}.",
    )
    parser.add_argument(
        "--collection",
        default="gas_import_daily",
        help="MongoDB curated collection. Default: gas_import_daily.",
    )
    parser.add_argument("--from-year", type=int, default=2016, help="First comparison year. Default: 2016.")
    parser.add_argument("--to-year", type=int, default=2025, help="Final comparison year. Default: 2025.")
    parser.add_argument(
        "--official-column",
        choices=["Official_EU27_pipeline_no_lng", "Official_EU27_pipeline_no_lng_no_dk"],
        default="Official_EU27_pipeline_no_lng",
        help="Official benchmark column to use. Default: Official_EU27_pipeline_no_lng.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for PNG/CSV outputs. Default: {DEFAULT_OUTPUT_DIR}.",
    )
    return parser.parse_args()


def year_window_filter(from_year: Optional[int], to_year: Optional[int]) -> Dict[str, Any]:
    query: Dict[str, Any] = {}
    if from_year is not None:
        query["$gte"] = dt.datetime(from_year, 1, 1)
    if to_year is not None:
        query["$lte"] = dt.datetime(to_year, 12, 31)
    return query


def load_curated(
    collection_name: str,
    manifest_version: str,
    from_year: Optional[int],
    to_year: Optional[int],
) -> pd.DataFrame:
    db = get_database()
    query: Dict[str, Any] = {
        "manifestVersion": manifest_version,
        "aggregationLevel": "source_group",
        "seriesKey": "Norway",
        "flowConcept": "gross_import",
    }
    date_query = year_window_filter(from_year, to_year)
    if date_query:
        query["date"] = date_query

    rows = list(
        db[collection_name].find(
            query,
            {"_id": 0, "date": 1, "valueMcm": 1, "valueKwh": 1},
        )
    )
    if not rows:
        raise RuntimeError(
            f"No Norway gross_import rows found in {collection_name} for manifestVersion={manifest_version}"
        )
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["valueMcm"] = pd.to_numeric(df["valueMcm"], errors="coerce").fillna(0)
    df["valueKwh"] = pd.to_numeric(df["valueKwh"], errors="coerce").fillna(0)
    annual = (
        df.groupby("year", as_index=False)
        .agg(
            curatedMcm=("valueMcm", "sum"),
            curatedKwh=("valueKwh", "sum"),
            days=("date", "nunique"),
        )
        .sort_values("year")
    )
    annual["curatedBcm"] = annual["curatedMcm"] / 1_000.0
    annual["curatedTwh"] = annual["curatedKwh"] / 1_000_000_000.0
    return annual


def load_official(path: Path, official_column: str, from_year: Optional[int], to_year: Optional[int]) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"year", official_column}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"{path} is missing required columns: {sorted(missing)}")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["officialBcm"] = pd.to_numeric(df[official_column], errors="coerce")
    if from_year is not None:
        df = df[df["year"] >= from_year]
    if to_year is not None:
        df = df[df["year"] <= to_year]
    return df.dropna(subset=["year"])[["year", "officialBcm"]].copy()


def compare(curated: pd.DataFrame, official: pd.DataFrame) -> pd.DataFrame:
    merged = curated.merge(official, on="year", how="outer")
    merged["curatedBcm"] = pd.to_numeric(merged["curatedBcm"], errors="coerce")
    merged["officialBcm"] = pd.to_numeric(merged["officialBcm"], errors="coerce")
    merged["diffBcm"] = merged["curatedBcm"] - merged["officialBcm"]
    merged["absDiffBcm"] = merged["diffBcm"].abs()
    merged["pctDiff"] = merged["diffBcm"] / merged["officialBcm"].replace({0: pd.NA}) * 100
    merged["status"] = "matched"
    merged.loc[merged["curatedBcm"].isna(), "status"] = "official_only"
    merged.loc[merged["officialBcm"].isna(), "status"] = "curated_only"
    return merged.sort_values("year").reset_index(drop=True)


def plot_comparison(comparison: pd.DataFrame, output_path: Path, manifest_version: str) -> None:
    data = comparison.sort_values("year")
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.plot(data["year"], data["curatedBcm"], marker="o", linewidth=2.2, color="#1261a6", label="Curated Norway")
    ax.plot(
        data["year"],
        data["officialBcm"],
        marker="s",
        linewidth=1.8,
        linestyle="--",
        color="#111827",
        label="Official Norwegian delivery data",
    )
    ax.set_title(f"Norway Pipeline Imports: Curated Series vs Official Source ({manifest_version})")
    ax.set_ylabel("annual bcm")
    ax.set_xlabel("")
    ax.grid(axis="y", color="#d1d5db", linewidth=0.8, alpha=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def plot_difference(comparison: pd.DataFrame, output_path: Path, manifest_version: str) -> None:
    data = comparison.sort_values("year")
    colors = ["#b3261e" if value < 0 else "#1261a6" for value in data["diffBcm"].fillna(0)]
    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.bar(data["year"], data["diffBcm"], color=colors, alpha=0.85)
    ax.axhline(0, color="#111827", linewidth=0.9)
    ax.set_title(f"Curated Norway Minus Official Source ({manifest_version})")
    ax.set_ylabel("bcm")
    ax.set_xlabel("")
    ax.grid(axis="y", color="#d1d5db", linewidth=0.8, alpha=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def summarize(comparison: pd.DataFrame) -> None:
    matched = comparison[comparison["status"] == "matched"]
    print(f"Comparison rows: {len(comparison)}")
    print("\nAnnual comparison:")
    for _, row in comparison.iterrows():
        print(
            "  "
            f"{int(row['year'])}: curated={row['curatedBcm']:.2f} bcm, "
            f"official={row['officialBcm']:.2f} bcm, diff={row['diffBcm']:.2f} bcm"
        )
    if not matched.empty:
        print(
            "\nMatched summary: "
            f"years={len(matched)}, mean_abs={matched['absDiffBcm'].mean():.2f} bcm, "
            f"max_abs={matched['absDiffBcm'].max():.2f} bcm, "
            f"mean_pct={matched['pctDiff'].mean():.2f}%"
        )


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / ".matplotlib").mkdir(parents=True, exist_ok=True)

    curated = load_curated(args.collection, args.manifest_version, args.from_year, args.to_year)
    official = load_official(args.official_csv, args.official_column, args.from_year, args.to_year)
    comparison = compare(curated, official)

    slug = f"{args.manifest_version}_norway_vs_official"
    csv_path = args.output_dir / f"{slug}.csv"
    line_path = args.output_dir / f"{slug}.png"
    diff_path = args.output_dir / f"{slug}_differences.png"
    comparison.to_csv(csv_path, index=False)
    plot_comparison(comparison, line_path, args.manifest_version)
    plot_difference(comparison, diff_path, args.manifest_version)
    summarize(comparison)
    print(f"\nCSV: {csv_path}")
    print(f"Comparison plot: {line_path}")
    print(f"Difference plot: {diff_path}")


if __name__ == "__main__":
    main()
