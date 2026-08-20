"""Plot annual gas-import source comparisons against Eurostat."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

os.environ.setdefault("MPLCONFIGDIR", "modern_pipeline/snapshots/.matplotlib")

import matplotlib.pyplot as plt
import pandas as pd

from modern_pipeline.db import get_database

DEFAULT_EUROSTAT_JSON = Path("modern_pipeline/snapshots/eurostat_nrg_ti_gas_eu27_annual.json")
DEFAULT_OUTPUT_DIR = Path("modern_pipeline/snapshots")
DEFAULT_PIPELINE_MANIFEST_VERSION = "legacy_excel_v3"
DEFAULT_LNG_MANIFEST_VERSION = "gie_alsi_lng_terminals_v2"
SOURCE_ORDER = ["Russia", "Norway", "UK", "Azerbaijan", "Algeria", "LNG"]
SOURCE_COLORS = {
    "Russia": "#b3261e",
    "Norway": "#1261a6",
    "UK": "#6b7280",
    "Azerbaijan": "#0f8b8d",
    "Algeria": "#d18400",
    "LNG": "#7c3aed",
}


def eurostat_flow_concept(_series_key: str) -> str:
    return "gross_import"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare annual modern gas_import_daily source values with Eurostat nrg_ti_gas."
    )
    parser.add_argument(
        "--eurostat-json",
        type=Path,
        default=DEFAULT_EUROSTAT_JSON,
        help=f"Eurostat JSON snapshot path. Default: {DEFAULT_EUROSTAT_JSON}.",
    )
    parser.add_argument(
        "--manifest-version",
        default=DEFAULT_PIPELINE_MANIFEST_VERSION,
        help=f"Pipeline manifest version. Default: {DEFAULT_PIPELINE_MANIFEST_VERSION}.",
    )
    parser.add_argument(
        "--lng-manifest-version",
        default=DEFAULT_LNG_MANIFEST_VERSION,
        help=f"LNG manifest version. Default: {DEFAULT_LNG_MANIFEST_VERSION}.",
    )
    parser.add_argument(
        "--collection",
        default="gas_import_daily",
        help="MongoDB curated collection. Default: gas_import_daily.",
    )
    parser.add_argument(
        "--from-year",
        type=int,
        default=None,
        help="Optional first comparison year.",
    )
    parser.add_argument(
        "--to-year",
        type=int,
        default=None,
        help="Optional final comparison year.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for PNG/CSV outputs. Default: {DEFAULT_OUTPUT_DIR}.",
    )
    return parser.parse_args()


def load_eurostat(path: Path) -> pd.DataFrame:
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records") or []
    if not records:
        raise RuntimeError(f"No Eurostat records found in {path}")
    df = pd.DataFrame(records)
    df = df[df["seriesKey"].isin(SOURCE_ORDER)].copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["eurostatMcm"] = pd.to_numeric(df["valueMcm"], errors="coerce")
    return df[["year", "seriesKey", "eurostatMcm", "siec", "partner", "status"]].dropna(subset=["year"])


def year_window_filter(from_year: Optional[int], to_year: Optional[int]) -> Dict[str, Any]:
    query: Dict[str, Any] = {}
    if from_year is not None:
        query["$gte"] = dt.datetime(from_year, 1, 1)
    if to_year is not None:
        query["$lte"] = dt.datetime(to_year, 12, 31)
    return query


def query_modern_rows(
    collection_name: str,
    manifest_version: str,
    series_keys: List[str],
    from_year: Optional[int],
    to_year: Optional[int],
) -> pd.DataFrame:
    if not series_keys:
        return pd.DataFrame(columns=["date", "seriesKey", "valueMcm"])
    db = get_database()
    query: Dict[str, Any] = {
        "manifestVersion": manifest_version,
        "aggregationLevel": "source_group",
        "seriesKey": {"$in": series_keys},
    }
    date_filter = year_window_filter(from_year, to_year)
    if date_filter:
        query["date"] = date_filter
    rows = list(
        db[collection_name].find(
            query,
            {"_id": 0, "date": 1, "seriesKey": 1, "flowConcept": 1, "valueMcm": 1},
        )
    )
    if not rows:
        return pd.DataFrame(columns=["date", "seriesKey", "valueMcm"])
    df = pd.DataFrame(rows)
    if "flowConcept" not in df.columns:
        df["flowConcept"] = pd.NA
    has_flow_concept = df["flowConcept"].notna()
    if has_flow_concept.any():
        expected = df["seriesKey"].map(eurostat_flow_concept)
        df = df[(~has_flow_concept) | (df["flowConcept"] == expected)].copy()
    df["date"] = pd.to_datetime(df["date"])
    df["valueMcm"] = pd.to_numeric(df["valueMcm"], errors="coerce").fillna(0)
    return df


def load_modern(args: argparse.Namespace) -> pd.DataFrame:
    frames = [
        query_modern_rows(
            args.collection,
            args.manifest_version,
            [series for series in SOURCE_ORDER if series != "LNG"],
            args.from_year,
            args.to_year,
        ),
        query_modern_rows(
            args.collection,
            args.lng_manifest_version,
            ["LNG"],
            args.from_year,
            args.to_year,
        ),
    ]
    daily = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)
    if daily.empty:
        raise RuntimeError("No modern gas_import_daily rows found for requested manifests.")
    daily["year"] = daily["date"].dt.year
    return (
        daily.groupby(["year", "seriesKey"], as_index=False)["valueMcm"]
        .sum()
        .rename(columns={"valueMcm": "modernMcm"})
    )


def compare(modern: pd.DataFrame, eurostat: pd.DataFrame, from_year: Optional[int], to_year: Optional[int]) -> pd.DataFrame:
    merged = modern.merge(eurostat, on=["year", "seriesKey"], how="outer")
    if from_year is not None:
        merged = merged[merged["year"] >= from_year]
    if to_year is not None:
        merged = merged[merged["year"] <= to_year]
    merged["modernMcm"] = pd.to_numeric(merged["modernMcm"], errors="coerce")
    merged["eurostatMcm"] = pd.to_numeric(merged["eurostatMcm"], errors="coerce")
    merged["diffMcm"] = merged["modernMcm"] - merged["eurostatMcm"]
    merged["pctDiff"] = merged["diffMcm"] / merged["eurostatMcm"].replace({0: pd.NA}) * 100
    merged["absDiffMcm"] = merged["diffMcm"].abs()
    merged["status"] = "matched"
    merged.loc[merged["modernMcm"].isna(), "status"] = "eurostat_only"
    merged.loc[merged["eurostatMcm"].isna(), "status"] = "modern_only"
    return merged.sort_values(["seriesKey", "year"]).reset_index(drop=True)


def plot_comparison(comparison: pd.DataFrame, output_path: Path) -> None:
    columns = SOURCE_ORDER
    fig, axes = plt.subplots(len(columns), 1, figsize=(12, 11), sharex=True)
    for ax, series in zip(axes, columns):
        data = comparison[comparison["seriesKey"] == series].sort_values("year")
        years = data["year"].astype(int)
        color = SOURCE_COLORS[series]
        ax.plot(years, data["modernMcm"], marker="o", linewidth=2.0, color=color, label="Curated series")
        ax.plot(
            years,
            data["eurostatMcm"],
            marker="s",
            linewidth=1.7,
            color="#111827",
            linestyle="--",
            label="Eurostat",
        )
        ax.set_title(series, loc="left", fontsize=11)
        ax.grid(axis="y", color="#d1d5db", linewidth=0.8, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.tick_params(axis="x", labelrotation=0)
    axes[0].legend(ncol=2, frameon=False, loc="upper right")
    axes[-1].set_xlabel("")
    fig.suptitle("Annual EU27 Gas Imports: Curated Series vs Eurostat", y=0.995)
    fig.text(0.01, 0.5, "annual million cubic metres", va="center", rotation="vertical")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def plot_difference(comparison: pd.DataFrame, output_path: Path) -> None:
    columns = SOURCE_ORDER
    fig, axes = plt.subplots(len(columns), 1, figsize=(12, 10), sharex=True)
    for ax, series in zip(axes, columns):
        data = comparison[comparison["seriesKey"] == series].sort_values("year")
        colors = ["#b3261e" if value < 0 else "#1261a6" for value in data["diffMcm"].fillna(0)]
        ax.bar(data["year"].astype(int), data["diffMcm"], color=colors, alpha=0.8)
        ax.axhline(0, color="#111827", linewidth=0.8)
        ax.set_title(series, loc="left", fontsize=11)
        ax.grid(axis="y", color="#d1d5db", linewidth=0.8, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    fig.suptitle("Curated Series Minus Eurostat Annual Difference", y=0.995)
    fig.text(0.01, 0.5, "million cubic metres", va="center", rotation="vertical")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def summarize(comparison: pd.DataFrame) -> None:
    print(f"Comparison rows: {len(comparison)}")
    print("\nMean absolute difference by source:")
    for series in SOURCE_ORDER:
        data = comparison[(comparison["seriesKey"] == series) & (comparison["status"] == "matched")]
        if data.empty:
            print(f"  {series}: no matched rows")
            continue
        print(
            f"  {series}: years={len(data)}, "
            f"mean_abs={data['absDiffMcm'].mean():.1f} mcm, "
            f"max_abs={data['absDiffMcm'].max():.1f} mcm, "
            f"mean_pct={data['pctDiff'].mean():.2f}%"
        )


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / ".matplotlib").mkdir(parents=True, exist_ok=True)
    eurostat = load_eurostat(args.eurostat_json)
    modern = load_modern(args)
    comparison = compare(modern, eurostat, args.from_year, args.to_year)

    slug = f"{args.manifest_version}_plus_{args.lng_manifest_version}_vs_eurostat_annual"
    csv_path = args.output_dir / f"{slug}.csv"
    line_path = args.output_dir / f"{slug}_small_multiples.png"
    diff_path = args.output_dir / f"{slug}_differences.png"
    comparison.to_csv(csv_path, index=False)
    plot_comparison(comparison, line_path)
    plot_difference(comparison, diff_path)
    summarize(comparison)
    print(f"\nCSV: {csv_path}")
    print(f"Comparison plot: {line_path}")
    print(f"Difference plot: {diff_path}")


if __name__ == "__main__":
    main()
