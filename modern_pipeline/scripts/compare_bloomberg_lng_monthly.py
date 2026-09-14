"""Compare modern Bloomberg LNG monthly origin flows with the legacy Excel vintage."""

from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
from typing import List

os.environ.setdefault("MPLCONFIGDIR", "modern_pipeline/snapshots/.matplotlib")

import matplotlib.pyplot as plt
import pandas as pd

from modern_pipeline.scripts.build_bloomberg_lng_monthly import (
    DEFAULT_MCM_PER_TONNE,
    NAMED_ORIGINS,
    build_tables,
    combine_files,
    read_bloomberg_excel,
)

DEFAULT_MODERN_CSV = Path("modern_pipeline/output/bloomberg_lng/eu27_monthly_by_origin.csv")
DEFAULT_LEGACY_DIR = Path("modern_pipeline/input/bloomberg_lng_legacy")
DEFAULT_OUTPUT_DIR = Path("modern_pipeline/snapshots")
SERIES_ORDER = [*NAMED_ORIGINS, "Other", "Total"]
SERIES_COLORS = {
    "Algeria": "#d18400",
    "Egypt": "#0f8b8d",
    "Nigeria": "#2BB0CB",
    "Norway": "#111827",
    "Qatar": "#ca8a04",
    "Russia": "#b3261e",
    "Trinidad & Tobago": "#7c3aed",
    "United States": "#1261a6",
    "Other": "#6b7280",
    "Total": "#1f2937",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare monthly EU27 Bloomberg LNG origin flows with the legacy Excel vintage. "
            "Legacy dumps can be the old Europe LNG workbooks or granular LNG imports.xlsx."
        )
    )
    parser.add_argument(
        "--modern-csv",
        type=Path,
        default=DEFAULT_MODERN_CSV,
        help=f"Modern origin-month CSV. Default: {DEFAULT_MODERN_CSV}.",
    )
    parser.add_argument(
        "--legacy-dir",
        type=Path,
        default=DEFAULT_LEGACY_DIR,
        help=f"Directory of legacy Bloomberg .xlsx dumps. Default: {DEFAULT_LEGACY_DIR}.",
    )
    parser.add_argument(
        "--legacy-workbook",
        type=Path,
        default=None,
        help="Optional pre-aggregated granular LNG imports.xlsx instead of raw dumps.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for CSV/PNG outputs. Default: {DEFAULT_OUTPUT_DIR}.",
    )
    parser.add_argument(
        "--converter-mcm-per-tonne",
        type=float,
        default=DEFAULT_MCM_PER_TONNE,
        help=f"Tonnes-to-mcm factor for raw legacy dumps. Default: {DEFAULT_MCM_PER_TONNE}.",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=20,
        help="Number of largest absolute monthly differences to print. Default: 20.",
    )
    return parser.parse_args()


def excel_files(directory: Path) -> List[Path]:
    files = []
    for path in sorted(directory.glob("*.xlsx")):
        name = path.name
        if name.startswith("~$"):
            continue
        if "plot data" in name.lower() or " BEN." in name:
            continue
        files.append(path)
    return files


def parse_month(value: object) -> dt.date:
    return pd.Timestamp(value).date()


def add_total(frame: pd.DataFrame, value_col: str) -> pd.DataFrame:
    totals = frame.groupby("month", as_index=False)[value_col].sum()
    totals["originGroup"] = "Total"
    return pd.concat([frame, totals], ignore_index=True)


def load_modern(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Modern Bloomberg monthly CSV not found: {path}")
    df = pd.read_csv(path)
    if "originGroup" not in df.columns or "valueMcm" not in df.columns:
        raise RuntimeError(f"{path} is missing originGroup/valueMcm columns")
    df["month"] = df["month"].map(parse_month)
    grouped = (
        df.groupby(["month", "originGroup"], as_index=False)["valueMcm"]
        .sum()
        .rename(columns={"valueMcm": "modernMcm"})
    )
    return add_total(grouped, "modernMcm")


def load_legacy_workbook(path: Path) -> pd.DataFrame:
    wide = pd.read_excel(path, index_col=0)
    wide.index = pd.to_datetime(wide.index)
    long = (
        wide.rename(columns={"United States of America": "United States"})
        .apply(pd.to_numeric, errors="coerce")
        .melt(ignore_index=False, var_name="originGroup", value_name="legacyMcm")
        .dropna(subset=["legacyMcm"])
        .reset_index()
    )
    month_col = long.columns[0]
    long = long.rename(columns={month_col: "month"})
    long["month"] = long["month"].map(parse_month)
    long["originGroup"] = long["originGroup"].replace({"USA": "United States"})
    grouped = long.groupby(["month", "originGroup"], as_index=False)["legacyMcm"].sum()
    return add_total(grouped, "legacyMcm")


def load_legacy_dumps(directory: Path, converter: float) -> pd.DataFrame:
    files = excel_files(directory)
    if not files:
        raise FileNotFoundError(f"No legacy Bloomberg .xlsx files found in {directory}")
    frames = [read_bloomberg_excel(path) for path in files]
    combined, _duplicate_rows = combine_files(frames)
    _pairs, by_origin = build_tables(combined, converter)
    grouped = (
        by_origin.groupby(["month", "originGroup"], as_index=False)["valueMcm"]
        .sum()
        .rename(columns={"valueMcm": "legacyMcm"})
    )
    grouped["month"] = grouped["month"].map(parse_month)
    return add_total(grouped, "legacyMcm")


def overlap_grid(legacy: pd.DataFrame, modern: pd.DataFrame) -> pd.DataFrame:
    months = sorted(set(legacy["month"]) & set(modern["month"]))
    series = [name for name in SERIES_ORDER if name in set(legacy["originGroup"]) | set(modern["originGroup"])]
    if not months or not series:
        return pd.DataFrame(columns=["month", "originGroup", "legacyMcm", "modernMcm"])
    grid = pd.MultiIndex.from_product([months, series], names=["month", "originGroup"]).to_frame(index=False)
    merged = grid.merge(legacy, on=["month", "originGroup"], how="left").merge(
        modern, on=["month", "originGroup"], how="left"
    )
    merged["legacyMcm"] = pd.to_numeric(merged["legacyMcm"], errors="coerce").fillna(0.0)
    merged["modernMcm"] = pd.to_numeric(merged["modernMcm"], errors="coerce").fillna(0.0)
    merged["diffMcm"] = merged["modernMcm"] - merged["legacyMcm"]
    merged["absDiffMcm"] = merged["diffMcm"].abs()
    merged["pctDiff"] = merged["diffMcm"] / merged["legacyMcm"].replace({0: pd.NA}) * 100
    merged["status"] = "matched"
    merged.loc[(merged["legacyMcm"] == 0) & (merged["modernMcm"] != 0), "status"] = "modern_only"
    merged.loc[(merged["modernMcm"] == 0) & (merged["legacyMcm"] != 0), "status"] = "legacy_only"
    return merged.sort_values(["originGroup", "month"]).reset_index(drop=True)


def annual_summary(comparison: pd.DataFrame) -> pd.DataFrame:
    frame = comparison[comparison["originGroup"] != "Total"].copy()
    frame["year"] = pd.to_datetime(frame["month"]).dt.year
    annual = frame.groupby(["year", "originGroup"], as_index=False).agg(
        legacyMcm=("legacyMcm", "sum"),
        modernMcm=("modernMcm", "sum"),
        absDiffMcm=("absDiffMcm", "sum"),
    )
    annual["diffMcm"] = annual["modernMcm"] - annual["legacyMcm"]
    annual["pctDiff"] = annual["diffMcm"] / annual["legacyMcm"].replace({0: pd.NA}) * 100
    totals = annual.groupby("year", as_index=False).agg(
        legacyMcm=("legacyMcm", "sum"),
        modernMcm=("modernMcm", "sum"),
        absDiffMcm=("absDiffMcm", "sum"),
        diffMcm=("diffMcm", "sum"),
    )
    totals["originGroup"] = "Total"
    totals["pctDiff"] = totals["diffMcm"] / totals["legacyMcm"].replace({0: pd.NA}) * 100
    return pd.concat([annual, totals], ignore_index=True).sort_values(["year", "originGroup"]).reset_index(drop=True)


def plot_lines(comparison: pd.DataFrame, output_path: Path) -> None:
    series = [name for name in SERIES_ORDER if name in set(comparison["originGroup"])]
    fig, axes = plt.subplots(len(series), 1, figsize=(12, 2.1 * len(series)), sharex=True)
    if len(series) == 1:
        axes = [axes]
    for ax, name in zip(axes, series):
        data = comparison[comparison["originGroup"] == name].sort_values("month")
        months = pd.to_datetime(data["month"])
        color = SERIES_COLORS.get(name, "#1261a6")
        ax.plot(months, data["legacyMcm"], color="#111827", linestyle="--", linewidth=1.6, label="Legacy Excel")
        ax.plot(months, data["modernMcm"], color=color, linewidth=2.0, label="New Bloomberg dumps")
        ax.set_title(name, loc="left", fontsize=11)
        ax.grid(axis="y", color="#d1d5db", linewidth=0.8, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    axes[0].legend(ncol=2, frameon=False, loc="upper right")
    fig.suptitle("Monthly EU27 LNG by origin: new Bloomberg dumps vs legacy Excel", y=0.995)
    fig.text(0.01, 0.5, "million cubic metres", va="center", rotation="vertical")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def plot_differences(comparison: pd.DataFrame, output_path: Path) -> None:
    series = [name for name in SERIES_ORDER if name in set(comparison["originGroup"])]
    fig, axes = plt.subplots(len(series), 1, figsize=(12, 2.0 * len(series)), sharex=True)
    if len(series) == 1:
        axes = [axes]
    for ax, name in zip(axes, series):
        data = comparison[comparison["originGroup"] == name].sort_values("month")
        months = pd.to_datetime(data["month"])
        colors = ["#b3261e" if value < 0 else "#1261a6" for value in data["diffMcm"]]
        ax.bar(months, data["diffMcm"], color=colors, width=20, alpha=0.85)
        ax.axhline(0, color="#111827", linewidth=0.8)
        ax.set_title(name, loc="left", fontsize=11)
        ax.grid(axis="y", color="#d1d5db", linewidth=0.8, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    fig.suptitle("New Bloomberg dumps minus legacy Excel", y=0.995)
    fig.text(0.01, 0.5, "million cubic metres", va="center", rotation="vertical")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def summarize(comparison: pd.DataFrame, annual: pd.DataFrame, show: int) -> None:
    months = sorted(comparison["month"].unique())
    print(f"Comparison rows: {len(comparison)}")
    if months:
        print(f"Overlap: {months[0].isoformat()} to {months[-1].isoformat()} ({len(months)} months)")

    print("\nAnnual totals (mcm):")
    totals = annual[annual["originGroup"] == "Total"]
    for row in totals.itertuples(index=False):
        print(
            f"  {int(row.year)}: legacy={row.legacyMcm:.0f}, modern={row.modernMcm:.0f}, "
            f"diff={row.diffMcm:+.0f} ({row.pctDiff:+.1f}%)"
        )

    print("\nMean absolute monthly difference by supplier:")
    matched = comparison[comparison["originGroup"] != "Total"]
    summary = (
        matched.groupby("originGroup", as_index=False)
        .agg(
            months=("month", "count"),
            meanAbsDiffMcm=("absDiffMcm", "mean"),
            maxAbsDiffMcm=("absDiffMcm", "max"),
            sumDiffMcm=("diffMcm", "sum"),
        )
        .sort_values("meanAbsDiffMcm", ascending=False)
    )
    for row in summary.itertuples(index=False):
        print(
            f"  {row.originGroup}: mean_abs={row.meanAbsDiffMcm:.1f} mcm, "
            f"max_abs={row.maxAbsDiffMcm:.1f} mcm, net={row.sumDiffMcm:+.0f} mcm"
        )

    print(f"\nLargest {show} monthly supplier differences:")
    largest = matched.sort_values("absDiffMcm", ascending=False).head(show)
    for row in largest.itertuples(index=False):
        pct = "" if pd.isna(row.pctDiff) else f" ({row.pctDiff:+.1f}%)"
        print(
            f"  {row.month.isoformat()} {row.originGroup}: "
            f"legacy={row.legacyMcm:.1f}, modern={row.modernMcm:.1f}, "
            f"diff={row.diffMcm:+.1f}{pct}"
        )


def main() -> None:
    args = parse_args()
    modern = load_modern(args.modern_csv)
    if args.legacy_workbook:
        legacy = load_legacy_workbook(args.legacy_workbook)
        legacy_label = str(args.legacy_workbook)
    else:
        legacy = load_legacy_dumps(args.legacy_dir, args.converter_mcm_per_tonne)
        legacy_label = str(args.legacy_dir)

    comparison = overlap_grid(legacy, modern)
    if comparison.empty:
        raise RuntimeError(f"No overlapping months between {legacy_label} and {args.modern_csv}")
    annual = annual_summary(comparison)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    monthly_path = args.output_dir / "bloomberg_lng_monthly_comparison.csv"
    annual_path = args.output_dir / "bloomberg_lng_monthly_comparison_annual.csv"
    lines_path = args.output_dir / "bloomberg_lng_monthly_comparison_lines.png"
    diffs_path = args.output_dir / "bloomberg_lng_monthly_comparison_differences.png"

    comparison.to_csv(monthly_path, index=False)
    annual.to_csv(annual_path, index=False)
    plot_lines(comparison, lines_path)
    plot_differences(comparison, diffs_path)
    summarize(comparison, annual, args.show)

    print(f"\nMonthly CSV: {monthly_path}")
    print(f"Annual CSV: {annual_path}")
    print(f"Lines plot: {lines_path}")
    print(f"Difference plot: {diffs_path}")


if __name__ == "__main__":
    main()
