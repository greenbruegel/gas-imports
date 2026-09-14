"""Compare the published five-region LNG plot with the modern pipeline series."""

from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "modern_pipeline/snapshots/.matplotlib")

import matplotlib.pyplot as plt
import pandas as pd

from modern_pipeline.scripts.build_bloomberg_lng_monthly import (
    DEFAULT_FRESH_FROM,
    DEFAULT_LEGACY_PLOT_DIR,
    REGION_COLUMNS,
    find_legacy_region_workbook,
)

DEFAULT_MODERN_CSV = Path("modern_pipeline/output/bloomberg_lng/eu27_monthly_by_region.csv")
DEFAULT_OUTPUT_DIR = Path("modern_pipeline/snapshots")
SERIES_ORDER = [*REGION_COLUMNS, "Total"]
SERIES_COLORS = {
    "America": "#1261a6",
    "Africa": "#d18400",
    "Middle East": "#ca8a04",
    "Russia": "#b3261e",
    "Other": "#6b7280",
    "Total": "#111827",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare monthly EU27 LNG region totals: published LNG plot data vs the "
            "modern Bloomberg pipeline series."
        )
    )
    parser.add_argument(
        "--modern-csv",
        type=Path,
        default=DEFAULT_MODERN_CSV,
        help=f"Modern wide region CSV. Default: {DEFAULT_MODERN_CSV}.",
    )
    parser.add_argument(
        "--legacy-workbook",
        type=Path,
        default=None,
        help=(
            "Published LNG plot data workbook. "
            f"Default: latest 'LNG plot data *.xlsx' in {DEFAULT_LEGACY_PLOT_DIR}."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for CSV/PNG outputs. Default: {DEFAULT_OUTPUT_DIR}.",
    )
    parser.add_argument(
        "--fresh-from",
        default=None,
        help=(
            "Optional dotted reference line and secondary metric window. "
            f"Default: no splice line; also print a {DEFAULT_FRESH_FROM.isoformat()} window."
        ),
    )
    parser.add_argument(
        "--show",
        type=int,
        default=15,
        help="Number of largest absolute monthly differences to print. Default: 15.",
    )
    return parser.parse_args()


def parse_month(value: object) -> dt.date:
    return pd.Timestamp(value).date()


def to_long(wide: pd.DataFrame, value_col: str) -> pd.DataFrame:
    frame = wide.copy()
    frame["month"] = frame["month"].map(parse_month)
    long = frame.melt(id_vars=["month"], value_vars=REGION_COLUMNS, var_name="region", value_name=value_col)
    totals = frame.copy()
    totals["region"] = "Total"
    totals[value_col] = totals[REGION_COLUMNS].sum(axis=1)
    totals = totals[["month", "region", value_col]]
    return pd.concat([long, totals], ignore_index=True)


def load_legacy_plot(path: Path) -> pd.DataFrame:
    wide = pd.read_excel(path, index_col=0)
    wide.index = pd.to_datetime(wide.index)
    missing = [column for column in REGION_COLUMNS if column not in wide.columns]
    if missing:
        raise RuntimeError(f"{path} is missing region columns: {', '.join(missing)}")
    wide = wide[REGION_COLUMNS].reset_index()
    wide = wide.rename(columns={wide.columns[0]: "month"})
    return to_long(wide, "legacyMcm")


def load_modern_regions(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Modern region CSV not found: {path}")
    wide = pd.read_csv(path)
    missing = [column for column in REGION_COLUMNS if column not in wide.columns]
    if missing:
        raise RuntimeError(f"{path} is missing region columns: {', '.join(missing)}")
    return to_long(wide, "modernMcm")


def compare(legacy: pd.DataFrame, modern: pd.DataFrame) -> pd.DataFrame:
    merged = legacy.merge(modern, on=["month", "region"], how="outer")
    merged["legacyMcm"] = pd.to_numeric(merged["legacyMcm"], errors="coerce")
    merged["modernMcm"] = pd.to_numeric(merged["modernMcm"], errors="coerce")
    merged["diffMcm"] = merged["modernMcm"] - merged["legacyMcm"]
    merged["absDiffMcm"] = merged["diffMcm"].abs()
    merged["pctDiff"] = merged["diffMcm"] / merged["legacyMcm"].replace({0: pd.NA}) * 100
    merged["status"] = "matched"
    merged.loc[merged["legacyMcm"].isna(), "status"] = "modern_only"
    merged.loc[merged["modernMcm"].isna(), "status"] = "legacy_only"
    return merged.sort_values(["region", "month"]).reset_index(drop=True)


def mark_splice(ax: plt.Axes, fresh_from: dt.date | None) -> None:
    if fresh_from is None:
        return
    ax.axvline(pd.Timestamp(fresh_from), color="#9ca3af", linestyle=":", linewidth=1.2, zorder=0)


def plot_lines(comparison: pd.DataFrame, fresh_from: dt.date | None, output_path: Path) -> None:
    fig, axes = plt.subplots(len(SERIES_ORDER), 1, figsize=(12, 14), sharex=True)
    for ax, name in zip(axes, SERIES_ORDER):
        data = comparison[comparison["region"] == name].sort_values("month")
        months = pd.to_datetime(data["month"])
        color = SERIES_COLORS[name]
        ax.plot(months, data["legacyMcm"], color="#111827", linestyle="--", linewidth=1.6, label="Published plot data")
        ax.plot(months, data["modernMcm"], color=color, linewidth=2.0, label="Modern pipeline")
        mark_splice(ax, fresh_from)
        ax.set_title(name, loc="left", fontsize=11)
        ax.grid(axis="y", color="#d1d5db", linewidth=0.8, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    axes[0].legend(ncol=2, frameon=False, loc="upper right")
    fig.suptitle("Monthly EU27 LNG by region: published plot data vs modern pipeline", y=0.995)
    fig.text(0.01, 0.5, "million cubic metres", va="center", rotation="vertical")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def plot_differences(comparison: pd.DataFrame, fresh_from: dt.date | None, output_path: Path) -> None:
    fig, axes = plt.subplots(len(SERIES_ORDER), 1, figsize=(12, 13), sharex=True)
    for ax, name in zip(axes, SERIES_ORDER):
        data = comparison[comparison["region"] == name].sort_values("month")
        months = pd.to_datetime(data["month"])
        colors = ["#b3261e" if value < 0 else "#1261a6" for value in data["diffMcm"].fillna(0)]
        ax.bar(months, data["diffMcm"], color=colors, width=20, alpha=0.85)
        ax.axhline(0, color="#111827", linewidth=0.8)
        mark_splice(ax, fresh_from)
        ax.set_title(name, loc="left", fontsize=11)
        ax.grid(axis="y", color="#d1d5db", linewidth=0.8, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    fig.suptitle("Modern pipeline minus published plot data", y=0.995)
    fig.text(0.01, 0.5, "million cubic metres", va="center", rotation="vertical")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def matched_window(
    comparison: pd.DataFrame,
    start: dt.date | None = None,
    end: dt.date | None = None,
) -> pd.DataFrame:
    frame = comparison[comparison["status"] == "matched"].copy()
    if start is not None:
        frame = frame.loc[frame["month"] >= start]
    if end is not None:
        frame = frame.loc[frame["month"] < end]
    return frame


def print_window_metrics(comparison: pd.DataFrame, label: str, start: dt.date | None, end: dt.date | None) -> None:
    regions = matched_window(comparison, start, end)
    regions = regions[regions["region"] != "Total"]
    totals = matched_window(comparison, start, end)
    totals = totals[totals["region"] == "Total"]
    print(f"\n{label}:")
    if regions.empty:
        print("  no overlapping months")
        return
    summary = (
        regions.groupby("region", as_index=False)
        .agg(
            months=("month", "count"),
            meanAbsDiffMcm=("absDiffMcm", "mean"),
            maxAbsDiffMcm=("absDiffMcm", "max"),
            netDiffMcm=("diffMcm", "sum"),
        )
        .set_index("region")
        .reindex([name for name in REGION_COLUMNS if name in set(regions["region"])])
        .reset_index()
    )
    for row in summary.itertuples(index=False):
        print(
            f"  {row.region}: mean_abs={row.meanAbsDiffMcm:.1f} mcm, "
            f"max_abs={row.maxAbsDiffMcm:.1f} mcm, net={row.netDiffMcm:+.0f} mcm"
        )
    if not totals.empty:
        print(
            f"  Total: legacy={totals['legacyMcm'].sum():.0f} mcm, "
            f"modern={totals['modernMcm'].sum():.0f} mcm, "
            f"diff={totals['diffMcm'].sum():+.0f} mcm "
            f"({100 * totals['diffMcm'].sum() / totals['legacyMcm'].sum():+.1f}%)"
        )


def summarize(comparison: pd.DataFrame, fresh_from: dt.date | None, show: int) -> None:
    months = sorted(comparison.loc[comparison["status"] == "matched", "month"].unique())
    print(f"Comparison rows: {len(comparison)}")
    if months:
        print(f"Overlap: {months[0].isoformat()} to {months[-1].isoformat()} ({len(months)} months)")
    if fresh_from is None:
        print("No splice date: modern series is Bloomberg dumps throughout.")
    else:
        print(f"Reference line: {fresh_from.isoformat()}")

    print("\nStatus counts:")
    for status, count in comparison["status"].value_counts().sort_index().items():
        print(f"  {status}: {count}")

    print_window_metrics(comparison, "Full overlap, mean absolute monthly difference by region", None, None)
    print_window_metrics(
        comparison,
        f"Before {DEFAULT_FRESH_FROM.isoformat()}, mean absolute monthly difference by region",
        None,
        DEFAULT_FRESH_FROM,
    )
    print_window_metrics(
        comparison,
        f"From {DEFAULT_FRESH_FROM.isoformat()} onward, mean absolute monthly difference by region",
        DEFAULT_FRESH_FROM,
        None,
    )

    matched_regions = matched_window(comparison, None, None)
    matched_regions = matched_regions[matched_regions["region"] != "Total"]
    print(f"\nLargest {show} monthly region differences over the full overlap:")
    largest_all = matched_regions.sort_values("absDiffMcm", ascending=False).head(show)
    for row in largest_all.itertuples(index=False):
        pct = "" if pd.isna(row.pctDiff) else f" ({row.pctDiff:+.1f}%)"
        print(
            f"  {row.month.isoformat()} {row.region}: "
            f"legacy={row.legacyMcm:.1f}, modern={row.modernMcm:.1f}, "
            f"diff={row.diffMcm:+.1f}{pct}"
        )

    fresh = matched_window(comparison, DEFAULT_FRESH_FROM, None)
    fresh = fresh[fresh["region"] != "Total"]
    print(f"\nLargest {show} monthly region differences from {DEFAULT_FRESH_FROM.isoformat()} onward:")
    largest = fresh.sort_values("absDiffMcm", ascending=False).head(show)
    for row in largest.itertuples(index=False):
        pct = "" if pd.isna(row.pctDiff) else f" ({row.pctDiff:+.1f}%)"
        print(
            f"  {row.month.isoformat()} {row.region}: "
            f"legacy={row.legacyMcm:.1f}, modern={row.modernMcm:.1f}, "
            f"diff={row.diffMcm:+.1f}{pct}"
        )


def main() -> None:
    args = parse_args()
    fresh_from = dt.date.fromisoformat(args.fresh_from) if args.fresh_from else None
    legacy_path = find_legacy_region_workbook(args.legacy_workbook, DEFAULT_LEGACY_PLOT_DIR)
    if legacy_path is None:
        raise FileNotFoundError(
            f"No LNG plot data workbook found in {DEFAULT_LEGACY_PLOT_DIR}"
        )

    comparison = compare(load_legacy_plot(legacy_path), load_modern_regions(args.modern_csv))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.output_dir / "bloomberg_lng_region_comparison.csv"
    lines_path = args.output_dir / "bloomberg_lng_region_comparison_lines.png"
    diffs_path = args.output_dir / "bloomberg_lng_region_comparison_differences.png"

    comparison.to_csv(csv_path, index=False)
    plot_lines(comparison, fresh_from, lines_path)
    plot_differences(comparison, fresh_from, diffs_path)
    print(f"Legacy workbook: {legacy_path}")
    summarize(comparison, fresh_from, args.show)
    print(f"\nCSV: {csv_path}")
    print(f"Lines plot: {lines_path}")
    print(f"Difference plot: {diffs_path}")


if __name__ == "__main__":
    main()
