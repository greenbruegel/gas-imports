"""Compare curated gas_import_daily weekly values with legacy country workbook."""

from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

os.environ.setdefault("MPLCONFIGDIR", "modern_pipeline/snapshots/.matplotlib")

import matplotlib.pyplot as plt
import pandas as pd

from modern_pipeline.db import get_database

DEFAULT_LEGACY_WORKBOOK = Path("legacy_country_data.xlsx")
DEFAULT_OUTPUT_DIR = Path("modern_pipeline/snapshots/legacy_country_data")
DEFAULT_MANIFEST_VERSION = "legacy_excel_v3"
DEFAULT_LNG_MANIFEST_VERSION = "gie_alsi_lng_terminals_v2"
DEFAULT_SERIES = ("Russia", "Norway", "UK", "Azerbaijan", "Algeria", "Libya", "LNG", "Total")
SOURCE_COLORS = {
    "Total": "#222222",
    "Russia": "#b3261e",
    "Norway": "#1261a6",
    "UK": "#6b7280",
    "Azerbaijan": "#0f8b8d",
    "Algeria": "#d18400",
    "Libya": "#92400e",
    "LNG": "#7c3aed",
}
UK_EDITIONS = (
    {
        "name": "uk_gross",
        "flowConcept": "gross_import",
        "legacySheet": "EU27 gross imports ",
        "columnPrefixes": {"UK": "UK"},
        "label": "UK gross",
        "title": "Weekly EU27 UK gross imports: modern pipeline vs legacy Excel",
        "differenceTitle": "Modern pipeline minus legacy Excel (UK gross)",
    },
    {
        "name": "uk_net",
        "flowConcept": "net_import",
        "legacySheet": "EU27 net UK imports",
        "columnPrefixes": {"UK": "EU_UK_net"},
        "label": "UK net",
        "title": "Weekly EU27 UK net imports: modern pipeline vs legacy Excel",
        "differenceTitle": "Modern pipeline minus legacy Excel (UK net)",
    },
)


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
        default=DEFAULT_LNG_MANIFEST_VERSION,
        help=(
            "LNG manifest version in gas_import_daily. "
            f"Default: {DEFAULT_LNG_MANIFEST_VERSION}, which excludes Spain's TVB virtual tank. "
            "Pass gie_alsi_lng_terminals_v1 to compare the older inclusive LNG series."
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
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for per-source CSVs and plots. Default: {DEFAULT_OUTPUT_DIR}.",
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


def series_column_prefix(series_key: str, column_prefixes: Optional[Dict[str, str]] = None) -> str:
    if column_prefixes and series_key in column_prefixes:
        return column_prefixes[series_key]
    return "EU" if series_key == "Total" else series_key


def load_legacy(
    path: Path,
    sheet: str,
    series: Sequence[str],
    years: Optional[Sequence[int]],
    column_prefixes: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
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
            prefix = series_column_prefix(series_key, column_prefixes)
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
    if not rows:
        return pd.DataFrame(columns=["seriesKey", "isoYear", "isoWeek", "legacyMcm"])
    return pd.DataFrame(rows)


def query_modern_rows(
    collection_name: str,
    manifest_version: str,
    series: Sequence[str],
    years: Optional[Sequence[int]],
    flow_concept: Optional[str] = None,
) -> pd.DataFrame:
    empty = pd.DataFrame(columns=["seriesKey", "isoYear", "isoWeek", "modernMcm"])
    if not series:
        return empty
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
        return empty
    df = pd.DataFrame(rows)
    if "flowConcept" not in df.columns:
        df["flowConcept"] = pd.NA
    has_flow_concept = df["flowConcept"].notna()
    if has_flow_concept.any():
        expected = (
            pd.Series(flow_concept, index=df.index)
            if flow_concept
            else df["seriesKey"].map(figure1_flow_concept)
        )
        df = df[(~has_flow_concept) | (df["flowConcept"] == expected)].copy()
    if df.empty:
        return empty
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
    merged["weekStart"] = [
        iso_week_start(year, week) for year, week in zip(merged["isoYear"], merged["isoWeek"])
    ]
    columns = [
        "seriesKey",
        "isoYear",
        "isoWeek",
        "weekStart",
        "legacyMcm",
        "modernMcm",
        "diffMcm",
        "absDiffMcm",
        "pctDiff",
        "status",
    ]
    return merged.sort_values(["seriesKey", "isoYear", "isoWeek"]).reset_index(drop=True)[columns]


def iso_week_start(year: Any, week: Any) -> pd.Timestamp:
    try:
        if pd.isna(year) or pd.isna(week):
            return pd.NaT
        return pd.Timestamp(dt.date.fromisocalendar(int(year), int(week), 1))
    except (TypeError, ValueError):
        return pd.NaT


def series_to_plot(comparison: pd.DataFrame, requested: Sequence[str]) -> List[str]:
    present = set(comparison["seriesKey"])
    ordered = [name for name in requested if name in present]
    extras = sorted(present.difference(ordered))
    return ordered + extras


def source_slug(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def plot_lines(
    comparison: pd.DataFrame,
    series: Sequence[str],
    output_path: Path,
    title: str = "Weekly EU27 imports by source: modern pipeline vs legacy Excel",
    series_labels: Optional[Dict[str, str]] = None,
) -> None:
    height = max(4.2, 2.1 * len(series)) if len(series) == 1 else 2.1 * len(series)
    fig, axes = plt.subplots(len(series), 1, figsize=(12, height), sharex=True)
    if len(series) == 1:
        axes = [axes]
    for ax, name in zip(axes, series):
        data = comparison[comparison["seriesKey"] == name].sort_values("weekStart")
        weeks = pd.to_datetime(data["weekStart"])
        color = SOURCE_COLORS.get(name, "#1261a6")
        ax.plot(weeks, data["legacyMcm"], color="#111827", linestyle="--", linewidth=1.6, label="Legacy Excel")
        ax.plot(weeks, data["modernMcm"], color=color, linewidth=2.0, label="Modern pipeline")
        ax.set_title((series_labels or {}).get(name, name), loc="left", fontsize=11)
        ax.grid(axis="y", color="#d1d5db", linewidth=0.8, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    axes[0].legend(ncol=2, frameon=False, loc="upper right")
    fig.suptitle(title, y=0.995)
    fig.text(0.01, 0.5, "million cubic metres", va="center", rotation="vertical")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def plot_differences(
    comparison: pd.DataFrame,
    series: Sequence[str],
    output_path: Path,
    title: str = "Modern pipeline minus legacy Excel",
    series_labels: Optional[Dict[str, str]] = None,
) -> None:
    height = max(4.0, 2.0 * len(series)) if len(series) == 1 else 2.0 * len(series)
    fig, axes = plt.subplots(len(series), 1, figsize=(12, height), sharex=True)
    if len(series) == 1:
        axes = [axes]
    for ax, name in zip(axes, series):
        data = comparison[comparison["seriesKey"] == name].dropna(subset=["diffMcm"]).sort_values("weekStart")
        weeks = pd.to_datetime(data["weekStart"])
        colors = ["#b3261e" if value < 0 else "#1261a6" for value in data["diffMcm"]]
        ax.bar(weeks, data["diffMcm"], color=colors, width=5, alpha=0.85)
        ax.axhline(0, color="#111827", linewidth=0.8)
        ax.set_title((series_labels or {}).get(name, name), loc="left", fontsize=11)
        ax.grid(axis="y", color="#d1d5db", linewidth=0.8, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    fig.suptitle(title, y=0.995)
    fig.text(0.01, 0.5, "million cubic metres", va="center", rotation="vertical")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


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


def write_source_outputs(
    comparison: pd.DataFrame,
    series_key: str,
    output_dir: Path,
    stem: str,
    label: str,
    lines_title: str,
    differences_title: str,
) -> List[Path]:
    subset = comparison[comparison["seriesKey"] == series_key].copy()
    if subset.empty:
        return []
    csv_path = output_dir / f"{stem}.csv"
    lines_path = output_dir / f"{stem}_lines.png"
    diffs_path = output_dir / f"{stem}_differences.png"
    subset.to_csv(csv_path, index=False)
    labels = {series_key: label}
    plot_lines(subset, [series_key], lines_path, title=lines_title, series_labels=labels)
    plot_differences(subset, [series_key], diffs_path, title=differences_title, series_labels=labels)
    return [csv_path, lines_path, diffs_path]


def write_comparison(
    comparison: pd.DataFrame,
    series: Sequence[str],
    output_dir: Path,
    show: int,
    lines_title: str,
    differences_title: str,
    series_labels: Optional[Dict[str, str]] = None,
    combined_stem: str = "overview",
    write_combined_csv: bool = True,
    write_combined_plots: bool = True,
    source_stems: Optional[Dict[str, str]] = None,
    skip_source_files: Optional[Sequence[str]] = None,
) -> None:
    plotted = series_to_plot(comparison, series)
    skip = set(skip_source_files or [])
    output_dir.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []
    if write_combined_csv:
        combined_csv = output_dir / f"{combined_stem}.csv"
        comparison.to_csv(combined_csv, index=False)
        written.append(combined_csv)
    if write_combined_plots and plotted:
        lines_path = output_dir / f"{combined_stem}_lines.png"
        diffs_path = output_dir / f"{combined_stem}_differences.png"
        plot_lines(comparison, plotted, lines_path, title=lines_title, series_labels=series_labels)
        plot_differences(
            comparison,
            plotted,
            diffs_path,
            title=differences_title,
            series_labels=series_labels,
        )
        written.extend([lines_path, diffs_path])
    for name in plotted:
        if name in skip:
            continue
        label = (series_labels or {}).get(name, name)
        stem = (source_stems or {}).get(name) or source_slug(label)
        written.extend(
            write_source_outputs(
                comparison,
                name,
                output_dir,
                stem,
                label,
                lines_title=f"Weekly EU27 {label} imports: modern pipeline vs legacy Excel",
                differences_title=f"Modern pipeline minus legacy Excel ({label})",
            )
        )
    summarize(comparison, show)
    print(f"\nOutput directory: {output_dir}")
    for path in written:
        print(f"  {path}")


def compare_uk_edition(
    args: argparse.Namespace,
    edition: Dict[str, Any],
) -> None:
    series = ["UK"]
    print(f"\n=== UK edition: {edition['label']} ({edition['flowConcept']}) ===")
    legacy = load_legacy(
        args.legacy_workbook,
        edition["legacySheet"],
        series,
        args.year,
        column_prefixes=edition["columnPrefixes"],
    )
    modern = query_modern_rows(
        args.collection,
        args.manifest_version,
        series,
        args.year,
        flow_concept=edition["flowConcept"],
    )
    comparison = compare(legacy, modern)
    comparison.insert(1, "flowConcept", edition["flowConcept"])
    write_comparison(
        comparison,
        series,
        args.output_dir,
        args.show,
        lines_title=edition["title"],
        differences_title=edition["differenceTitle"],
        series_labels={"UK": edition["label"]},
        combined_stem=edition["name"],
        write_combined_csv=False,
        write_combined_plots=False,
        source_stems={"UK": edition["name"]},
    )


def main() -> None:
    args = parse_args()
    series = list(args.series or DEFAULT_SERIES)
    legacy = load_legacy(args.legacy_workbook, args.sheet, series, args.year)
    modern = load_modern(args.collection, args.manifest_version, args.lng_manifest_version, series, args.year)
    comparison = compare(legacy, modern)
    write_comparison(
        comparison,
        series,
        args.output_dir,
        args.show,
        lines_title="Weekly EU27 imports by source: modern pipeline vs legacy Excel",
        differences_title="Modern pipeline minus legacy Excel",
        skip_source_files=["UK"],
    )
    for edition in UK_EDITIONS:
        compare_uk_edition(args, edition)


if __name__ == "__main__":
    main()
