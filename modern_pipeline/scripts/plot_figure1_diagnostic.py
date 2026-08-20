"""Create simple diagnostic plots for Figure 1-style gas import series."""

from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

os.environ.setdefault("MPLCONFIGDIR", "modern_pipeline/snapshots/.matplotlib")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from modern_pipeline.db import get_database

DEFAULT_MANIFEST_VERSION = "legacy_excel_v3"
DEFAULT_LNG_MANIFEST_VERSION = "gie_alsi_lng_terminals_v2"
DEFAULT_OUTPUT_DIR = Path("modern_pipeline/snapshots")
SOURCE_ORDER = ["Total", "Russia", "Norway", "UK", "Azerbaijan", "Algeria", "LNG"]
SOURCE_COLORS = {
    "Total": "#222222",
    "Russia": "#b3261e",
    "Norway": "#1261a6",
    "UK": "#6b7280",
    "Azerbaijan": "#0f8b8d",
    "Algeria": "#d18400",
    "LNG": "#7c3aed",
}


def figure1_flow_concept(series_key: str) -> str:
    return "net_import" if series_key == "UK" else "gross_import"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export quick matplotlib diagnostics from gas_import_daily."
    )
    parser.add_argument(
        "--manifest-version",
        default=DEFAULT_MANIFEST_VERSION,
        help=f"Pipeline manifest version to plot. Default: {DEFAULT_MANIFEST_VERSION}.",
    )
    parser.add_argument(
        "--lng-manifest-version",
        default=None,
        help=(
            "Optional LNG manifest version to combine with the pipeline manifest. "
            f"Use {DEFAULT_LNG_MANIFEST_VERSION} for the current ALSI v1 rows."
        ),
    )
    parser.add_argument(
        "--collection",
        default="gas_import_daily",
        help="MongoDB curated collection. Default: gas_import_daily.",
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
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for PNG/CSV outputs. Default: {DEFAULT_OUTPUT_DIR}.",
    )
    parser.add_argument(
        "--value-field",
        choices=["valueMcm", "valueGwh", "valueTwh"],
        default="valueMcm",
        help="Value field to plot. Default: valueMcm.",
    )
    parser.add_argument(
        "--frequency",
        choices=["D", "W", "Y"],
        default="W",
        help="Plot daily, weekly, or annual sums. Default: W.",
    )
    return parser.parse_args()


def parse_date(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    return dt.datetime.combine(dt.date.fromisoformat(value), dt.time.min)


def date_filter(from_date: Optional[dt.datetime], to_date: Optional[dt.datetime]) -> Dict[str, Any]:
    query: Dict[str, Any] = {}
    if from_date:
        query["$gte"] = from_date
    if to_date:
        query["$lte"] = to_date
    return query


def load_manifest_daily(
    collection_name: str,
    manifest_version: str,
    series_keys: List[str],
    value_field: str,
    from_date: Optional[dt.datetime],
    to_date: Optional[dt.datetime],
) -> pd.DataFrame:
    if not series_keys:
        return pd.DataFrame(columns=["date", "aggregationLevel", "seriesKey", value_field])
    db = get_database()
    query: Dict[str, Any] = {
        "manifestVersion": manifest_version,
        "aggregationLevel": "source_group",
        "seriesKey": {"$in": series_keys},
    }
    date_query = date_filter(from_date, to_date)
    if date_query:
        query["date"] = date_query

    projection = {
        "_id": 0,
        "date": 1,
        "aggregationLevel": 1,
        "seriesKey": 1,
        "flowConcept": 1,
        value_field: 1,
    }
    rows = list(db[collection_name].find(query, projection).sort("date", 1))
    if not rows:
        return pd.DataFrame(columns=["date", "aggregationLevel", "seriesKey", value_field])

    df = pd.DataFrame(rows)
    if "flowConcept" not in df.columns:
        df["flowConcept"] = pd.NA
    has_flow_concept = df["flowConcept"].notna()
    if has_flow_concept.any():
        expected = df["seriesKey"].map(figure1_flow_concept)
        df = df[(~has_flow_concept) | (df["flowConcept"] == expected)].copy()
    df["date"] = pd.to_datetime(df["date"])
    df[value_field] = pd.to_numeric(df[value_field], errors="coerce").fillna(0)
    return df


def load_daily(args: argparse.Namespace) -> pd.DataFrame:
    from_date = parse_date(args.from_date)
    to_date = parse_date(args.to_date)
    pipeline_series = [item for item in SOURCE_ORDER if item not in {"Total", "LNG"}]
    frames = [
        load_manifest_daily(
            args.collection,
            args.manifest_version,
            pipeline_series,
            args.value_field,
            from_date,
            to_date,
        )
    ]
    if args.lng_manifest_version:
        frames.append(
            load_manifest_daily(
                args.collection,
                args.lng_manifest_version,
                ["LNG"],
                args.value_field,
                from_date,
                to_date,
            )
        )

    df = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)
    if df.empty:
        raise RuntimeError(f"No source_group rows found in {args.collection} for requested manifest version(s)")
    return df


def pivot_series(df: pd.DataFrame, value_field: str, frequency: str) -> pd.DataFrame:
    pivot = df.pivot_table(index="date", columns="seriesKey", values=value_field, aggfunc="sum")
    pivot = pivot.sort_index().fillna(0)
    for column in SOURCE_ORDER:
        if column not in pivot.columns:
            pivot[column] = 0.0
    component_columns = [column for column in SOURCE_ORDER if column != "Total"]
    pivot["Total"] = pivot[component_columns].sum(axis=1)
    pivot = pivot[SOURCE_ORDER]
    if frequency == "W":
        pivot = pivot.resample("W-SUN").sum()
    elif frequency == "Y":
        pivot = pivot.resample("YS").sum()
    return pivot


def value_label(value_field: str, frequency: str) -> str:
    units = {
        "valueMcm": "million cubic metres",
        "valueGwh": "GWh",
        "valueTwh": "TWh",
    }[value_field]
    period = {"D": "daily", "W": "weekly", "Y": "annual"}[frequency]
    return f"{period} {units}"


def configure_x_axis(ax: plt.Axes, frequency: str) -> None:
    if frequency == "Y":
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    else:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))


def plot_lines(pivot: pd.DataFrame, args: argparse.Namespace, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 7))
    for column in SOURCE_ORDER:
        width = 2.6 if column == "Total" else 1.8
        alpha = 1.0 if column == "Total" else 0.9
        ax.plot(
            pivot.index,
            pivot[column],
            label=column,
            color=SOURCE_COLORS.get(column),
            linewidth=width,
            marker="o" if args.frequency == "Y" else None,
            markersize=4 if args.frequency == "Y" else 0,
            alpha=alpha,
        )
    ax.set_title(f"European Natural Gas Imports ({args.manifest_version})")
    ax.set_ylabel(value_label(args.value_field, args.frequency))
    ax.set_xlabel("")
    ax.grid(axis="y", color="#d1d5db", linewidth=0.8, alpha=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(ncol=3, frameon=False)
    configure_x_axis(ax, args.frequency)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def plot_small_multiples(pivot: pd.DataFrame, args: argparse.Namespace, output_path: Path) -> None:
    columns = ["Russia", "Norway", "UK", "Azerbaijan", "Algeria", "LNG"]
    fig, axes = plt.subplots(len(columns), 1, figsize=(12, 10), sharex=True)
    for ax, column in zip(axes, columns):
        ax.plot(
            pivot.index,
            pivot[column],
            color=SOURCE_COLORS.get(column),
            linewidth=1.8,
            marker="o" if args.frequency == "Y" else None,
            markersize=4 if args.frequency == "Y" else 0,
        )
        ax.set_title(column, loc="left", fontsize=11)
        ax.grid(axis="y", color="#d1d5db", linewidth=0.8, alpha=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    configure_x_axis(axes[-1], args.frequency)
    fig.suptitle(f"Source Detail ({args.manifest_version})", y=0.995)
    fig.text(0.01, 0.5, value_label(args.value_field, args.frequency), va="center", rotation="vertical")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / ".matplotlib").mkdir(parents=True, exist_ok=True)

    daily = load_daily(args)
    pivot = pivot_series(daily, args.value_field, args.frequency)
    suffix = {"D": "daily", "W": "weekly", "Y": "annual"}[args.frequency]
    manifest_slug = args.manifest_version
    if args.lng_manifest_version:
        manifest_slug = f"{manifest_slug}_plus_{args.lng_manifest_version}"
    csv_path = args.output_dir / f"figure1_diagnostic_{manifest_slug}_{suffix}.csv"
    line_path = args.output_dir / f"figure1_diagnostic_{manifest_slug}_{suffix}_lines.png"
    small_path = args.output_dir / f"figure1_diagnostic_{manifest_slug}_{suffix}_small_multiples.png"

    pivot.to_csv(csv_path, index_label="date")
    plot_lines(pivot, args, line_path)
    plot_small_multiples(pivot, args, small_path)

    print(f"Rows plotted: {len(pivot)}")
    print(f"Date coverage: {pivot.index.min().date()} to {pivot.index.max().date()}")
    print(f"CSV: {csv_path}")
    print(f"Line plot: {line_path}")
    print(f"Small multiples: {small_path}")


if __name__ == "__main__":
    main()
