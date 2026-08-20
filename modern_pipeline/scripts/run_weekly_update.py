"""Run the safe weekly modern gas-import update workflow."""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

from modern_pipeline.db import get_database

DEFAULT_PIPELINE_MANIFEST_VERSION = "legacy_excel_v4"
DEFAULT_LNG_MANIFEST_VERSION = "gie_alsi_lng_terminals_v2"
DEFAULT_OUTPUT_COLLECTION = "gas_import_daily"
DEFAULT_DATA_LAG_DAYS = 3
DEFAULT_REBUILD_WEEKS = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch recent raw ENTSOG/GIE data and rebuild gas_import_daily only "
            "through the latest complete ISO week."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Plan and dry-run child jobs. This is the default.")
    mode.add_argument("--write", action="store_true", help="Run child jobs with writes enabled.")
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Print computed dates and commands without running child jobs or touching MongoDB.",
    )
    parser.add_argument(
        "--today",
        default=None,
        help="Override today's date for testing, YYYY-MM-DD. Default: system local date.",
    )
    parser.add_argument(
        "--data-lag-days",
        type=int,
        default=DEFAULT_DATA_LAG_DAYS,
        help=f"Do not fetch newer than today minus this many days. Default: {DEFAULT_DATA_LAG_DAYS}.",
    )
    parser.add_argument(
        "--rebuild-weeks",
        type=int,
        default=DEFAULT_REBUILD_WEEKS,
        help=f"Number of complete ISO weeks to rebuild in gas_import_daily. Default: {DEFAULT_REBUILD_WEEKS}.",
    )
    parser.add_argument(
        "--pipeline-manifest-version",
        default=DEFAULT_PIPELINE_MANIFEST_VERSION,
        help=f"Pipeline manifest version. Default: {DEFAULT_PIPELINE_MANIFEST_VERSION}.",
    )
    parser.add_argument(
        "--lng-manifest-version",
        default=DEFAULT_LNG_MANIFEST_VERSION,
        help=f"LNG manifest version. Default: {DEFAULT_LNG_MANIFEST_VERSION}.",
    )
    parser.add_argument(
        "--output-collection",
        default=DEFAULT_OUTPUT_COLLECTION,
        help=f"Curated MongoDB collection. Default: {DEFAULT_OUTPUT_COLLECTION}.",
    )
    parser.add_argument(
        "--skip-route-groups",
        action="store_true",
        help="Do not materialize pipeline route_group rows.",
    )
    parser.add_argument(
        "--skip-partial-week-purge",
        action="store_true",
        help="Do not purge materialized rows after the latest complete Sunday.",
    )
    return parser.parse_args()


def parse_today(value: Optional[str]) -> dt.date:
    if value:
        return dt.date.fromisoformat(value)
    return dt.date.today()


def latest_sunday_on_or_before(day: dt.date) -> dt.date:
    days_since_sunday = (day.weekday() + 1) % 7
    return day - dt.timedelta(days=days_since_sunday)


def command_to_text(command: List[str]) -> str:
    return " ".join(command)


def run_command(command: List[str], dry_run: bool) -> None:
    started_at = time.monotonic()
    print(f"\n$ {command_to_text(command)}", flush=True)
    subprocess.run(command, check=True)
    elapsed = time.monotonic() - started_at
    mode = "dry-run" if dry_run else "write"
    print(f"Finished {mode} command in {elapsed:.1f}s.", flush=True)


def purge_partial_week_rows(
    collection_name: str,
    manifest_versions: List[str],
    from_exclusive: dt.date,
    to_inclusive: dt.date,
    write: bool,
) -> int:
    if to_inclusive <= from_exclusive:
        print("\nNo partial-week materialized window to purge.")
        return 0

    db = get_database()
    query = {
        "manifestVersion": {"$in": manifest_versions},
        "date": {
            "$gt": dt.datetime.combine(from_exclusive, dt.time.min),
            "$lte": dt.datetime.combine(to_inclusive, dt.time.min),
        },
    }
    if not write:
        count = db[collection_name].count_documents(query)
        print(
            "\nPartial-week purge dry-run: "
            f"would delete {count} rows from {collection_name} for "
            f"{from_exclusive + dt.timedelta(days=1)} to {to_inclusive}."
        )
        return count

    result = db[collection_name].delete_many(query)
    print(
        "\nPartial-week purge complete: "
        f"deleted {result.deleted_count} rows from {collection_name} for "
        f"{from_exclusive + dt.timedelta(days=1)} to {to_inclusive}."
    )
    return result.deleted_count


def main() -> None:
    args = parse_args()
    if args.data_lag_days < 0:
        raise RuntimeError("--data-lag-days must be >= 0")
    if args.rebuild_weeks < 1:
        raise RuntimeError("--rebuild-weeks must be >= 1")

    write = bool(args.write)
    dry_run = not write
    today = parse_today(args.today)
    raw_to = today - dt.timedelta(days=args.data_lag_days)
    materialize_to = latest_sunday_on_or_before(raw_to)
    materialize_from = materialize_to - dt.timedelta(days=args.rebuild_weeks * 7 - 1)
    fetch_from = materialize_from
    mode_flag = "--write" if write else "--dry-run"

    print("Weekly update plan")
    print(f"  today: {today}")
    print(f"  data lag days: {args.data_lag_days}")
    print(f"  raw fetch window: {fetch_from} to {raw_to}")
    print(f"  materialize complete-week window: {materialize_from} to {materialize_to}")
    print(f"  partial-week purge window: {materialize_to + dt.timedelta(days=1)} to {raw_to}")
    print(f"  pipeline manifest: {args.pipeline_manifest_version}")
    print(f"  LNG manifest: {args.lng_manifest_version}")

    commands: List[List[str]] = [
        [
            sys.executable,
            "-m",
            "modern_pipeline.scripts.fetch_entsog_raw_observations",
            "--mode",
            "daily",
            "--from",
            fetch_from.isoformat(),
            "--to",
            raw_to.isoformat(),
            mode_flag,
        ],
        [
            sys.executable,
            "-m",
            "modern_pipeline.scripts.fetch_gie_lng_raw_observations",
            "--mode",
            "daily",
            "--manifest-version",
            args.lng_manifest_version,
            "--from",
            fetch_from.isoformat(),
            "--to",
            raw_to.isoformat(),
            mode_flag,
        ],
    ]

    pipeline_materialize = [
        sys.executable,
        "-m",
        "modern_pipeline.scripts.materialize_gas_import_daily",
        "--manifest-version",
        args.pipeline_manifest_version,
        "--from",
        materialize_from.isoformat(),
        "--to",
        materialize_to.isoformat(),
        "--delete-existing-window",
        mode_flag,
    ]
    if not args.skip_route_groups:
        pipeline_materialize.append("--include-route-groups")
    commands.append(pipeline_materialize)
    commands.append(
        [
            sys.executable,
            "-m",
            "modern_pipeline.scripts.materialize_gie_lng_daily",
            "--manifest-version",
            args.lng_manifest_version,
            "--from",
            materialize_from.isoformat(),
            "--to",
            materialize_to.isoformat(),
            "--delete-existing-window",
            mode_flag,
        ]
    )

    print("\nCommands:")
    for command in commands:
        print(f"  {command_to_text(command)}")

    if args.plan_only:
        print("\nPlan only. No child jobs run and no MongoDB rows touched.")
        return

    for command in commands[:2]:
        run_command(command, dry_run)

    if not args.skip_partial_week_purge:
        purge_partial_week_rows(
            args.output_collection,
            [args.pipeline_manifest_version, args.lng_manifest_version],
            materialize_to,
            raw_to,
            write,
        )

    for command in commands[2:]:
        run_command(command, dry_run)

    print("\nWeekly update complete.")


if __name__ == "__main__":
    main()
