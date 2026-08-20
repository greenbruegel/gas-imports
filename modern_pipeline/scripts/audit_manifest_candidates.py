"""Audit rule-based draft manifest candidates from the enriched ENTSOG catalog."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from modern_pipeline.db import get_database

EU27_COUNTRY_KEYS: Set[str] = {
    "AT",
    "BE",
    "BG",
    "HR",
    "CY",
    "CZ",
    "DK",
    "EE",
    "FI",
    "FR",
    "DE",
    "GR",
    "HU",
    "IE",
    "IT",
    "LV",
    "LT",
    "LU",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SK",
    "SI",
    "ES",
    "SE",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit draft import-manifest candidates from enriched connection points."
    )
    parser.add_argument(
        "--collection",
        default="entsog_connection_points",
        help="MongoDB connection-points collection. Default: entsog_connection_points.",
    )
    parser.add_argument(
        "--point-key",
        action="append",
        dest="point_keys",
        help="Specific catalog pointKey to audit. Can be passed multiple times. Defaults to all catalog candidates.",
    )
    parser.add_argument(
        "--max-points",
        type=int,
        default=None,
        help="Maximum points to audit. Useful for quick checks.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Optional path to write the audit table as CSV.",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=20,
        help="Number of point summaries to print. Default: 20.",
    )
    return parser.parse_args()


def point_direction_id(operator_key: Optional[str], point_key: Optional[str], direction_key: Optional[str]) -> Optional[str]:
    if not operator_key or not point_key or not direction_key:
        return None
    return f"{operator_key}{point_key}{direction_key}".lower()


def load_docs(collection_name: str, point_keys: Optional[List[str]], max_points: Optional[int]) -> List[Dict[str, Any]]:
    db = get_database()
    collection = db[collection_name]
    if point_keys:
        query = {"pointKey": {"$in": list(dict.fromkeys(point_keys))}}
    else:
        query = {"catalogStatus": "candidate"}

    cursor = collection.find(query).sort("pointKey", 1)
    docs = list(cursor)
    if max_points is not None:
        if max_points < 1:
            raise ValueError("--max-points must be >= 1 when provided")
        docs = docs[:max_points]
    return docs


def opd_ids(doc: Dict[str, Any]) -> Set[str]:
    return {
        row["pointDirection"]
        for row in doc.get("pointDirections") or []
        if row.get("pointDirection")
    }


def non_empty_join(values: Iterable[Optional[str]], sep: str = ";") -> str:
    return sep.join(str(value) for value in values if value not in (None, ""))


def unique_sorted(values: Iterable[Optional[str]]) -> List[str]:
    return sorted({str(value) for value in values if value not in (None, "")})


def derive_candidates(doc: Dict[str, Any]) -> Dict[str, Any]:
    interconnections = doc.get("interconnections") or []
    point_directions = doc.get("pointDirections") or []
    available_opd_ids = opd_ids(doc)

    selected_candidates: List[str] = []
    selected_matched: List[str] = []
    selected_unmatched: List[str] = []
    fallback_candidates: List[str] = []
    fallback_matched: List[str] = []
    source_country_keys: List[str] = []
    source_country_labels: List[str] = []
    import_country_keys: List[str] = []
    import_country_labels: List[str] = []

    import_rows = []
    export_rows = []

    for row in interconnections:
        from_country = row.get("fromCountryKey")
        to_country = row.get("toCountryKey")
        from_direction = row.get("fromDirectionKey")
        to_direction = row.get("toDirectionKey")

        is_non_eu_to_eu_entry = (
            from_country not in EU27_COUNTRY_KEYS
            and to_country in EU27_COUNTRY_KEYS
            and to_direction == "entry"
        )
        is_eu_to_non_eu_exit = (
            from_country in EU27_COUNTRY_KEYS
            and to_country not in EU27_COUNTRY_KEYS
            and from_direction == "exit"
        )

        if is_non_eu_to_eu_entry:
            import_rows.append(row)
            selected = point_direction_id(row.get("toOperatorKey"), row.get("toPointKey"), row.get("toDirectionKey"))
            fallback = point_direction_id(row.get("fromOperatorKey"), row.get("fromPointKey"), row.get("fromDirectionKey"))
            if selected:
                selected_candidates.append(selected)
                if selected in available_opd_ids:
                    selected_matched.append(selected)
                else:
                    selected_unmatched.append(selected)
            if fallback:
                fallback_candidates.append(fallback)
                if fallback in available_opd_ids:
                    fallback_matched.append(fallback)
            source_country_keys.append(row.get("fromCountryKey"))
            source_country_labels.append(row.get("fromCountryLabel"))
            import_country_keys.append(row.get("toCountryKey"))
            import_country_labels.append(row.get("toCountryLabel"))

        if is_eu_to_non_eu_exit:
            export_rows.append(row)

    selected_candidates = unique_sorted(selected_candidates)
    selected_matched = unique_sorted(selected_matched)
    selected_unmatched = unique_sorted(selected_unmatched)
    fallback_candidates = unique_sorted(fallback_candidates)
    fallback_matched = unique_sorted(fallback_matched)

    flags: List[str] = []
    if not point_directions:
        flags.append("no_opd")
    if not interconnections:
        flags.append("no_interconnections")
    if len(doc.get("relatedOperationalPointKeys") or []) > 1:
        flags.append("grouped_point")
    if not import_rows:
        flags.append("no_import_entry")
    if selected_unmatched:
        flags.append("import_entry_not_in_opd")
    if len(selected_matched) > 1:
        flags.append("multiple_import_entries")
    if len(unique_sorted(source_country_keys)) == 0:
        flags.append("source_country_missing")
    if import_rows and export_rows:
        flags.append("bidirectional_point")

    return {
        "catalogPointKey": doc.get("pointKey"),
        "pointLabel": doc.get("pointLabel"),
        "connectionpointsImportFromCountryKey": doc.get("importFromCountryKey"),
        "connectionpointsImportFromCountryLabel": doc.get("importFromCountryLabel"),
        "sourceCountryKeys": unique_sorted(source_country_keys),
        "sourceCountryLabels": unique_sorted(source_country_labels),
        "importCountryKeys": unique_sorted(import_country_keys),
        "importCountryLabels": unique_sorted(import_country_labels),
        "relatedOperationalPointKeys": doc.get("relatedOperationalPointKeys") or [],
        "availablePointDirections": unique_sorted(available_opd_ids),
        "recommendedSelectedPointDirections": selected_matched,
        "unmatchedSelectedPointDirections": selected_unmatched,
        "fallbackPointDirections": fallback_matched,
        "fallbackCandidatePointDirections": fallback_candidates,
        "nInterconnections": len(interconnections),
        "nPointDirections": len(point_directions),
        "flags": sorted(flags),
    }


def flatten_row(row: Dict[str, Any]) -> Dict[str, Any]:
    flattened: Dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, list):
            flattened[key] = non_empty_join(value)
        else:
            flattened[key] = value
    return flattened


def summarize(rows: List[Dict[str, Any]], show: int) -> None:
    print(f"Manifest candidate rows: {len(rows)}")

    flag_counts: Dict[str, int] = {}
    selected_count = 0
    for row in rows:
        if row["recommendedSelectedPointDirections"]:
            selected_count += 1
        for flag in row["flags"]:
            flag_counts[flag] = flag_counts.get(flag, 0) + 1

    print(f"Rows with recommended selected pointDirections: {selected_count}")
    print("\nFlag Counts:")
    if not flag_counts:
        print("  none")
    for flag, count in sorted(flag_counts.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {flag}: {count}")

    print("\nSample:")
    for row in rows[:show]:
        print(
            "  "
            f"{row['catalogPointKey']}: {row['pointLabel']} | "
            f"source={row['sourceCountryKeys']} -> import={row['importCountryKeys']} | "
            f"selected={row['recommendedSelectedPointDirections']} | "
            f"fallback={row['fallbackPointDirections']} | "
            f"flags={row['flags']}"
        )


def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flattened_rows = [flatten_row(row) for row in rows]
    if not flattened_rows:
        print(f"No rows to write to {path}")
        return

    fieldnames = list(flattened_rows[0].keys())
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flattened_rows)
    print(f"\nWrote CSV: {path}")


def main() -> None:
    args = parse_args()
    docs = load_docs(
        collection_name=args.collection,
        point_keys=args.point_keys,
        max_points=args.max_points,
    )
    if not docs:
        raise SystemExit("No connection-point documents found.")

    rows = [derive_candidates(doc) for doc in docs]
    summarize(rows, show=args.show)

    if args.csv:
        write_csv(rows, args.csv)


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
