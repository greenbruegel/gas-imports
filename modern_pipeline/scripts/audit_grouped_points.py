"""Audit grouped ENTSOG points and optionally expand OPD coverage for related keys."""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Set

from pymongo import UpdateOne

from modern_pipeline.db import get_database
from modern_pipeline.scripts.enrich_operator_point_directions import (
    MIN_REQUEST_INTERVAL_SEC,
    fetch_point_directions,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit connection points whose interconnections reference multiple operational point keys, "
            "and optionally fetch missing operatorPointDirections for those related keys."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Report grouped points and missing OPD coverage without writing. This is the default.",
    )
    mode.add_argument(
        "--write",
        action="store_true",
        help="Fetch missing related-key OPD rows and write merged pointDirections back to MongoDB.",
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
        help="Specific catalog pointKey to audit. Can be passed multiple times. Defaults to all grouped points.",
    )
    parser.add_argument(
        "--max-points",
        type=int,
        default=None,
        help="Maximum grouped points to process. Useful for testing.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum operatorPointDirections rows requested per related point key. Default: 100.",
    )
    return parser.parse_args()


def point_direction_key(row: Dict[str, Any]) -> Optional[str]:
    return row.get("pointDirection")


def opd_point_keys(point_directions: Iterable[Dict[str, Any]]) -> Set[str]:
    return {row["pointKey"] for row in point_directions if row.get("pointKey")}


def load_docs(collection_name: str, point_keys: Optional[List[str]], max_points: Optional[int]) -> List[Dict[str, Any]]:
    db = get_database()
    collection = db[collection_name]

    if point_keys:
        query = {"pointKey": {"$in": list(dict.fromkeys(point_keys))}}
    else:
        query = {
            "catalogStatus": "candidate",
            "relatedOperationalPointKeys.1": {"$exists": True},
        }

    cursor = collection.find(query).sort("pointKey", 1)
    docs = list(cursor)
    if max_points is not None:
        if max_points < 1:
            raise ValueError("--max-points must be >= 1 when provided")
        docs = docs[:max_points]
    return docs


def missing_related_keys(doc: Dict[str, Any]) -> List[str]:
    related_keys = set(doc.get("relatedOperationalPointKeys") or [])
    if not related_keys:
        related_keys = {doc.get("pointKey")} if doc.get("pointKey") else set()
    existing_keys = opd_point_keys(doc.get("pointDirections") or [])
    already_queried_keys = set(doc.get("pointDirectionsQueriedKeys") or [])
    return sorted(related_keys - existing_keys - already_queried_keys)


def merge_point_directions(existing: List[Dict[str, Any]], fetched: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for row in existing:
        key = point_direction_key(row)
        if key:
            merged[key] = row
    for row in fetched:
        key = point_direction_key(row)
        if key:
            merged[key] = row
    return sorted(
        merged.values(),
        key=lambda row: (
            row.get("pointKey") or "",
            row.get("operatorKey") or "",
            row.get("directionKey") or "",
        ),
    )


def summarize(docs: List[Dict[str, Any]]) -> None:
    print(f"Grouped/catalog points checked: {len(docs)}")
    if not docs:
        print("No grouped points found. Run interconnections enrichment first, or pass --point-key explicitly.")
        return

    total_missing_keys = 0
    print("\nGrouped Points:")
    for doc in docs:
        related = doc.get("relatedOperationalPointKeys") or []
        existing = sorted(opd_point_keys(doc.get("pointDirections") or []))
        queried = sorted(set(doc.get("pointDirectionsQueriedKeys") or []))
        missing = missing_related_keys(doc)
        total_missing_keys += len(missing)
        print(
            f"  {doc.get('pointKey')}: {doc.get('pointLabel')} | "
            f"related={related} | opdKeys={existing} | queried={queried} | missing={missing}"
        )

    print(f"\nMissing related OPD point-key lookups: {total_missing_keys}")


def fetch_missing_for_docs(docs: List[Dict[str, Any]], limit: int) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    results: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    lookups: List[tuple[str, str]] = []
    for doc in docs:
        catalog_key = doc.get("pointKey")
        for related_key in missing_related_keys(doc):
            if catalog_key and related_key:
                lookups.append((catalog_key, related_key))

    for idx, (catalog_key, related_key) in enumerate(lookups, start=1):
        print(
            f"Fetching missing OPD for catalog {catalog_key}, related {related_key} "
            f"({idx}/{len(lookups)})...",
            flush=True,
        )
        rows = fetch_point_directions(related_key, limit=limit)
        results.setdefault(catalog_key, {})[related_key] = rows
        print(f"  fetched {len(rows)} rows", flush=True)
        if idx < len(lookups):
            time.sleep(MIN_REQUEST_INTERVAL_SEC)

    return results


def write_expanded_opd(
    collection_name: str,
    docs: List[Dict[str, Any]],
    fetched: Dict[str, Dict[str, List[Dict[str, Any]]]],
    fetched_at: dt.datetime,
) -> Dict[str, int]:
    operations: List[UpdateOne] = []
    for doc in docs:
        catalog_key = doc.get("pointKey")
        fetched_for_doc = fetched.get(catalog_key or "", {})
        if not fetched_for_doc:
            continue

        new_rows: List[Dict[str, Any]] = []
        for rows in fetched_for_doc.values():
            new_rows.extend(rows)

        merged = merge_point_directions(doc.get("pointDirections") or [], new_rows)
        queried_keys = sorted(set(doc.get("pointDirectionsQueriedKeys") or []) | set(fetched_for_doc.keys()))

        operations.append(
            UpdateOne(
                {"pointKey": catalog_key},
                {
                    "$set": {
                        "pointDirections": merged,
                        "pointDirectionsQueriedKeys": queried_keys,
                        "relatedPointDirectionsExpandedAt": fetched_at,
                    }
                },
            )
        )

    if not operations:
        return {"matched": 0, "modified": 0}

    db = get_database()
    collection = db[collection_name]
    result = collection.bulk_write(operations, ordered=False)
    return {"matched": result.matched_count, "modified": result.modified_count}


def main() -> None:
    args = parse_args()
    dry_run = not args.write

    if args.limit < 1:
        raise SystemExit("--limit must be >= 1")

    docs = load_docs(
        collection_name=args.collection,
        point_keys=args.point_keys,
        max_points=args.max_points,
    )
    summarize(docs)

    if not docs:
        return

    if dry_run:
        print("\nDry run only. No missing OPD rows fetched or written.")
        return

    fetched_at = dt.datetime.now(dt.timezone.utc)
    fetched = fetch_missing_for_docs(docs, limit=args.limit)

    total_rows = sum(len(rows) for by_key in fetched.values() for rows in by_key.values())
    print(f"\nFetched missing OPD rows: {total_rows}")
    stats = write_expanded_opd(
        collection_name=args.collection,
        docs=docs,
        fetched=fetched,
        fetched_at=fetched_at,
    )
    print(f"Mongo update complete (matched={stats['matched']}, modified={stats['modified']}).")


if __name__ == "__main__":
    main()
