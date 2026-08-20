"""Compare modern manifest candidates with the legacy Excel point selection."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import pandas as pd

from modern_pipeline.db import get_database
from modern_pipeline.scripts.audit_manifest_candidates import derive_candidates

DEFAULT_LEGACY_LOCATIONS = Path("working data/locationsSAFE.xlsx")
DEFAULT_OUTPUT = Path("modern_pipeline/snapshots/legacy_modern_manifest_review.csv")

OUT_OF_SCOPE_AGGREGATIONS = {"LNG", "UK"}
OUT_OF_SCOPE_COUNTRIES = {"CH", "AL", "BA", "ME", "MK", "RS"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare modern ENTSOG manifest candidates with legacy locationsSAFE.xlsx."
    )
    parser.add_argument(
        "--legacy-locations",
        type=Path,
        default=DEFAULT_LEGACY_LOCATIONS,
        help=f"Path to legacy locationsSAFE.xlsx. Default: {DEFAULT_LEGACY_LOCATIONS}",
    )
    parser.add_argument(
        "--collection",
        default="entsog_connection_points",
        help="MongoDB connection-points collection. Default: entsog_connection_points.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output review CSV. Default: {DEFAULT_OUTPUT}",
    )
    return parser.parse_args()


def point_direction_id(operator_key: str, point_key: str, direction_key: str) -> str:
    return f"{operator_key}{point_key}{direction_key}".strip().lower()


def split_semicolon(value: Any) -> List[str]:
    if value in (None, ""):
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    if isinstance(value, list):
        return [str(part).strip() for part in value if str(part).strip()]
    return [part.strip() for part in str(value).split(";") if part.strip()]


def value_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if isinstance(value, list):
        return ";".join(str(item) for item in value if item not in (None, ""))
    return str(value)


def load_legacy(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path).fillna("")
    for col in ("operator", "location", "direction", "aggregation", "aggregation2", "aggregation3"):
        df[col] = df[col].astype(str).str.strip()

    df = df[df["location"].str.startswith("ITP-")].copy()
    df = df[~df["aggregation"].isin(OUT_OF_SCOPE_AGGREGATIONS)].copy()
    df["legacyPointDirection"] = (
        df["operator"] + df["location"] + df["direction"]
    ).str.lower()
    return df


def load_modern_candidates(collection_name: str) -> pd.DataFrame:
    db = get_database()
    docs = list(db[collection_name].find({"catalogStatus": "candidate"}).sort("pointKey", 1))
    rows = [derive_candidates(doc) for doc in docs]
    return pd.DataFrame(rows).fillna("")


def build_legacy_rows(legacy: pd.DataFrame, modern_by_pd: Dict[str, Dict[str, Any]], modern_by_point: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for _, row in legacy.iterrows():
        pdid = row["legacyPointDirection"]
        point_key = row["location"]
        exact = modern_by_pd.get(pdid)
        same_point = modern_by_point.get(point_key, [])

        if exact:
            category = "matched_exact"
            modern_selected = pdid
            modern_flags = exact.get("flagsText", "")
            modern_source = exact.get("sourceCountryKeysText", "")
            modern_import = exact.get("importCountryKeysText", "")
        elif same_point:
            category = "legacy_same_point_different_series"
            modern_selected = ";".join(
                sorted({item["selectedPointDirection"] for item in same_point if item.get("selectedPointDirection")})
            )
            modern_flags = ";".join(sorted({item.get("flagsText", "") for item in same_point if item.get("flagsText", "")}))
            modern_source = ";".join(sorted({item.get("sourceCountryKeysText", "") for item in same_point if item.get("sourceCountryKeysText", "")}))
            modern_import = ";".join(sorted({item.get("importCountryKeysText", "") for item in same_point if item.get("importCountryKeysText", "")}))
        else:
            category = "legacy_not_in_modern"
            modern_selected = ""
            modern_flags = ""
            modern_source = ""
            modern_import = ""

        review_hint = ""
        if category == "legacy_same_point_different_series":
            review_hint = "same physical point; compare EU-side entry vs legacy side"
        elif category == "legacy_not_in_modern":
            review_hint = "historical/deleted/currently absent or out of EUNONEU catalog"

        rows.append(
            {
                "source": "legacy",
                "reviewCategory": category,
                "pointKey": point_key,
                "pointLabel": row.get("label", ""),
                "legacyPointDirection": pdid,
                "modernSelectedPointDirection": modern_selected,
                "legacyDirection": row.get("direction", ""),
                "legacyExportCountry": row.get("exportcountry", ""),
                "legacyImportCountry": row.get("importcountry", ""),
                "legacyAggregation": row.get("aggregation", ""),
                "legacyRouteGroup": row.get("aggregation2", ""),
                "legacyAggregation3": row.get("aggregation3", ""),
                "modernSourceCountryKeys": modern_source,
                "modernImportCountryKeys": modern_import,
                "modernFlags": modern_flags,
                "reviewHint": review_hint,
                "reviewDecision": "",
                "notes": "",
            }
        )
    return rows


def modern_selected_rows(modern: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for _, row in modern.iterrows():
        selected = split_semicolon(row.get("recommendedSelectedPointDirections", ""))
        for pdid in selected:
            rows.append(
                {
                    "selectedPointDirection": pdid,
                    "pointKey": row.get("catalogPointKey", ""),
                    "pointLabel": row.get("pointLabel", ""),
                    "sourceCountryKeysText": value_text(row.get("sourceCountryKeys", "")),
                    "importCountryKeysText": value_text(row.get("importCountryKeys", "")),
                    "flagsText": value_text(row.get("flags", "")),
                    "relatedOperationalPointKeys": value_text(row.get("relatedOperationalPointKeys", "")),
                    "availablePointDirections": value_text(row.get("availablePointDirections", "")),
                }
            )
    return rows


def is_out_of_scope_modern(row: Dict[str, Any]) -> bool:
    source_keys = set(split_semicolon(row.get("sourceCountryKeysText", "")))
    import_keys = set(split_semicolon(row.get("importCountryKeysText", "")))
    if source_keys & OUT_OF_SCOPE_COUNTRIES:
        return True
    if source_keys and import_keys and (source_keys | import_keys) <= OUT_OF_SCOPE_COUNTRIES:
        return True
    return False


def build_modern_only_rows(modern_rows: List[Dict[str, Any]], legacy_pds: Set[str], legacy_point_keys: Set[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in modern_rows:
        pdid = row["selectedPointDirection"]
        if pdid in legacy_pds:
            continue

        point_key = row["pointKey"]
        if point_key in legacy_point_keys:
            category = "modern_same_point_different_series"
            review_hint = "same physical point as legacy; likely entry/exit/operator choice difference"
        elif is_out_of_scope_modern(row):
            category = "modern_out_of_scope_candidate"
            review_hint = "default exclude for now: Switzerland or intra-Balkans/non-core border"
        else:
            category = "modern_new_candidate"
            review_hint = "new current EUNONEU candidate; review include/exclude"

        rows.append(
            {
                "source": "modern",
                "reviewCategory": category,
                "pointKey": point_key,
                "pointLabel": row.get("pointLabel", ""),
                "legacyPointDirection": "",
                "modernSelectedPointDirection": pdid,
                "legacyDirection": "",
                "legacyExportCountry": "",
                "legacyImportCountry": "",
                "legacyAggregation": "",
                "legacyRouteGroup": "",
                "legacyAggregation3": "",
                "modernSourceCountryKeys": row.get("sourceCountryKeysText", ""),
                "modernImportCountryKeys": row.get("importCountryKeysText", ""),
                "modernFlags": row.get("flagsText", ""),
                "reviewHint": review_hint,
                "reviewDecision": "exclude_candidate" if category == "modern_out_of_scope_candidate" else "",
                "notes": "",
            }
        )
    return rows


def summarize(rows: List[Dict[str, Any]]) -> None:
    counts: Dict[str, int] = {}
    for row in rows:
        category = row["reviewCategory"]
        counts[category] = counts.get(category, 0) + 1

    print(f"Review rows: {len(rows)}")
    print("\nReview category counts:")
    for category, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {category}: {count}")

    print("\nSample rows needing review:")
    shown = 0
    for row in rows:
        if row["reviewCategory"] == "matched_exact":
            continue
        print(
            "  "
            f"{row['reviewCategory']} | {row['pointKey']} {row['pointLabel']} | "
            f"legacy={row['legacyPointDirection']} modern={row['modernSelectedPointDirection']} | "
            f"hint={row['reviewHint']}"
        )
        shown += 1
        if shown >= 20:
            break


def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "source",
        "reviewCategory",
        "pointKey",
        "pointLabel",
        "legacyPointDirection",
        "modernSelectedPointDirection",
        "legacyDirection",
        "legacyExportCountry",
        "legacyImportCountry",
        "legacyAggregation",
        "legacyRouteGroup",
        "legacyAggregation3",
        "modernSourceCountryKeys",
        "modernImportCountryKeys",
        "modernFlags",
        "reviewHint",
        "reviewDecision",
        "notes",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nWrote CSV: {path}")


def main() -> None:
    args = parse_args()
    legacy = load_legacy(args.legacy_locations)
    modern = load_modern_candidates(args.collection)
    modern_rows = modern_selected_rows(modern)

    modern_by_pd = {row["selectedPointDirection"]: row for row in modern_rows}
    modern_by_point: Dict[str, List[Dict[str, Any]]] = {}
    for row in modern_rows:
        modern_by_point.setdefault(row["pointKey"], []).append(row)

    legacy_pds = set(legacy["legacyPointDirection"])
    legacy_point_keys = set(legacy["location"])

    rows = build_legacy_rows(legacy, modern_by_pd=modern_by_pd, modern_by_point=modern_by_point)
    rows.extend(build_modern_only_rows(modern_rows, legacy_pds=legacy_pds, legacy_point_keys=legacy_point_keys))
    rows = sorted(rows, key=lambda row: (row["pointKey"], row["source"], row["legacyPointDirection"], row["modernSelectedPointDirection"]))

    summarize(rows)
    write_csv(rows, args.csv)


if __name__ == "__main__":
    main()
