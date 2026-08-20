"""Build a versioned LNG terminal manifest from the GIE ALSI listing endpoint."""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
from typing import Any, Dict, Iterable, List, Optional

import requests
from pymongo import UpdateOne
from requests import JSONDecodeError, RequestException

from modern_pipeline.db import get_database

DEFAULT_LISTING_URL = "https://alsi.gie.eu/api/about?show=listing"
DEFAULT_VERSION = "gie_alsi_lng_terminals_v1"
DEFAULT_LNG_GCV_KWH_PER_M3 = 11.58
GCV_REFERENCE = {
    "name": "ENTSOG Gas Quality Outlook 2024",
    "url": "https://www.entsog.eu/sites/default/files/2026-03/entsog_GQO_2024_260327.pdf",
    "unit": "kWh/m3 (25/0 C)",
    "notes": (
        "LNG source-average GCV read from the report's import-source input-data chart; "
        "for GWh/MCM the numeric value is the same as kWh/m3."
    ),
}
LNG_V2_EXCLUDED_FACILITY_EICS = {
    "18W000000000GVMT": (
        "gie_alsi_lng_terminals_v2 excludes Spain TVB virtual balancing LNG tank "
        "to avoid double-counting individual Spanish terminal send-out"
    ),
}
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
MIN_REQUEST_INTERVAL_SEC = 1.1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a GIE ALSI LNG terminal manifest from the public listing endpoint."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and summarize the manifest without writing. This is the default.",
    )
    mode.add_argument(
        "--write",
        action="store_true",
        help="Upsert the manifest into MongoDB.",
    )
    parser.add_argument(
        "--version",
        default=DEFAULT_VERSION,
        help=f"Manifest version id. Default: {DEFAULT_VERSION}.",
    )
    parser.add_argument(
        "--collection",
        default="gie_lng_terminal_manifest",
        help="MongoDB manifest collection. Default: gie_lng_terminal_manifest.",
    )
    parser.add_argument(
        "--listing-url",
        default=DEFAULT_LISTING_URL,
        help=f"GIE ALSI listing endpoint. Default: {DEFAULT_LISTING_URL}",
    )
    parser.add_argument(
        "--country",
        action="append",
        dest="countries",
        help="Restrict to ISO2 country code. Can be passed multiple times. Default: include all listed facilities.",
    )
    return parser.parse_args()


def clean_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).strip()


def clean_date(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return text


def clean_country_iso2(value: Any) -> str:
    letters = "".join(char for char in clean_text(value).upper() if char.isalpha())
    return letters[:2]


def get_json_with_retries(url: str) -> Any:
    session = requests.Session()
    for attempt in range(1, MAX_RETRIES + 1):
        if attempt > 1:
            time.sleep(MIN_REQUEST_INTERVAL_SEC)
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
        except RequestException as exc:
            if attempt >= MAX_RETRIES:
                raise RuntimeError(f"GIE ALSI listing request failed after retries: {exc}") from exc
            print(
                f"GIE ALSI listing request failed: {exc}. Waiting {MIN_REQUEST_INTERVAL_SEC:.1f}s "
                f"(attempt {attempt}/{MAX_RETRIES}).",
                file=sys.stderr,
            )
            continue

        if response.status_code in RETRY_STATUS_CODES and attempt < MAX_RETRIES:
            retry_after = response.headers.get("Retry-After")
            wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else MIN_REQUEST_INTERVAL_SEC
            print(
                f"Retryable GIE ALSI listing response {response.status_code}; waiting {wait_seconds:.1f}s "
                f"(attempt {attempt}/{MAX_RETRIES}).",
                file=sys.stderr,
            )
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        try:
            return response.json()
        except JSONDecodeError as exc:
            preview = response.text.strip().replace("\n", " ")[:500]
            raise RuntimeError(f"GIE ALSI listing returned non-JSON response. Preview: {preview!r}") from exc

    raise RuntimeError("Failed to fetch GIE ALSI listing after retries.")


def api_params(country: str, operator_eic: str, facility_eic: str) -> Dict[str, str]:
    return {
        "country": country,
        "company": operator_eic,
        "facility": facility_eic,
    }


def facility_status(operational_start: Optional[str], operational_end: Optional[str]) -> str:
    today = dt.date.today()
    if operational_end:
        try:
            if dt.date.fromisoformat(operational_end[:10]) < today:
                return "ended"
        except ValueError:
            pass
    if operational_start:
        try:
            if dt.date.fromisoformat(operational_start[:10]) > today:
                return "future"
        except ValueError:
            pass
    return "active"


def iter_entries(payload: Any, version: str, countries: Optional[Iterable[str]]) -> List[Dict[str, Any]]:
    if not isinstance(payload, list):
        raise RuntimeError("Unexpected ALSI listing payload: expected a list of operators.")

    country_filter = {country.upper() for country in countries or []}
    entries: List[Dict[str, Any]] = []
    seen = set()
    ordinal = 0

    for operator in payload:
        if not isinstance(operator, dict):
            continue
        operator_country_raw = clean_text(operator.get("country")).upper()
        operator_country_iso2 = clean_country_iso2(operator_country_raw)
        operator_eic = clean_text(operator.get("eic"))
        facilities = operator.get("facilities") or []
        if not isinstance(facilities, list):
            continue

        for facility in facilities:
            if not isinstance(facility, dict):
                continue
            country_raw = clean_text(facility.get("country") or operator_country_raw).upper()
            country_iso2 = clean_country_iso2(country_raw)
            if country_filter and country_raw not in country_filter and country_iso2 not in country_filter:
                continue
            facility_eic = clean_text(facility.get("eic"))
            if not facility_eic:
                continue
            if facility_eic in seen:
                continue
            seen.add(facility_eic)
            ordinal += 1

            start_date = clean_date(facility.get("operational_start_date"))
            end_date = clean_date(facility.get("operational_end_date"))
            entries.append(
                {
                    "entryId": f"{version}:{ordinal:04d}",
                    "provider": "GIE_ALSI",
                    "sourceGroup": "LNG",
                    "figure1Group": "LNG",
                    "selected": True,
                    "sign": 1,
                    "sourceGcvKwhPerM3": DEFAULT_LNG_GCV_KWH_PER_M3,
                    "converterGwhPerMcm": DEFAULT_LNG_GCV_KWH_PER_M3,
                    "conversionSource": GCV_REFERENCE["name"],
                    "conversionReferenceUrl": GCV_REFERENCE["url"],
                    "facilityEic": facility_eic,
                    "facilityName": clean_text(facility.get("name")),
                    "facilityType": clean_text(facility.get("type")),
                    "countryIso2": country_iso2,
                    "countryCodeRaw": country_raw,
                    "operatorEic": operator_eic,
                    "operatorName": clean_text(operator.get("name")),
                    "operatorShortName": clean_text(operator.get("short_name")),
                    "operatorType": clean_text(operator.get("type")),
                    "operatorCountryIso2": operator_country_iso2,
                    "operatorCountryCodeRaw": operator_country_raw,
                    "operationalStartDate": start_date,
                    "operationalEndDate": end_date,
                    "facilityStatus": facility_status(start_date, end_date),
                    "apiUrl": clean_text(facility.get("url")),
                    "apiParams": api_params(country_raw, operator_eic, facility_eic),
                    "selectionSource": DEFAULT_LISTING_URL,
                    "notes": "",
                }
            )

    return sorted(entries, key=lambda item: (item["countryIso2"], item["facilityName"], item["facilityEic"]))


def apply_version_compatibility(entry: Dict[str, Any], version: str) -> Dict[str, Any]:
    entry = dict(entry)
    facility_eic = entry["facilityEic"]
    if version == "gie_alsi_lng_terminals_v2" and facility_eic in LNG_V2_EXCLUDED_FACILITY_EICS:
        reason = LNG_V2_EXCLUDED_FACILITY_EICS[facility_eic]
        entry["selected"] = False
        entry["compatibilityExclusionReason"] = reason
        entry["notes"] = reason
    return entry


def build_manifest(args: argparse.Namespace) -> Dict[str, Any]:
    payload = get_json_with_retries(args.listing_url)
    entries = [
        apply_version_compatibility(entry, args.version)
        for entry in iter_entries(payload, version=args.version, countries=args.countries)
    ]
    now = dt.datetime.now(dt.timezone.utc)
    return {
        "version": args.version,
        "kind": "gie_lng_terminal_manifest",
        "status": "draft",
        "description": (
            "Facility-level GIE ALSI LNG terminal manifest built from the live ALSI listing endpoint. "
            "All listed facilities are selected initially; exclusions should be versioned explicitly later."
        ),
        "source": {
            "type": "gie_alsi_listing",
            "url": args.listing_url,
            "fetchedAt": now,
        },
        "scope": {
            "sourceGroup": "LNG",
            "countries": sorted({entry["countryIso2"] for entry in entries}),
            "sourceGcvKwhPerM3": DEFAULT_LNG_GCV_KWH_PER_M3,
            "converterGwhPerMcm": DEFAULT_LNG_GCV_KWH_PER_M3,
            "conversionReference": GCV_REFERENCE,
            "notes": (
                "Initial broad manifest includes all ALSI-listed facilities."
                if args.version == "gie_alsi_lng_terminals_v1"
                else "Compatibility manifest excludes known duplicate/virtual rows."
            ),
        },
        "compatibilityRules": (
            {
                "basis": "legacy_country_data.xlsx Figure 1 LNG comparison",
                "excludedFacilityEics": LNG_V2_EXCLUDED_FACILITY_EICS,
            }
            if args.version == "gie_alsi_lng_terminals_v2"
            else {}
        ),
        "entries": entries,
        "entryCount": len(entries),
        "selectedEntryCount": sum(1 for entry in entries if entry.get("selected", True)),
        "createdAt": now,
        "updatedAt": now,
    }


def summarize_entries(entries: Iterable[Dict[str, Any]]) -> None:
    rows = list(entries)
    selected_rows = [row for row in rows if row.get("selected", True)]
    by_country: Dict[str, int] = {}
    by_status: Dict[str, int] = {}
    by_type: Dict[str, int] = {}
    for row in selected_rows:
        by_country[row["countryIso2"]] = by_country.get(row["countryIso2"], 0) + 1
        by_status[row["facilityStatus"]] = by_status.get(row["facilityStatus"], 0) + 1
        by_type[row["facilityType"]] = by_type.get(row["facilityType"], 0) + 1

    print(f"Manifest entries: {len(rows)}")
    print(f"Selected entries: {len(selected_rows)}")

    print("\nBy countryIso2:")
    for key, count in sorted(by_country.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nBy facilityStatus:")
    for key, count in sorted(by_status.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nBy facilityType:")
    for key, count in sorted(by_type.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print("\nSample:")
    for row in selected_rows[:20]:
        print(
            "  "
            f"{row['countryIso2']} | {row['facilityName']} | "
            f"{row['facilityEic']} | {row['operatorShortName'] or row['operatorName']}"
        )


def ensure_indexes(collection_name: str) -> None:
    db = get_database()
    collection = db[collection_name]
    collection.create_index("version", unique=True)
    collection.create_index([("status", 1), ("version", 1)])


def write_manifest(collection_name: str, manifest: Dict[str, Any]) -> Dict[str, int]:
    ensure_indexes(collection_name)
    db = get_database()
    collection = db[collection_name]
    manifest_update = dict(manifest)
    created_at = manifest_update.pop("createdAt")
    result = collection.bulk_write(
        [
            UpdateOne(
                {"version": manifest["version"]},
                {
                    "$set": manifest_update,
                    "$setOnInsert": {"createdAt": created_at},
                },
                upsert=True,
            )
        ]
    )
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "upserted": len(result.upserted_ids),
    }


def main() -> None:
    args = parse_args()
    manifest = build_manifest(args)
    summarize_entries(manifest["entries"])

    if not args.write:
        print("\nDry run only. No MongoDB writes performed.")
        return

    result = write_manifest(args.collection, manifest)
    print(
        "\nMongo upsert complete "
        f"(matched={result['matched']}, modified={result['modified']}, upserted={result['upserted']})."
    )


if __name__ == "__main__":
    main()
