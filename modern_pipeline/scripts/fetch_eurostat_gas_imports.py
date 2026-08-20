"""Fetch a compact Eurostat annual gas-import comparison snapshot."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import requests

EUROSTAT_DATA_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_ti_gas"
DEFAULT_OUTPUT = Path("modern_pipeline/snapshots/eurostat_nrg_ti_gas_eu27_annual.json")
DEFAULT_REPORTER = "EU27_2020"
DEFAULT_UNIT = "MIO_M3"
DEFAULT_START_YEAR = 2016
PIPELINE_SIEC = "G3000"
LNG_SIEC = "G3200"
PIPELINE_PARTNERS = {
    "NO": "Norway",
    "RU": "Russia",
    "UK": "UK",
    "AZ": "Azerbaijan",
    "DZ": "Algeria",
}
LNG_PARTNER = "TOTAL"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch annual Eurostat nrg_ti_gas values for EU27 gas import comparison."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"JSON output path. Default: {DEFAULT_OUTPUT}.",
    )
    parser.add_argument(
        "--reporter",
        default=DEFAULT_REPORTER,
        help=f"Eurostat geo/reporter code. Default: {DEFAULT_REPORTER}.",
    )
    parser.add_argument(
        "--unit",
        default=DEFAULT_UNIT,
        help=f"Eurostat unit code. Default: {DEFAULT_UNIT}.",
    )
    parser.add_argument(
        "--from-year",
        type=int,
        default=DEFAULT_START_YEAR,
        help=f"First annual time period to fetch. Default: {DEFAULT_START_YEAR}.",
    )
    parser.add_argument(
        "--to-year",
        type=int,
        default=None,
        help="Optional final annual time period to fetch.",
    )
    parser.add_argument(
        "--partner",
        action="append",
        dest="partners",
        help=(
            "Pipeline partner code to fetch with siec=G3000. Can be passed multiple times. "
            f"Defaults: {', '.join(PIPELINE_PARTNERS)}."
        ),
    )
    parser.add_argument(
        "--lng-partner",
        default=LNG_PARTNER,
        help=f"Partner code to use for LNG total with siec=G3200. Default: {LNG_PARTNER}.",
    )
    return parser.parse_args()


def eurostat_params(args: argparse.Namespace) -> List[tuple[str, Any]]:
    partners = list(args.partners or PIPELINE_PARTNERS.keys())
    all_partners = sorted(set(partners + [args.lng_partner]))
    params: List[tuple[str, Any]] = [
        ("format", "JSON"),
        ("lang", "EN"),
        ("freq", "A"),
        ("unit", args.unit),
        ("geo", args.reporter),
        ("sinceTimePeriod", str(args.from_year)),
        ("siec", PIPELINE_SIEC),
        ("siec", LNG_SIEC),
    ]
    if args.to_year is not None:
        params.append(("untilTimePeriod", str(args.to_year)))
    for partner in all_partners:
        params.append(("partner", partner))
    return params


def fetch_json(params: Sequence[tuple[str, Any]]) -> Dict[str, Any]:
    response = requests.get(EUROSTAT_DATA_URL, params=params, timeout=90)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected non-dict Eurostat response.")
    return payload


def category_order(payload: Dict[str, Any], dimension: str) -> List[str]:
    dim = payload["dimension"][dimension]
    index = dim["category"]["index"]
    return [code for code, _ in sorted(index.items(), key=lambda item: item[1])]


def category_labels(payload: Dict[str, Any], dimension: str) -> Dict[str, str]:
    return payload["dimension"][dimension].get("category", {}).get("label", {})


def iter_cells(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    ids = payload["id"]
    sizes = payload["size"]
    orders = {dim: category_order(payload, dim) for dim in ids}
    values = payload.get("value", {})
    statuses = payload.get("status", {})
    total_size = 1
    for size in sizes:
        total_size *= size

    for flat_index in range(total_size):
        value = values.get(str(flat_index))
        status = statuses.get(str(flat_index))
        remainder = flat_index
        coords: Dict[str, str] = {}
        for dim, stride_size in reversed(list(zip(ids, sizes))):
            position = remainder % stride_size
            remainder //= stride_size
            coords[dim] = orders[dim][position]
        yield {**coords, "value": value, "status": status}


def source_key_for_cell(cell: Dict[str, Any], pipeline_partners: Dict[str, str], lng_partner: str) -> Optional[str]:
    siec = cell.get("siec")
    partner = cell.get("partner")
    if siec == PIPELINE_SIEC and partner in pipeline_partners:
        return pipeline_partners[partner]
    if siec == LNG_SIEC and partner == lng_partner:
        return "LNG"
    return None


def build_snapshot(args: argparse.Namespace, payload: Dict[str, Any], request_url: str) -> Dict[str, Any]:
    partners = list(args.partners or PIPELINE_PARTNERS.keys())
    pipeline_partner_map = {code: PIPELINE_PARTNERS.get(code, code) for code in partners}
    partner_labels = category_labels(payload, "partner")
    siec_labels = category_labels(payload, "siec")
    unit_labels = category_labels(payload, "unit")
    geo_labels = category_labels(payload, "geo")

    records = []
    for cell in iter_cells(payload):
        series_key = source_key_for_cell(cell, pipeline_partner_map, args.lng_partner)
        if not series_key:
            continue
        value = cell.get("value")
        if value is None:
            continue
        records.append(
            {
                "year": int(cell["time"]),
                "seriesKey": series_key,
                "valueMcm": float(value),
                "unit": cell["unit"],
                "siec": cell["siec"],
                "siecLabel": siec_labels.get(cell["siec"], cell["siec"]),
                "partner": cell["partner"],
                "partnerLabel": partner_labels.get(cell["partner"], cell["partner"]),
                "reporter": cell["geo"],
                "reporterLabel": geo_labels.get(cell["geo"], cell["geo"]),
                "status": cell.get("status"),
            }
        )

    records = sorted(records, key=lambda row: (row["year"], row["seriesKey"]))
    return {
        "dataset": "nrg_ti_gas",
        "doi": "https://doi.org/10.2908/NRG_TI_GAS",
        "sourceUrl": request_url,
        "provider": "Eurostat",
        "fetchedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
        "updated": payload.get("updated"),
        "label": payload.get("label"),
        "filters": {
            "freq": "A",
            "unit": args.unit,
            "unitLabel": unit_labels.get(args.unit, args.unit),
            "reporter": args.reporter,
            "reporterLabel": geo_labels.get(args.reporter, args.reporter),
            "fromYear": args.from_year,
            "toYear": args.to_year,
            "pipelineSiec": PIPELINE_SIEC,
            "pipelineSiecLabel": siec_labels.get(PIPELINE_SIEC, PIPELINE_SIEC),
            "pipelinePartners": pipeline_partner_map,
            "lngSiec": LNG_SIEC,
            "lngSiecLabel": siec_labels.get(LNG_SIEC, LNG_SIEC),
            "lngPartner": args.lng_partner,
            "lngPartnerLabel": partner_labels.get(args.lng_partner, args.lng_partner),
        },
        "records": records,
    }


def summarize(snapshot: Dict[str, Any]) -> None:
    records = snapshot["records"]
    years = sorted({row["year"] for row in records})
    series = sorted({row["seriesKey"] for row in records})
    print(f"Records: {len(records)}")
    if years:
        print(f"Years: {min(years)} to {max(years)}")
    print(f"Series: {', '.join(series)}")
    latest_year = max(years) if years else None
    if latest_year:
        print(f"\nLatest year values ({latest_year}, MIO_M3):")
        for row in sorted((r for r in records if r["year"] == latest_year), key=lambda item: item["seriesKey"]):
            print(f"  {row['seriesKey']}: {row['valueMcm']:.1f}")


def main() -> None:
    args = parse_args()
    params = eurostat_params(args)
    payload = fetch_json(params)
    prepared = requests.Request("GET", EUROSTAT_DATA_URL, params=params).prepare()
    snapshot = build_snapshot(args, payload, prepared.url or EUROSTAT_DATA_URL)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    summarize(snapshot)
    print(f"\nJSON written: {args.output}")


if __name__ == "__main__":
    main()
