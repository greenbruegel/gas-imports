"""Derive the Norway v4 volume converter from Gassco BCM/TWh figures."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, Iterable, List

import pandas as pd

DEFAULT_OUTPUT_JSON = Path("modern_pipeline/snapshots/gassco_norway_converter_derivation.json")
DEFAULT_OUTPUT_CSV = Path("modern_pipeline/snapshots/gassco_norway_converter_derivation.csv")

GASSCO_KEY_FIGURES_URL = "https://gassco.eu/en/about-us/what-we-do/key-figures/"
GASSCO_2023_NEWS_URL = "https://gassco.eu/en/new-delivery-records-for-norwegian-natural-gas/"
GASSCO_2024_NEWS_URL = "https://gassco.eu/en/record-delivery-of-natural-gas-through-the-gas-transport-system-to-europe-in-2024/"

ANNUAL_TOTALS = [
    {
        "recordType": "annual_total",
        "year": 2022,
        "label": "Total deliveries to receiving terminals in Europe",
        "bcm": 116.9,
        "twh": 1294.0,
        "sourceUrl": GASSCO_KEY_FIGURES_URL,
    },
    {
        "recordType": "annual_total",
        "year": 2023,
        "label": "Total deliveries to receiving terminals in Europe",
        "bcm": 109.1,
        "twh": 1207.0,
        "sourceUrl": GASSCO_KEY_FIGURES_URL,
    },
    {
        "recordType": "annual_total",
        "year": 2024,
        "label": "Total deliveries to receiving terminals in Europe",
        "bcm": 117.6,
        "twh": 1295.0,
        "sourceUrl": GASSCO_KEY_FIGURES_URL,
    },
    {
        "recordType": "annual_total",
        "year": 2025,
        "label": "Total deliveries to receiving terminals in Europe",
        "bcm": 114.9,
        "twh": 1271.0,
        "sourceUrl": GASSCO_KEY_FIGURES_URL,
    },
]

MARKET_BREAKDOWN = [
    {
        "recordType": "market_breakdown",
        "year": 2023,
        "label": "Germany/Denmark",
        "bcm": 56.2,
        "twh": 619.0,
        "sourceUrl": GASSCO_2023_NEWS_URL,
    },
    {
        "recordType": "market_breakdown",
        "year": 2023,
        "label": "Great Britain",
        "bcm": 24.1,
        "twh": 274.0,
        "sourceUrl": GASSCO_2023_NEWS_URL,
    },
    {
        "recordType": "market_breakdown",
        "year": 2023,
        "label": "France",
        "bcm": 13.8,
        "twh": 149.0,
        "sourceUrl": GASSCO_2023_NEWS_URL,
    },
    {
        "recordType": "market_breakdown",
        "year": 2023,
        "label": "Belgium",
        "bcm": 15.0,
        "twh": 164.5,
        "sourceUrl": GASSCO_2023_NEWS_URL,
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Derive the Norway legacy_excel_v4 converter from Gassco-reported BCM and TWh figures."
    )
    parser.add_argument(
        "--json",
        type=Path,
        default=DEFAULT_OUTPUT_JSON,
        help=f"Output JSON path. Default: {DEFAULT_OUTPUT_JSON}.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help=f"Output CSV path. Default: {DEFAULT_OUTPUT_CSV}.",
    )
    return parser.parse_args()


def add_ratios(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for record in records:
        row = dict(record)
        row["twhPerBcm"] = row["twh"] / row["bcm"]
        row["kwhPerM3"] = row["twhPerBcm"]
        row["kwhPerMcm"] = row["kwhPerM3"] * 1_000_000.0
        rows.append(row)
    return rows


def summarize(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    annual = [row for row in records if row["recordType"] == "annual_total"]
    market = [row for row in records if row["recordType"] == "market_breakdown"]
    annual_ratios = [row["kwhPerM3"] for row in annual]
    market_ratios = [row["kwhPerM3"] for row in market]
    weighted_annual = sum(row["twh"] for row in annual) / sum(row["bcm"] for row in annual)
    weighted_market = sum(row["twh"] for row in market) / sum(row["bcm"] for row in market)
    return {
        "basis": "Gassco reported BCM and TWh delivery figures; kWh/m3 = TWh / BCM.",
        "annualTotalYears": [row["year"] for row in annual],
        "annualMeanKwhPerM3": mean(annual_ratios),
        "annualMedianKwhPerM3": median(annual_ratios),
        "annualWeightedKwhPerM3": weighted_annual,
        "marketBreakdownMeanKwhPerM3": mean(market_ratios),
        "marketBreakdownWeightedKwhPerM3": weighted_market,
        "selectedLegacyExcelV4KwhPerM3": 11.0,
        "selectedLegacyExcelV4KwhPerMcm": 11_000_000.0,
        "selectionRationale": (
            "Gassco annual totals imply about 11.05 kWh/m3. legacy_excel_v4 uses "
            "a rounded compatibility value of 11.0 kWh/m3 for Norway."
        ),
        "sources": [
            {
                "name": "Gassco key figures",
                "url": GASSCO_KEY_FIGURES_URL,
                "notes": "Annual gas transport table reports deliveries in billion scm and TWh.",
            },
            {
                "name": "Gassco 2023 delivery records news",
                "url": GASSCO_2023_NEWS_URL,
                "notes": "Reports 2023 total and market breakdown figures in BCM with TWh in brackets.",
            },
            {
                "name": "Gassco 2024 record delivery news",
                "url": GASSCO_2024_NEWS_URL,
                "notes": "Reports 2024 total deliveries as 117.6 BCM and 1,295 TWh.",
            },
        ],
    }


def main() -> None:
    args = parse_args()
    records = add_ratios([*ANNUAL_TOTALS, *MARKET_BREAKDOWN])
    summary = summarize(records)
    payload = {"summary": summary, "records": records}

    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.csv.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    pd.DataFrame(records).to_csv(args.csv, index=False)

    print("Gassco Norway converter derivation")
    print(f"Annual weighted kWh/m3: {summary['annualWeightedKwhPerM3']:.4f}")
    print(f"Annual mean kWh/m3: {summary['annualMeanKwhPerM3']:.4f}")
    print(f"Market-breakdown weighted kWh/m3: {summary['marketBreakdownWeightedKwhPerM3']:.4f}")
    print(f"Selected v4 kWh/m3: {summary['selectedLegacyExcelV4KwhPerM3']:.1f}")
    print(f"JSON: {args.json}")
    print(f"CSV: {args.csv}")


if __name__ == "__main__":
    main()
