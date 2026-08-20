# Modern Gas Imports Pipeline

Initial replacement pipeline for European gas import data.

For the detailed differences between `legacy_excel_v1`, `legacy_excel_v2`, and
`legacy_excel_v3`, see [manifest_versions.md](manifest_versions.md).
For the source-specific gas-quality conversion values, see
[gas_quality_adjustment.md](gas_quality_adjustment.md).

## Environment

The MongoDB backend is configured with:

```bash
MONGO_URI="mongodb+srv://..."
GAS_IMPORTS_DB="gas_imports"
```

`python-dotenv` is supported, so these can live in a local ignored `.env` while developing.

## Build Initial ENTSOG Connection Points

Dry-run candidate EUNONEU connection points:

```bash
python3 -m modern_pipeline.scripts.build_connection_points --dry-run
```

Write/upsert to MongoDB:

```bash
python3 -m modern_pipeline.scripts.build_connection_points --write
```

Useful development options:

```bash
python3 -m modern_pipeline.scripts.build_connection_points --dry-run --limit 25
python3 -m modern_pipeline.scripts.build_connection_points --dry-run --page-size 25 --max-pages 2
python3 -m modern_pipeline.scripts.build_connection_points --dry-run --eu-crossing EUNONEU
```

`--limit` fetches one page only. Omit it to fetch all pages politely with `--page-size`.

## Enrich Operator Point Directions

After loading `entsog_connection_points`, attach ENTSOG's queryable operator/point/direction series:

```bash
python3 -m modern_pipeline.scripts.enrich_operator_point_directions --dry-run --max-points 3
```

Write the enrichment back into each connection-point document:

```bash
python3 -m modern_pipeline.scripts.enrich_operator_point_directions --write
```

Useful targeted checks:

```bash
python3 -m modern_pipeline.scripts.enrich_operator_point_directions --dry-run --point-key ITP-00048
python3 -m modern_pipeline.scripts.enrich_operator_point_directions --dry-run --point-key ITP-00045 --point-key ITP-00297
```

This command fetches one `operatorPointDirections?hasData=1&pointKey=...` request per point and pauses between requests.

## Enrich Interconnections

Attach ENTSOG from/to topology metadata for each candidate point:

```bash
python3 -m modern_pipeline.scripts.enrich_interconnections --dry-run --max-points 3
```

Useful targeted checks:

```bash
python3 -m modern_pipeline.scripts.enrich_interconnections --dry-run --point-key ITP-00048
python3 -m modern_pipeline.scripts.enrich_interconnections --dry-run --point-key ITP-00045 --point-key ITP-00209
```

Write the enrichment back into each connection-point document:

```bash
python3 -m modern_pipeline.scripts.enrich_interconnections --write
```

This command stores `interconnections` plus `relatedOperationalPointKeys`, which helps identify map/catalog points that resolve to different operational point keys.

## Audit Grouped Points

After interconnections are enriched, inspect map/catalog points that fan out to multiple operational point keys:

```bash
python3 -m modern_pipeline.scripts.audit_grouped_points --dry-run
```

Targeted check:

```bash
python3 -m modern_pipeline.scripts.audit_grouped_points --dry-run --point-key ITP-00209
```

Fetch missing `operatorPointDirections` rows for related operational point keys and merge them into `pointDirections`:

```bash
python3 -m modern_pipeline.scripts.audit_grouped_points --write
```

## Audit Draft Manifest Candidates

After OPD and interconnections enrichment, generate a read-only rule-based manifest audit:

```bash
python3 -m modern_pipeline.scripts.audit_manifest_candidates
```

Write a CSV for comparison with the legacy Excel files:

```bash
python3 -m modern_pipeline.scripts.audit_manifest_candidates --csv modern_pipeline/snapshots/manifest_candidates.csv
```

Useful targeted checks:

```bash
python3 -m modern_pipeline.scripts.audit_manifest_candidates --point-key ITP-00048
python3 -m modern_pipeline.scripts.audit_manifest_candidates --max-points 20
```

## Build Legacy Excel Manifest

Convert the Figure 1 legacy Excel selection into a versioned Mongo manifest:

```bash
python3 -m modern_pipeline.scripts.build_legacy_excel_manifest --dry-run
python3 -m modern_pipeline.scripts.build_legacy_excel_manifest --write
```

By default this creates `legacy_excel_v1` from `locationsSAFE.xlsx`, excludes LNG,
excludes imports into the UK, and keeps the Figure 1 source buckets: Russia,
Norway, UK, Azerbaijan, and Algeria.

For a closer reproduction of the downloaded Figure 1 workbook, build
`legacy_excel_v2`. This keeps the v1 rows but marks known duplicate/legacy
pointDirections as unselected for Azerbaijan, Norway, and Russia:

```bash
python3 -m modern_pipeline.scripts.build_legacy_excel_manifest \
  --version legacy_excel_v2 \
  --write
```

For the current clean pipeline manifest, build `legacy_excel_v3`. This keeps
the v2 compatibility choices, adds source-specific GCV conversion factors from
ENTSOG's Gas Quality Outlook 2024, and adds a time-bounded Norway archive alias:

```bash
python3 -m modern_pipeline.scripts.build_legacy_excel_manifest \
  --version legacy_excel_v3 \
  --write
```

For the current Figure 1 compatibility candidate, build `legacy_excel_v4`. This
keeps the v3 point selection and UK net-flow logic, but uses the legacy `10.3`
conversion for non-Norway pipeline sources and `11.0` for Norway, matching the
Gassco official BCM/TWh convention much more closely:

```bash
python3 -m modern_pipeline.scripts.derive_gassco_norway_converter

python3 -m modern_pipeline.scripts.build_legacy_excel_manifest \
  --version legacy_excel_v4 \
  --write
```

The derivation command writes the Gassco source rows and implied kWh/m3 ratios
to `modern_pipeline/snapshots/gassco_norway_converter_derivation.json` and
`.csv`.

## Compare With Legacy Selection

Compare the modern manifest candidates with legacy `locationsSAFE.xlsx`, excluding LNG and UK:

```bash
python3 -m modern_pipeline.scripts.compare_legacy_manifest_candidates \
  --csv modern_pipeline/snapshots/legacy_modern_manifest_review.csv
```

The output CSV is a review surface for pointDirection decisions, not the final manifest.

## Fetch Raw ENTSOG Observations

Build a broad pointDirection universe from modern candidates plus legacy ITP selections, then fetch physical-flow daily observations.

Plan only:

```bash
python3 -m modern_pipeline.scripts.fetch_entsog_raw_observations --mode daily --dry-run
```

Small smoke write:

```bash
python3 -m modern_pipeline.scripts.fetch_entsog_raw_observations \
  --mode daily \
  --days 7 \
  --write \
  --max-batches 2
```

Daily server-style rolling update:

```bash
python3 -m modern_pipeline.scripts.fetch_entsog_raw_observations --mode daily --days 7 --write
```

Backfill within the live ENTSOG API window:

```bash
python3 -m modern_pipeline.scripts.fetch_entsog_raw_observations \
  --mode backfill \
  --from 2022-01-01 \
  --to 2026-08-15 \
  --write
```

The fetcher batches pointDirections and monthly date chunks, and pauses between ENTSOG requests.

## Fetch Raw ENTSOG Archive Observations

The live ENTSOG API currently serves data from the archive cutoff date onward.
Older Physical Flow data is available as annual all-TSO CSV archive files. The
archive importer streams those CSVs directly from ENTSOG and does not save local
copies.

Check archive files and pointDirection mapping coverage without streaming CSVs:

```bash
python3 -m modern_pipeline.scripts.fetch_entsog_archive_observations \
  --year 2020 \
  --plan-only
```

Small stream-only smoke test:

```bash
python3 -m modern_pipeline.scripts.fetch_entsog_archive_observations \
  --year 2020 \
  --limit-rows 10000 \
  --dry-run
```

Write one archive year:

```bash
python3 -m modern_pipeline.scripts.fetch_entsog_archive_observations \
  --year 2020 \
  --write
```

Write the full 2016-2020 archive range:

```bash
python3 -m modern_pipeline.scripts.fetch_entsog_archive_observations \
  --year 2016 \
  --year 2017 \
  --year 2018 \
  --year 2019 \
  --year 2020 \
  --write
```

## Materialize Daily Plotting Series

Build `gas_import_daily` from `entsog_raw_observations` and a chosen manifest:

```bash
python3 -m modern_pipeline.scripts.materialize_gas_import_daily \
  --manifest-version legacy_excel_v2 \
  --dry-run
```

Write the daily Figure 1-compatible source groups plus total:

```bash
python3 -m modern_pipeline.scripts.materialize_gas_import_daily \
  --manifest-version legacy_excel_v4 \
  --delete-existing-window \
  --write
```

The output stores daily kWh, GWh, TWh, and million cubic metres. For
`legacy_excel_v1` and `legacy_excel_v2`, `valueMcm` uses the legacy-compatible
default conversion of `10,300,000` kWh per million cubic metres. For
`legacy_excel_v3`, `valueMcm` uses the source-specific conversion factors stored
on the manifest entries. For `legacy_excel_v4`, Norway uses `11,000,000`
kWh/MCM and other pipeline source groups use the legacy `10,300,000` kWh/MCM.
Weekly chart values can be computed from this daily collection.

`legacy_excel_v3` and `legacy_excel_v4` also store UK counterflows so the
collection can support both Eurostat-style gross imports and the legacy Figure 1
net-flow panel. Use:

| seriesKey | flowConcept | Use |
| --- | --- | --- |
| `UK` | `gross_import` | UK-to-EU imports and Eurostat comparisons. |
| `UK` | `net_import` | Figure 1 UK panel. |
| `UK` | `counterflow_export` with `aggregationLevel=source_group_counterflow` | EU-to-UK counterflow component. |

Other Figure 1 source groups use `flowConcept=gross_import`.

## Weekly Server Update

For a safe unattended weekly update, use the wrapper script:

```bash
python3 -m modern_pipeline.scripts.run_weekly_update --write
```

The wrapper:

- fetches recent ENTSOG and GIE raw rows with upserts;
- uses a default 3-day source-data lag;
- rebuilds the last 5 complete ISO weeks in `gas_import_daily`;
- materializes only through the latest complete Sunday;
- purges any already-materialized partial-week rows after that Sunday.

This avoids a frontend naively plotting a latest week that only has one or two
days of data.

Plan without touching APIs or MongoDB:

```bash
python3 -m modern_pipeline.scripts.run_weekly_update --plan-only
```

Dry-run the child jobs:

```bash
python3 -m modern_pipeline.scripts.run_weekly_update --dry-run
```

Recommended cron timing is Thursday morning. With the default 3-day lag, a
Thursday run can fetch through Monday and materialize through the immediately
preceding Sunday, so the latest published week is complete.

Example Linux cron:

```cron
0 6 * * 4 cd /path/to/gas-imports && /usr/bin/env bash -lc 'python3 -m modern_pipeline.scripts.run_weekly_update --write >> logs/weekly_update.log 2>&1'
```

## Build GIE ALSI LNG Terminal Manifest

Build the first broad LNG terminal manifest from ALSI's live listing endpoint:

```bash
python3 -m modern_pipeline.scripts.build_gie_lng_terminal_manifest --dry-run
```

Write/upsert the manifest to MongoDB:

```bash
python3 -m modern_pipeline.scripts.build_gie_lng_terminal_manifest --write
```

The initial version is `gie_alsi_lng_terminals_v1` and deliberately selects all
listed ALSI facilities. Later versions should mark virtual, historical, or
otherwise superfluous rows as unselected rather than deleting them.

## Fetch Raw GIE ALSI LNG Observations

The fetcher reads the LNG terminal manifest and writes daily terminal rows to
`gie_lng_raw_observations`. It expects one of these environment variables:
`ALSI_API_KEY`, `GIE_API_KEY`, or `AGSI_API_KEY`.

Plan only:

```bash
python3 -m modern_pipeline.scripts.fetch_gie_lng_raw_observations --mode daily --dry-run
```

Small smoke write:

```bash
python3 -m modern_pipeline.scripts.fetch_gie_lng_raw_observations \
  --mode daily \
  --days 7 \
  --max-facilities 3 \
  --write
```

Backfill:

```bash
python3 -m modern_pipeline.scripts.fetch_gie_lng_raw_observations \
  --mode backfill \
  --from 2016-01-01 \
  --to 2026-08-15 \
  --write
```

The raw rows preserve ALSI's source payload and normalize the fields used by the
legacy LNG scripts: `sendOut`, `inventory`, `dtmi`, and `dtrs`.

## Materialize Daily LNG Plotting Series

Build daily LNG rows in `gas_import_daily` from `gie_lng_raw_observations` and
the ALSI terminal manifest:

```bash
python3 -m modern_pipeline.scripts.materialize_gie_lng_daily --dry-run
```

Write the daily LNG total plus country-level LNG series:

```bash
python3 -m modern_pipeline.scripts.materialize_gie_lng_daily \
  --delete-existing-window \
  --write
```

The initial `gie_alsi_lng_terminals_v1` version includes all listed ALSI
facilities, so comparison with legacy LNG data should be used to decide the v2
exclusions for virtual, historical, or otherwise duplicate terminal rows.

For the first legacy-compatible LNG manifest, build `gie_alsi_lng_terminals_v2`.
This excludes Spain's TVB virtual balancing LNG tank while preserving the raw
facility in v1:

```bash
python3 -m modern_pipeline.scripts.build_gie_lng_terminal_manifest \
  --version gie_alsi_lng_terminals_v2 \
  --write
```

`gie_alsi_lng_terminals_v2` also stores the LNG GCV conversion from ENTSOG's Gas
Quality Outlook 2024. The LNG materialiser uses the manifest converter when
building `valueMcm`.

## Compare And Plot Pipeline Plus LNG

Compare the pipeline manifest plus ALSI LNG manifest against the downloaded
legacy Figure 1 country workbook:

```bash
python3 -m modern_pipeline.scripts.compare_legacy_country_data \
  --manifest-version legacy_excel_v4 \
  --lng-manifest-version gie_alsi_lng_terminals_v2 \
  --year 2022 \
  --year 2023 \
  --year 2024
```

Generate quick combined Figure 1-style diagnostics:

```bash
python3 -m modern_pipeline.scripts.plot_figure1_diagnostic \
  --manifest-version legacy_excel_v4 \
  --lng-manifest-version gie_alsi_lng_terminals_v2 \
  --from 2022-01-01 \
  --to 2024-12-31
```

Generate annual source totals in million cubic metres:

```bash
python3 -m modern_pipeline.scripts.plot_figure1_diagnostic \
  --manifest-version legacy_excel_v4 \
  --lng-manifest-version gie_alsi_lng_terminals_v2 \
  --from 2016-01-01 \
  --to 2025-12-31 \
  --frequency Y \
  --value-field valueMcm
```

## Fetch Eurostat Annual Comparison Snapshot

Fetch a compact annual Eurostat `nrg_ti_gas` snapshot for EU27_2020 imports in
million cubic metres. Pipeline sources use `siec=G3000` and partners
`NO/RU/UK/AZ/DZ`; LNG uses `siec=G3200` and `partner=TOTAL`.

```bash
python3 -m modern_pipeline.scripts.fetch_eurostat_gas_imports
```

The JSON snapshot is written to:

```text
modern_pipeline/snapshots/eurostat_nrg_ti_gas_eu27_annual.json
```

## Compare Norway With Official Delivery Data

Compare annual curated Norway values with the official Norwegian delivery-point
benchmark used in the Norway diagnostics:

```bash
python3 -m modern_pipeline.scripts.plot_norway_official_comparison \
  --manifest-version legacy_excel_v4 \
  --from-year 2016 \
  --to-year 2025
```

The comparison writes a CSV plus line and difference plots to
`modern_pipeline/snapshots`.

## Compare With Legacy Figure 1 Workbook

Compare weekly source-group values in `gas_import_daily` with
`legacy_country_data.xlsx`:

```bash
python3 -m modern_pipeline.scripts.compare_legacy_country_data \
  --manifest-version legacy_excel_v4 \
  --year 2022
```

The comparison uses `isoYear`/`isoWeek`, matching the legacy weekly Figure 1
construction. For manifests with `flowConcept`, it uses `net_import` for UK and
`gross_import` for the other source groups. It writes:

```text
modern_pipeline/snapshots/legacy_country_data_comparison.csv
```
