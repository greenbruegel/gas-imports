"""Russian transit must not include post-contract Ukrainian storage returns."""
import datetime as dt
import unittest

from modern_pipeline.scripts.materialize_gas_import_daily import materialize_rows


class UkraineTransitTest(unittest.TestCase):
    def test_cutoff_and_aggregate_totals(self):
        entries = [
            {"pointDirection": "ua", "sourceGroup": "Russia", "routeGroup": "Ukraine Gas Transit"},
            {"pointDirection": "tr", "sourceGroup": "Russia", "routeGroup": "Turkstream"},
            {"pointDirection": "no", "sourceGroup": "Norway"},
        ]
        days = [dt.datetime(2024, 12, 31), dt.datetime(2025, 1, 1), dt.datetime(2026, 10, 1)]
        raw = [{"pointDirection": point, "gasDay": day, "value": value}
               for day in days for point, value in [("ua", 100), ("tr", 200), ("no", 300)]]
        rows = materialize_rows("legacy_excel_v4", entries, [], raw, 1, True, True)
        values = {(r["date"], r["aggregationLevel"], r["seriesKey"]): r["valueKwh"] for r in rows}
        for day in days:
            transit = 100 if day.year == 2024 else 0
            self.assertEqual(values[day, "route_group", "Ukraine Gas Transit"], transit)
            self.assertEqual(values[day, "source_group", "Russia"], 200 + transit)
            self.assertEqual(values[day, "total", "Total"], 500 + transit)
            self.assertEqual(values[day, "source_group", "Norway"], 300)
        self.assertEqual(raw[3]["value"], 100)


if __name__ == "__main__":
    unittest.main()
