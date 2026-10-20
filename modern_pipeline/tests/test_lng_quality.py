import datetime as dt
import unittest
from modern_pipeline.lng_quality import capacity_check
from modern_pipeline.scripts.fetch_gie_lng_raw_observations import normalize_observation
from modern_pipeline.scripts.materialize_gie_lng_daily import materialize_rows


class LngQualityTests(unittest.TestCase):
    def test_threshold_and_missing_capacity(self):
        self.assertEqual(capacity_check(200, 100)['status'], 'valid')
        self.assertEqual(capacity_check(200.01, 100)['status'], 'invalid')
        for capacity in [None, 0, -1, float('nan'), float('inf')]:
            self.assertEqual(capacity_check(100, capacity)['status'], 'not_checked')

    def test_ingestion_flags_and_revised_source_clears_flag(self):
        row = {'gasDayStart': '2026-10-05', 'sendOut': '100000', 'dtrs': '146.7'}
        obs = normalize_observation(row, {'facilityEic': 'ravenna'}, dt.datetime.now())
        self.assertEqual(obs['capacityCheck']['status'], 'invalid')
        self.assertEqual(obs['valueGwh'], 100000)
        row['sendOut'] = '25'
        self.assertEqual(normalize_observation(row, {}, dt.datetime.now())['capacityCheck']['status'], 'valid')

    def test_old_unflagged_data_and_manual_replacement(self):
        entries = [{'facilityEic': 'ravenna', 'countryIso2': 'IT'}, {'facilityEic': 'other', 'countryIso2': 'IT'}]
        raw = {'facilityEic': 'ravenna', 'gasDay': dt.datetime(2026, 10, 5), 'valueGwh': 100000, 'technicalSendoutCapacityGwhPerDay': 146.7}
        other = {**raw, 'facilityEic': 'other', 'valueGwh': 50}
        with self.assertWarns(UserWarning):
            rows = materialize_rows('test', entries, [raw, other], 10.3, False)
        self.assertTrue(all(row['valueGwh'] == 50 for row in rows))
        self.assertTrue(all(row['isIncomplete'] and row['excludedObservations'] for row in rows))
        raw['qualityOverride'] = {'sourceValueInvalid': True, 'replacementGwh': 25.05}
        rows = materialize_rows('test', entries, [raw, other], 10.3, False)
        self.assertTrue(all(row['valueGwh'] == 75.05 and row['qualityOverrides'] for row in rows))
        self.assertTrue(all(not row['isIncomplete'] and not row['excludedObservations'] for row in rows))
        raw['qualityOverride'].pop('replacementGwh')
        rows = materialize_rows('test', entries, [raw, other], 10.3, False)
        self.assertTrue(all(row['valueGwh'] == 50 for row in rows))


if __name__ == '__main__':
    unittest.main()
