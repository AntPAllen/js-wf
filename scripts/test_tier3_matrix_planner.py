import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('planner', Path(__file__).with_name('tier3-matrix-campaign.py'))
planner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(planner)


class PlannerTests(unittest.TestCase):
    def test_all_ranges_are_complete_unique_and_bounded(self):
        for count in (1, 20, 200):
            jobs = planner.campaign('all', count)
            self.assertLessEqual(len(jobs), 256)
            self.assertEqual(len(planner.ROWS), 14)
            for row in planner.ROWS:
                selected = [job for job in jobs if job['row'] == row]
                self.assertEqual([seed for job in selected for seed in range(job['first'], job['last']+1)], list(range(1, count+1)))
                self.assertTrue(all(job['last']-job['first'] < 12 for job in selected))
            self.assertEqual(len({(j['row'], j['artifact_seed']) for j in jobs}), len(jobs))

    def test_focused_ranges_preserve_single_seed_jobs(self):
        jobs = planner.campaign('server_clock_ahead', 20, 27)
        self.assertEqual([j['first'] for j in jobs], list(range(27, 47)))
        self.assertTrue(all(j['first'] == j['last'] for j in jobs))

    def test_invalid_ranges_and_counts(self):
        for args in [('unknown', 1), ('all', 2), ('all', True), ('all', 1, 0),
                     ('all', 1, True), ('all', 20, 2**63-10)]:
            with self.assertRaises(ValueError): planner.campaign(*args)
        with self.assertRaises(ValueError): planner.campaign('journal', 1, 1, True)
        self.assertEqual(len(planner.campaign('all', 1, 1, True)), 14)

    def test_registry_matches_workflow(self):
        workflow = Path('.github/workflows/tier3-mixed-journal.yml').read_text()
        for row, test in planner.rows.TESTS.items():
            self.assertIn(row, workflow)
            self.assertIn(test, workflow)


if __name__ == '__main__':
    unittest.main()
