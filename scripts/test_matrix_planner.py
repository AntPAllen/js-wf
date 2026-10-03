import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("matrix_campaign", Path(__file__).with_name("matrix-campaign.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class CampaignTests(unittest.TestCase):
    def test_full_release_has_every_seed_in_every_row_once(self):
        for count in (1, 20, 200):
            jobs = module.campaign("all", count, "10m")
            self.assertLessEqual(len(jobs), 256)
            for row in module.ROWS:
                actual = [seed for job in jobs if job["row"] == row for seed in range(job["first"], job["last"] + 1)]
                self.assertEqual(actual, list(range(1, count + 1)))
            self.assertTrue(all(1 <= job["last"] - job["first"] + 1 <= 12 for job in jobs))
            # Longest test command is twenty minutes; setup still fits six hours.
            self.assertLessEqual(max(job["last"] - job["first"] + 1 for job in jobs) * 20, 240)

    def test_individual_campaign_keeps_one_seed_per_job(self):
        jobs = module.campaign("worker", 200, "10m")
        self.assertEqual(len(jobs), 200)
        self.assertEqual([job["artifact_seed"] for job in jobs], [str(i) for i in range(1,201)])
        self.assertEqual([(job["first"], job["last"]) for job in jobs], [(i, i) for i in range(1, 201)])

    def test_smoke_does_not_change_seed_coverage(self):
        self.assertEqual(module.campaign("all", 20, "35s"), module.campaign("all", 20, "10m"))

    def test_focused_replay_preserves_actual_seed_identity(self):
        jobs = module.campaign("journal", 1, "10m", 39)
        self.assertEqual(jobs, [dict(row="journal", first=39, last=39, artifact_seed="39")])
        jobs = module.campaign("all", 20, "10m", 86)
        for row in module.ROWS:
            actual = [seed for job in jobs if job['row'] == row for seed in range(job['first'], job['last']+1)]
            self.assertEqual(actual, list(range(86,106)))

    def test_invalid_start_and_overflow_are_rejected(self):
        for start in (0, -1, True, 1.5, "39", 2**63-10):
            with self.assertRaises(ValueError):
                module.campaign("all", 20, "10m", start)
        with self.assertRaises(ValueError):
            module.campaign("journal", True, "10m")

    def test_invalid_scope_fails_before_launch(self):
        for args in (("unknown", 20, "10m"), ("all", 0, "10m"), ("all", 201, "10m"), ("all", 200, "24h")):
            with self.assertRaises(ValueError):
                module.campaign(*args)


if __name__ == "__main__":
    unittest.main()
