import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("matrix_result", Path(__file__).with_name("check-matrix-result.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class MatrixResultTests(unittest.TestCase):
    def test_full_duration_and_smoke_are_distinct(self):
        events = [{"Test": "Target", "Action": "pass", "Elapsed": 45}, {"Action": "pass"}]
        module.check(events, "Target", "35s")
        with self.assertRaises(ValueError):
            module.check(events, "Target", "10m")
        events[0]["Elapsed"] = 600
        module.check(events, "Target", "10m")
        with self.assertRaises(ValueError):
            module.check(events, "Target", "24h")
        events[0]["Elapsed"] = 86400
        module.check(events, "Target", "24h")

    def test_no_tests_skip_failure_duplicate_and_missing_completion_rejected(self):
        for events in (
            [{"Action": "pass"}],
            [{"Test": "Target", "Action": "skip"}, {"Action": "pass"}],
            [{"Test": "Target", "Action": "fail"}, {"Action": "fail"}],
            [{"Test": "Other", "Action": "pass", "Elapsed": 700}, {"Action": "pass"}],
            [{"Test": "Target", "Action": "pass", "Elapsed": 700}],
            [{"Test": "Target", "Action": "pass", "Elapsed": 700}] * 2 + [{"Action": "pass"}],
        ):
            with self.assertRaises(ValueError):
                module.check(events, "Target", "10m")


if __name__ == "__main__":
    unittest.main()
