import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("matrix_result", Path(__file__).with_name("check-matrix-result.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class MatrixResultTests(unittest.TestCase):
    def test_invalid_duration_cannot_establish_sustained_execution(self):
        for elapsed in (None, True, "600", -1, float("nan"), float("inf"), -float("inf")):
            with self.subTest(elapsed=elapsed), self.assertRaises(ValueError):
                module.check([{"Test": "Target", "Action": "pass", "Elapsed": elapsed},
                              {"Action": "pass"}], "Target", "10m")

    def test_failed_duplicate_or_unrelated_package_cannot_complete_test(self):
        target = {"Test": "Target", "Package": "one", "Action": "pass", "Elapsed": 600}
        for suffix in (
            [{"Package": "two", "Action": "pass"}],
            [{"Package": "one", "Action": "pass"}] * 2,
            [{"Package": "one", "Action": "skip"}],
            [{"Package": "one", "Action": "fail"}, {"Package": "one", "Action": "pass"}],
            [{"Action": "build-fail"}, {"Package": "one", "Action": "pass"}],
            [{"Test": "Other", "Action": "fail"}, {"Package": "one", "Action": "pass"}],
        ):
            with self.subTest(suffix=suffix), self.assertRaises(ValueError):
                module.check([target, *suffix], "Target", "10m")
        module.check([target, {"Package": "one", "Action": "pass"}], "Target", "10m")

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
