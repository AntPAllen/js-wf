import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("tier1_suite", Path(__file__).with_name("check-tier1-suite.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class Tier1SuiteTests(unittest.TestCase):
    def setUp(self):
        self.inventory = "TestSeededWorkload\nTestPinnedRegressionCorpus\n" + "\n".join(module.TRACE_SKIPS) + "\n"
        self.regressions = "sim/testdata/regressions/probe.json\n"
        self.events = []
        for name in self.inventory.splitlines() + ["TestSeededWorkload/subcase", "TestPinnedRegressionCorpus/probe.json"]:
            self.events.append({"Test": name, "Action": "run"})
            skip = name in module.TRACE_SKIPS
            if skip:
                self.events.append({"Test": name, "Action": "output", "Output": module.TRACE_SKIPS[name] + "\n"})
            self.events.append({"Test": name, "Action": "skip" if skip else "pass"})
        self.events += [
            {"Action": "output", "Output": "TIER1_COVERAGE model_version=3 seeds_per_workload=100000 generated_schedules=10 scheduler_choices=50 transport_events=100 virtual_ms_max=60000 virtual_buckets_zero=1 under_1s=2 under_1m=3 at_least_1m=4 elapsed=1s\n"},
            {"Action": "pass"},
        ]
        for event in self.events:
            event["Package"] = module.PACKAGE

    def check(self, events=None, inventory=None, seeds=100000, source="a" * 40):
        return module.check(self.events if events is None else events,
                            self.inventory if inventory is None else inventory, seeds, source, self.regressions)

    def test_complete_inventory_with_only_documented_skips(self):
        report = self.check()
        self.assertEqual(report["top_level_pass"], 2)
        self.assertEqual(report["aggregate_counts"]["generated_schedules"], 10)
        self.assertIn("not independent", report["scope"])

    def test_missing_workload_or_terminal(self):
        for action in (None, "pass", "run"):
            with self.subTest(action=action), self.assertRaises(ValueError):
                self.check([e for e in self.events if not (e.get("Test") == "TestSeededWorkload" and (action is None or e["Action"] == action))])

    def test_failed_skipped_or_duplicate_workload(self):
        for action in ("fail", "skip"):
            events = copy.deepcopy(self.events)
            events[1]["Action"] = action
            with self.subTest(action=action), self.assertRaises(ValueError):
                self.check(events)
        for index in (0, 1):
            with self.subTest(duplicate=index), self.assertRaises(ValueError):
                self.check(self.events + [self.events[index]])

    def test_skip_requires_documented_reason_and_trace_test_identity(self):
        events = [e for e in self.events if e.get("Test") != "TestReplayFaultTrace" or e["Action"] != "output"]
        with self.assertRaises(ValueError):
            self.check(events)
        with self.assertRaises(ValueError):
            self.check(inventory="TestSeededWorkload\n")

    def test_subtest_skip_or_unfinished_subtest_rejected(self):
        for action in ("skip", None):
            events = copy.deepcopy(self.events)
            index = next(i for i, e in enumerate(events) if e.get("Test", "").endswith("/subcase") and e["Action"] == "pass")
            if action:
                events[index]["Action"] = action
            else:
                events.pop(index)
            with self.subTest(action=action), self.assertRaises(ValueError):
                self.check(events)

    def test_missing_duplicate_failed_or_wrong_package_completion(self):
        for events in (self.events[:-1], self.events + [self.events[-1]],
                       self.events[:-1] + [{"Package": module.PACKAGE, "Action": "fail"}],
                       [{**e, "Package": "other"} for e in self.events]):
            with self.assertRaises(ValueError):
                self.check(events)

    def test_missing_duplicate_malformed_or_inconsistent_coverage(self):
        original = self.events[-2]
        for text in ("", original["Output"] + original["Output"],
                     original["Output"].replace("100000", "10000"),
                     original["Output"].replace("generated_schedules=10", "generated_schedules=11"),
                     original["Output"].replace("model_version=3", "model_version=0"),
                     original["Output"].replace("transport_events=100", "transport_events=-1"),
                     original["Output"] + "\nTIER1_COVERAGE generated_schedules=10\n",
                     original["Output"].replace("under_1m=3", "under_1m=x"),
                     original["Output"].replace("under_1m=3", "under_1m=3 under_1m=3")):
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.check(self.events[:-2] + [{**original, "Output": text}, self.events[-1]])

    def test_missing_or_unexpected_pin_and_invalid_trace_inventory(self):
        with self.assertRaises(ValueError):
            self.check([e for e in self.events if e.get("Test") != "TestPinnedRegressionCorpus/probe.json"])
        for traces in ("", self.regressions * 2, "wrong/probe.json\n", self.regressions + "sim/testdata/regressions/missing.json\n"):
            self.regressions = traces
            with self.subTest(traces=traces), self.assertRaises(ValueError):
                self.check()

    def test_empty_duplicate_incomplete_or_invalid_inventory(self):
        for inventory in ("", self.inventory + "TestSeededWorkload\n", self.inventory + "TestMissing\n", self.inventory + "TestMain\n"):
            with self.subTest(inventory=inventory), self.assertRaises(ValueError):
                self.check(inventory=inventory)
        with self.assertRaises(ValueError):
            self.check(source="main")
        with self.assertRaises(ValueError):
            self.check(seeds=0)


if __name__ == "__main__":
    unittest.main()
