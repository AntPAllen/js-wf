import copy
import importlib.util
from pathlib import Path
import unittest
from tier1_partitions import partition_tests

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

    def test_exact_per_workload_completion_and_rejections(self):
        record = {"Package": module.PACKAGE, "Test": "TestSeededWorkload", "Action": "output",
                  "Output": "seed_count_test.go:40: TIER1_SEEDS test=TestSeededWorkload first=1 last=100000 completed=100000 requested=100000\n"}
        def verify(events, inventory="TestSeededWorkload\n"):
            return module.check(events, self.inventory, 100000, "a" * 40, self.regressions, inventory)
        result = verify(self.events + [record])
        self.assertEqual(result["per_workload_seed_proof"]["completed_bodies"], 100000)
        for events in (self.events, self.events + [record, record]):
            with self.assertRaises(ValueError): verify(events)
        for old, new in (("first=1", "first=2"), ("last=100000", "last=99999"),
                         ("completed=100000", "completed=99999"), ("requested=100000", "requested=1000"),
                         ("completed=100000", "completed=x"), ("test=TestSeededWorkload", "test=Other"),
                         ("first=1", "first=1 first=1"), ("first=1", "extra=1")):
            with self.subTest(new=new), self.assertRaises(ValueError):
                verify(self.events + [{**record, "Output": record["Output"].replace(old, new)}])
        for inventory in ("", "TestSeededWorkload\n" * 2, "TestAbsent\n", "TestSeededWorkload/subcase\n"):
            with self.assertRaises(ValueError): verify(self.events + [record], inventory)

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


class Tier1PartitionTests(unittest.TestCase):
    def setUp(self):
        Tier1SuiteTests.setUp(self)
        self.inventory += "TestSeededOther\n"
        self.seeded = "TestSeededWorkload\nTestSeededOther\n"
        self.events += [
            {"Package": module.PACKAGE, "Action": "run", "Test": "TestSeededOther"},
            {"Package": module.PACKAGE, "Action": "pass", "Test": "TestSeededOther"},
        ]
        for name in self.seeded.splitlines():
            self.events.append({"Package": module.PACKAGE, "Action": "output", "Test": name,
                                "Output": f"TIER1_SEEDS test={name} first=1 last=100000 completed=100000 requested=100000\n"})
        self.parts = []
        for local in (["TestSeededWorkload", "TestPinnedRegressionCorpus"], ["TestSeededOther", *module.TRACE_SKIPS]):
            rows = [copy.deepcopy(e) for e in self.events if not e.get("Test") or e["Test"].split("/", 1)[0] in local]
            self.parts.append(("\n".join(local)+"\n", rows))

    def verify(self, parts=None):
        return module.check_parts(self.parts if parts is None else parts, self.inventory, 100000,
                                  "a"*40, self.regressions, self.seeded)

    def test_disjoint_process_union(self):
        report = self.verify()
        self.assertEqual(report["package_processes"], 2)
        self.assertEqual(report["top_level_pass"], 3)
        self.assertEqual(report["pinned_regressions_pass"], 1)
        self.assertEqual(report["per_workload_seed_proof"]["completed_bodies"], 200000)
        self.assertEqual(report["aggregate_counts"]["generated_schedules"], 20)
        self.assertEqual(report["aggregate_counts"]["virtual_ms_max"], 60000)

    def test_partition_plan_preserves_inventory_and_seeded_coverage(self):
        names = ["TestSeededA", "TestControlA", "TestSeededB", "TestSeededC", "TestControlB"]
        seeded = ["TestSeededA", "TestSeededB", "TestSeededC"]
        groups = partition_tests(names, seeded, 3)
        self.assertEqual(groups, partition_tests(names, seeded, 3))
        self.assertEqual(sorted(n for group in groups for n in group), sorted(names))
        self.assertTrue(all(set(group).intersection(seeded) for group in groups))
        for bad_names, bad_seeded, count in ((names+names[:1], seeded, 3), (names, seeded[:1], 3),
                                            (names, seeded+["TestAbsent"], 3), (names, seeded, 0),
                                            (names, seeded, 33), (["Bad"], ["Bad"], 2)):
            with self.assertRaises(ValueError):
                partition_tests(bad_names, bad_seeded, count)

    def test_reject_race_warning_even_with_passing_receipts(self):
        parts = copy.deepcopy(self.parts)
        parts[0][1].insert(0, {"Package": module.PACKAGE, "Action": "output", "Output": "WARNING: DATA RACE\n"})
        with self.assertRaises(ValueError):
            self.verify(parts)

    def test_reject_missing_overlapping_or_empty_processes(self):
        invalid = [self.parts[:1], self.parts+[self.parts[0]],
                   [("", self.parts[0][1]), self.parts[1]],
                   [(self.parts[0][0].replace("TestSeededWorkload\n", ""), self.parts[0][1]), self.parts[1]]]
        for parts in invalid:
            with self.subTest(parts=[p[0] for p in parts]), self.assertRaises(ValueError):
                self.verify(parts)

    def test_reject_cross_process_execution(self):
        parts = copy.deepcopy(self.parts)
        parts[0][1].append({"Package": module.PACKAGE, "Action": "run", "Test": "TestSeededOther"})
        with self.assertRaises(ValueError):
            self.verify(parts)

    def test_reject_incomplete_process_and_seed_proof(self):
        for predicate in (lambda e: not e.get("Test") and e["Action"]=="pass",
                          lambda e: "TIER1_COVERAGE" in e.get("Output", ""),
                          lambda e: "TIER1_SEEDS" in e.get("Output", ""),
                          lambda e: e.get("Test", "").startswith("TestPinnedRegressionCorpus/")):
            parts = copy.deepcopy(self.parts)
            parts[0] = (parts[0][0], [e for e in parts[0][1] if not predicate(e)])
            with self.assertRaises(ValueError):
                self.verify(parts)

    def test_reject_failed_or_incompatible_process(self):
        for old, new in (("model_version=3", "model_version=4"), ("seeds_per_workload=100000", "seeds_per_workload=1000"),
                         ("completed=100000", "completed=99999")):
            parts = copy.deepcopy(self.parts)
            for e in parts[1][1]:
                if "Output" in e:
                    e["Output"] = e["Output"].replace(old, new)
            with self.subTest(new=new), self.assertRaises(ValueError):
                self.verify(parts)
        parts = copy.deepcopy(self.parts)
        for e in parts[1][1]:
            if not e.get("Test") and e["Action"]=="pass":
                e["Action"]="fail"
        with self.assertRaises(ValueError):
            self.verify(parts)


if __name__ == "__main__":
    unittest.main()
