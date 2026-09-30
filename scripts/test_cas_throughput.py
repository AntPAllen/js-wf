import copy
import importlib.util
from pathlib import Path
import unittest
import json
import tempfile

spec = importlib.util.spec_from_file_location("cas_gate", Path(__file__).with_name("check-cas-throughput.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def report(hot=3000, parallel=14000):
    return {"go_version": "go1.27.1", "nats_server_version": "2.15.0", "replicas": 3,
            "storage": "file", "parallel_workers": 1000, "journal_messages": 110000,
            "journal_subjects": 1001,
            "hot": {"invocations": 1, "entries_each": 10000, "appends": 10000,
                    "elapsed_ms": 10000 * 1000 / hot, "appends_per_second": hot},
            "parallel": {"invocations": 1000, "entries_each": 100, "appends": 100000,
                         "elapsed_ms": 100000 * 1000 / parallel, "appends_per_second": parallel}}


class GateControls(unittest.TestCase):
    def test_boundary_and_both_regressions(self):
        baseline = [report()] * 3
        results = gate.compare(baseline, [report(2400, 11200)] * 3)
        self.assertTrue(all(r["passed"] for r in results.values()))
        self.assertFalse(gate.compare(baseline, [report(2399, 14000)] * 3)["hot"]["passed"])
        self.assertFalse(gate.compare(baseline, [report(3000, 11199)] * 3)["parallel"]["passed"])

    def test_one_outlier_does_not_replace_median(self):
        baseline = [report()] * 3
        self.assertTrue(all(r["passed"] for r in gate.compare(baseline, [report(), report(), report(1, 1)]).values()))
        self.assertFalse(gate.compare(baseline, [report(), report(1, 1), report(1, 1)])["hot"]["passed"])

    def test_missing_report_does_not_reuse_previous_measurement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "no-report"
            binary.write_text("#!/bin/sh\nexit 0\n")
            binary.chmod(0o700)
            output = root / "measurement.json"
            output.write_text(json.dumps(report()))
            with self.assertRaises(FileNotFoundError):
                gate.measurement(binary, output, root / "log", 1)

    def test_invalid_measurements_cannot_pass(self):
        for field, value in (("replicas", 1), ("journal_messages", 109999), ("go_version", "other")):
            bad = report()
            bad[field] = value
            with self.assertRaises(ValueError):
                gate.compare([report()] * 3, [bad] * 3)
        for value in (float("nan"), float("inf"), 0, -1):
            bad = copy.deepcopy(report())
            bad["hot"]["appends_per_second"] = value
            with self.assertRaises(ValueError):
                gate.compare([report()] * 3, [bad] * 3)
        bad = report()
        bad["hot"]["elapsed_ms"] *= 2
        with self.assertRaises(ValueError):
            gate.compare([report()] * 3, [bad] * 3)
        with self.assertRaises(ValueError):
            gate.compare([report()] * 3, [report()])


if __name__ == "__main__":
    unittest.main()
