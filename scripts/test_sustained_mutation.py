import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("mutations", Path(__file__).with_name("check-invariant-mutations.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class SustainedEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.prefix = str(Path(self.temporary.name) / "matrix")
        self.mutation = dict(test="Target", sustained_duration="10m", environment={"WF_SUSTAINED_MUTATION": "cas"})
        self.admission = dict(mode="cas", seed=42, duration_ns=600*10**9, elapsed_ns=601*10**9,
                              release_duration=True, same_stores=True, batches=20, faults=19,
                              invocation_cutoff=560, retained=dict(Invocations=560, Journals=560, Entries=6000, Terminal=560))
        self.faults = dict(seed=42, duration="10m0s", faults=[dict(killed="2026-10-02T00:00:30Z", healed="2026-10-02T00:00:31Z")] * 19)
        self.write_artifacts()

    def write_artifacts(self):
        Path(self.prefix + "-faults.json").write_text(json.dumps(self.faults))
        for suffix in ("-history.jsonl", "-dispatch.jsonl"):
            Path(self.prefix + suffix).write_text('{"original":true}\n')
        samples = []
        for typ, each in zip(("matrixshort", "matrixtimer", "matrixsignal", "matrixfanout", "matrixchild", "matrixgrandchild"), (4,3,2,1,6,12)):
            for index in range(self.admission["batches"]*each):
                for event, observed, delay in (("terminal", "2026-10-02T00:00:01.25Z", 1250000000), ("start", "2026-10-02T00:00:00.999Z", 999000000)):
                    samples.append(dict(type=typ, id=str(index), event=event, enabled="2026-10-02T00:00:00Z", observed=observed, delay_ns=delay))
        Path(self.prefix + "-latencies.json").write_text(json.dumps(samples))

    def events(self):
        output = "MIXED_SUSTAINED_ADMISSION " + json.dumps(self.admission) + "\nMIXED_SUSTAINED_PRESERVED cutoff=560 report={}\n"
        for typ, each in zip(("matrixshort", "matrixtimer", "matrixsignal", "matrixfanout", "matrixchild", "matrixgrandchild"), (4,3,2,1,6,12)):
            output += f"MATRIX_CELL type={typ} invocations={self.admission['batches']*each} terminal_p99=1.25s\n"
            output += f"MATRIX_PROGRESS type={typ} enabled_events={self.admission['batches']*each} progress_p99=999ms\n"
        return [dict(Test="Target", Output=output), dict(Test="Target", Action="pass", Elapsed=650)]

    def test_complete_and_smoke_classification(self):
        self.assertTrue(module.check_sustained_evidence(self.events(), self.mutation, self.prefix)["accepted"])
        self.mutation["sustained_duration"] = "35s"
        self.admission.update(duration_ns=35*10**9, elapsed_ns=36*10**9, release_duration=False, faults=1)
        self.faults.update(duration="35s", faults=self.faults["faults"][:1])
        self.write_artifacts()
        result = module.check_sustained_evidence(self.events(), self.mutation, self.prefix)
        self.assertFalse(result["admission"]["release_duration"])

    def test_missing_admission_wrong_seed_duration_cohort_and_store_rejected(self):
        with self.assertRaises(ValueError):
            module.check_sustained_evidence([], self.mutation, self.prefix)
        for action, elapsed in (("skip", 650), ("pass", 35), ("fail", 599)):
            events = self.events()
            events[-1].update(Action=action, Elapsed=elapsed)
            with self.subTest(action=action, elapsed=elapsed), self.assertRaises(ValueError):
                module.check_sustained_evidence(events, self.mutation, self.prefix)
        original = copy.deepcopy(self.admission)
        for key, bad in (("seed", 43), ("mode", "leases"), ("elapsed_ns", 599*10**9), ("duration_ns", 35*10**9),
                         ("release_duration", False), ("same_stores", False), ("faults", 18), ("batches", 0), ("invocation_cutoff", 0)):
            self.admission = copy.deepcopy(original)
            self.admission[key] = bad
            with self.subTest(key=key), self.assertRaises(ValueError):
                module.check_sustained_evidence(self.events(), self.mutation, self.prefix)
        self.admission = original
        self.admission["retained"]["Terminal"] = 559
        with self.assertRaises(ValueError):
            module.check_sustained_evidence(self.events(), self.mutation, self.prefix)

    def test_incomplete_original_evidence_and_liveness_rejected(self):
        original = self.events()
        for before, after in (("MIXED_SUSTAINED_PRESERVED", "missing"), ("type=matrixgrandchild", "type=other"),
                              ("terminal_p99=1.25s", "terminal_p99=30s"), ("progress_p99=999ms", "progress_p99=31s"),
                              ("invocations=80", "invocations=79")):
            events = copy.deepcopy(original)
            events[0]["Output"] = events[0]["Output"].replace(before, after)
            with self.subTest(before=before), self.assertRaises(ValueError):
                module.check_sustained_evidence(events, self.mutation, self.prefix)
        self.faults["faults"] = self.faults["faults"][:18]
        self.write_artifacts()
        with self.assertRaises(ValueError):
            module.check_sustained_evidence(original, self.mutation, self.prefix)
        self.faults["faults"] += [dict(killed="2026-10-02T00:00:30Z", healed="2026-10-02T00:00:29Z")]
        self.write_artifacts()
        with self.assertRaises(ValueError):
            module.check_sustained_evidence(original, self.mutation, self.prefix)
        self.faults["faults"][-1]["healed"] = "2026-10-02T00:00:31Z"
        self.write_artifacts()
        Path(self.prefix + "-history.jsonl").unlink()
        with self.assertRaises(OSError):
            module.check_sustained_evidence(original, self.mutation, self.prefix)

    def test_raw_latency_corruption_and_truncated_cohort_rejected(self):
        path = Path(self.prefix + "-latencies.json")
        original = json.loads(path.read_text())
        for change in (lambda samples: samples.pop(), lambda samples: samples[0].update(delay_ns=0),
                       lambda samples: samples[0].update(id="1"), lambda samples: samples[0].update(observed="2026-10-02T00:00:32Z", delay_ns=32000000000)):
            samples = copy.deepcopy(original)
            change(samples)
            path.write_text(json.dumps(samples))
            with self.assertRaises(ValueError):
                module.check_sustained_evidence(self.events(), self.mutation, self.prefix)


if __name__ == "__main__":
    unittest.main()
