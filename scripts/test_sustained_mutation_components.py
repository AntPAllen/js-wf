import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("components", Path(__file__).with_name("check-sustained-mutation-components.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
A, B = "a" * 40, "b" * 40


class ComponentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        runner = (module.campaign.runner.ROOT / module.RUNNER).read_bytes()
        self.sources = {A: {"runtime.go": b"runtime", "runtime_test.go": b"fixture",
                            "go.mod": b"module", "go.sum": b"dependencies",
                            module.WORKFLOW: b"workflow", module.RUNNER: runner},
                        B: {"runtime.go": b"runtime", "runtime_test.go": b"fixture",
                            "go.mod": b"module", "go.sum": b"dependencies",
                            module.WORKFLOW: b"workflow", module.RUNNER: runner + b"\n# preflight-only change\n"}}
        self.components = []
        for index, mode in enumerate(module.campaign.MODES):
            revision = B if mode == "start-repair" else A
            job = dict(name=f"sustained ({mode})", databaseId=index, status="completed", conclusion="success")
            metadata = dict(headSha=revision, status="completed", conclusion="failure", jobs=[job])
            (self.root / (mode + ".json")).write_text(json.dumps(metadata))
            (self.root / (mode + ".log")).write_text(revision + "\nbaseline passed; production mutation detected\n")
            artifact = self.root / mode
            artifact.mkdir()
            (artifact / "raw.json").write_text("synthetic reviewer control")
            self.components.append(dict(mode=mode, job_id=index, metadata=mode + ".json",
                                        job_log=mode + ".log", artifact_root=mode))
        self.source_reader = mock.patch.object(module.campaign, "git_sources", side_effect=lambda revision: self.sources[revision])
        self.raw_reviewer = mock.patch.object(module.campaign, "check_category", return_value={"synthetic": True})
        self.source_reader.start()
        self.reviewer = self.raw_reviewer.start()
        self.addCleanup(self.source_reader.stop)
        self.addCleanup(self.raw_reviewer.stop)

    def test_six_equivalent_components_reviewed_without_promoting_failed_parents(self):
        report = module.check(self.components, A, self.root)
        self.assertEqual(self.reviewer.call_count, 6)
        self.assertEqual(report["source_revisions"], [A, B])
        self.assertTrue(report["clears_sustained_six_mutation_gate"])
        self.assertFalse(report["qualifies_parent_campaigns"])
        self.assertFalse(report["clears_full_release"])
        for call in self.reviewer.call_args_list:
            self.assertEqual(call.args[2], "10m")
        self.assertTrue(all(item["artifact_sha256"] for item in report["categories"]))

    def test_missing_duplicate_unknown_and_smoke_are_rejected(self):
        for components in (self.components[:-1], self.components + [self.components[0]],
                           self.components[:-1] + [self.components[0]],
                           [dict(item, mode="unknown") for item in self.components]):
            with self.assertRaises(ValueError):
                module.check(components, A, self.root)
        with self.assertRaises(ValueError):
            module.check(self.components, A, self.root, "35s")
        self.reviewer.assert_not_called()

    def test_runtime_fixture_dependency_workflow_or_mutation_drift_is_rejected(self):
        original = copy.deepcopy(self.sources[B])
        for path in ("runtime.go", "runtime_test.go", "go.mod", "go.sum", module.WORKFLOW):
            self.sources[B] = dict(original, **{path: b"different"})
            with self.subTest(path=path), self.assertRaises(ValueError):
                module.check(self.components, A, self.root)
        self.sources[B] = dict(original)
        self.sources[B]["extra.go"] = b"new input"
        with self.assertRaises(ValueError):
            module.check(self.components, A, self.root)
        self.sources[B] = dict(original)
        self.sources[B][module.RUNNER] = original[module.RUNNER].replace(
            b"if true { return ScanResult{NextSequence: next}, nil }",
            b"if false { return ScanResult{NextSequence: next}, nil }", 1)
        with self.assertRaisesRegex(ValueError, "selected mutation"):
            module.check(self.components, A, self.root)

    def test_live_failed_skipped_wrong_or_duplicate_job_is_rejected(self):
        first = self.components[0]
        path = self.root / first["metadata"]
        original = json.loads(path.read_text())
        for kind in ("live", "failed", "skipped", "id", "duplicate", "revision"):
            metadata = copy.deepcopy(original)
            if kind == "live": metadata["jobs"][0]["status"] = "in_progress"
            if kind in ("failed", "skipped"): metadata["jobs"][0]["conclusion"] = kind
            if kind == "id": metadata["jobs"][0]["databaseId"] = 1000
            if kind == "duplicate": metadata["jobs"].append(copy.deepcopy(metadata["jobs"][0]))
            if kind == "revision": metadata["headSha"] = "main"
            path.write_text(json.dumps(metadata))
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                module.check(self.components, A, self.root)

    def test_missing_checkout_or_raw_reviewer_failure_prevents_promotion(self):
        log = self.root / self.components[0]["job_log"]
        original = log.read_text()
        log.write_text("baseline passed; production mutation detected")
        with self.assertRaises(ValueError):
            module.check(self.components, A, self.root)
        log.write_text(original)
        self.reviewer.side_effect = ValueError("raw phase rejected")
        with self.assertRaisesRegex(ValueError, "raw phase rejected"):
            module.check(self.components, A, self.root)


if __name__ == "__main__":
    unittest.main()
