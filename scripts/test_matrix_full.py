import copy
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest
import zipfile

spec = importlib.util.spec_from_file_location("full_matrix", Path(__file__).with_name("check-full-matrix.py"))
full = importlib.util.module_from_spec(spec)
spec.loader.exec_module(full)


def seed_log(row, seed):
    runtime, test = full.ROWS[row]
    lines = [f"Sustained matrix row={row} seed={seed} duration=10m",
             f"MATRIX_RESULT row={runtime} seed={seed} duration=10m0s release_duration=true batches=1 invocations=28 faults=119 terminal_p99=15s",
             f"MATRIX_RETAINED row={runtime} report={{Invocations:28 Journals:28 Entries:300 Terminal:28}} expected_invocations=28",
             f"--- PASS: {test} (628.65s)",
             f"Verified executed sustained test {test} duration=10m"]
    if row == "worker":
        lines.append("worker faults=119 active_worker_faults=20 row=worker_kill")
    for typ, count in zip(full.gate.TYPES, (4, 3, 2, 1, 6, 12)):
        lines.extend([f"MATRIX_CELL type={typ} invocations={count} terminal_p99=28s",
                      f"MATRIX_PROGRESS type={typ} enabled_events=12 progress_p99=473ms progress_max=13s above_30s=0"])
    return "\n".join(lines)


def fixture(count):
    jobs = full.planner.campaign("all", count, "10m")
    metadata = {"headSha": "a" * 40, "status": "completed", "conclusion": "success",
                "jobs": [{"name": name, "status": "completed", "conclusion": "success"}
                         for name in ["seeds"] + [full.job_name(job) for job in jobs]]}
    logs = {full.job_name(job): "checkout " + metadata["headSha"] + "\n" + "\n".join(seed_log(job["row"], seed) for seed in range(job["first"], job["last"] + 1)) for job in jobs}
    return metadata, logs


class FullMatrixTests(unittest.TestCase):
    def test_cli_rejects_release_without_raw_artifacts_before_reading_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = subprocess.run([sys.executable, str(Path(__file__).with_name("check-full-matrix.py")),
                                     "--jobs", str(root / "absent-jobs.json"),
                                     "--logs", str(root / "absent-logs.zip"),
                                     "--seeds", "200", "--output", str(root / "result.json")],
                                    text=True, capture_output=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn("requires --artifacts", result.stderr)
            self.assertFalse((root / "result.json").exists())

    def test_raw_artifacts_match_and_reject_missing_duplicate_incomplete_or_disagreement(self):
        metadata, logs = fixture(1)
        report = full.check_full_matrix(metadata, logs, 1)
        for change in (None, "missing", "duplicate", "incomplete", "disagreement"):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for row in full.ROWS:
                    test = full.ROWS[row][1]
                    output = seed_log(row, 1)
                    if change == "disagreement" and row == "journal":
                        output = output.replace("terminal_p99=15s", "terminal_p99=16s")
                    events = [{"Action": "output", "Test": test, "Output": output},
                              {"Action": "pass", "Test": test, "Elapsed": 628.65}]
                    if change != "incomplete" or row != "journal":
                        events.append({"Action": "pass"})
                    path = root / f"matrix-{row}-1-test.jsonl"
                    path.write_text("\n".join(json.dumps(e) for e in events))
                if change == "missing":
                    (root / "matrix-journal-1-test.jsonl").unlink()
                if change == "duplicate":
                    (root / "duplicate").mkdir()
                    (root / "duplicate/matrix-journal-1-test.jsonl").write_bytes((root / "matrix-journal-1-test.jsonl").read_bytes())
                if change is None:
                    self.assertEqual(len(full.check_artifacts(report, root)), 13)
                else:
                    with self.assertRaises(ValueError):
                        full.check_artifacts(report, root)

    def test_all_counts_and_scope(self):
        for count in (1, 20, 200):
            metadata, logs = fixture(count)
            report = full.check_full_matrix(metadata, logs, count)
            self.assertEqual(report["executions"], 13 * count)
            self.assertEqual(report["invocations"], 13 * count * 28)
            self.assertFalse(report["clears_tier2_200_seed_gate"])
            self.assertFalse(report["raw_artifacts_verified"])
            self.assertFalse(report["clears_tier3_24_hour_soak"])

    def test_200_seed_release_requires_all_raw_events_and_rejects_failed_promotion(self):
        metadata, logs = fixture(200)
        preflight = full.check_full_matrix(metadata, logs, 200)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(ValueError):
                full.qualify_artifacts(preflight, root)
            for row in full.ROWS:
                test = full.ROWS[row][1]
                for seed in range(1, 201):
                    events = [{"Action": "output", "Test": test, "Output": seed_log(row, seed)},
                              {"Action": "pass", "Test": test, "Elapsed": 628.65},
                              {"Action": "pass"}]
                    (root / f"matrix-{row}-{seed}-test.jsonl").write_text("\n".join(json.dumps(e) for e in events))
            qualified = full.qualify_artifacts(preflight, root)
            self.assertTrue(qualified["clears_tier2_200_seed_gate"])
            self.assertTrue(qualified["raw_artifacts_verified"])
            self.assertEqual(len(qualified["raw_event_sha256"]), 2600)
            # A bad final raw seed cannot promote the original preflight.
            path = root / "matrix-upgrade-200-test.jsonl"
            events = [json.loads(line) for line in path.read_text().splitlines()]
            events[-1]["Action"] = "fail"
            path.write_text("\n".join(json.dumps(e) for e in events))
            with self.assertRaises(ValueError):
                full.qualify_artifacts(preflight, root)
            self.assertFalse(preflight["clears_tier2_200_seed_gate"])

    def test_missing_duplicate_failed_live_and_revision_jobs(self):
        for change in ("missing", "duplicate", "failed", "live", "revision"):
            metadata, logs = fixture(20)
            if change == "missing": metadata["jobs"].pop()
            if change == "duplicate": metadata["jobs"][-1] = copy.deepcopy(metadata["jobs"][1])
            if change == "failed": metadata["jobs"][-1]["conclusion"] = "failure"
            if change == "live": metadata["status"] = "in_progress"
            if change == "revision": metadata["headSha"] = "b" * 40
            with self.assertRaises(ValueError): full.check_full_matrix(metadata, logs, 20)

    def test_partial_wrong_duplicate_smoke_and_missing_result_seeds(self):
        for change in ("missinglog", "wrongseed", "duplicate", "smoke", "missingguard", "missingpass", "latency"):
            metadata, logs = fixture(20)
            name = "leader (journal, 13-20)"
            if change == "missinglog": del logs[name]
            if change == "wrongseed": logs[name] = logs[name].replace("seed=20", "seed=21")
            if change == "duplicate": logs[name] += "\n" + seed_log("journal", 20)
            if change == "smoke": logs[name] = logs[name].replace("duration=10m", "duration=35s")
            if change == "missingguard": logs[name] = logs[name].replace("Verified executed sustained test", "MISSING")
            if change == "missingpass": logs[name] = logs[name].replace("--- PASS:", "--- FAIL:")
            if change == "latency": logs[name] = logs[name].replace("terminal_p99=15s", "terminal_p99=30s")
            with self.assertRaises(ValueError): full.check_full_matrix(metadata, logs, 20)

    def test_registry_matches_existing_workflow_and_go_tests(self):
        workflow = Path(".github/workflows/tier2-matrix-journal.yml").read_text()
        source = Path("integration/mixed_matrix_leader_test.go").read_text()
        self.assertEqual(set(full.ROWS), set(full.planner.ROWS))
        for _, test in full.ROWS.values():
            self.assertIn(test, workflow)
            self.assertIn("func " + test + "(", source)

    def test_duplicate_zip_job_log_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "logs.zip"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("1_leader (journal, 1).txt", "one")
                archive.writestr("2_leader (journal, 1).txt", "two")
            with self.assertRaises(ValueError): full.load_logs(path)


if __name__ == "__main__":
    unittest.main()
