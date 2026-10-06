import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("terminal", Path(__file__).with_name("review-bulk-soak-terminal.py"))
terminal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(terminal)


def stat(pid, started):
    return str(pid) + " (worker with ) name) " + " ".join(["0"] * 19 + [str(started)])


class TerminalControls(unittest.TestCase):
    def test_only_full_native_pass_with_original_deadline_can_be_accepted(self):
        event = dict(Test=terminal.TEST, Action="pass", Elapsed=86410)
        self.assertEqual(terminal.native_terminal([event], "24h"), event)
        for rows in ([], [event, event], [dict(event, Action="fail")],
                     [dict(event, Elapsed=626.35)], [dict(event, Elapsed=87601)],
                     [dict(event, Elapsed=float("inf"))], [dict(event, Elapsed="86410")]):
            with self.assertRaises(ValueError):
                terminal.native_terminal(rows, "24h")
        self.assertEqual(terminal.native_terminal([dict(event, Elapsed=626.35)], "10m")["Elapsed"], 626.35)

    def test_live_incarnation_rejected_reused_pid_accepted_and_missing_stat_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            proc = Path(temp)
            record = dict(pid=42, stat=stat(42, 123))
            self.assertEqual(terminal.process_closed(record, proc)["state"], "absent")
            (proc / "42").mkdir()
            (proc / "42/stat").write_text(stat(42, 123))
            with self.assertRaisesRegex(ValueError, "still live"):
                terminal.process_closed(record, proc)
            (proc / "42/stat").write_text(stat(42, 124))
            self.assertIn("PID reused", terminal.process_closed(record, proc)["state"])
            for invalid in (dict(pid=42, stat=stat(43, 123)), dict(pid=0, stat=stat(0, 123))):
                with self.assertRaises(ValueError):
                    terminal.process_closed(invalid, proc)
            with self.assertRaises((ValueError, IndexError)):
                terminal.birth("incomplete")

    def test_original_24h_profile_rejects_shortening_race_missing_gates_and_admission(self):
        state = dict(source="revision", duration="24h", row="journal", seed=1, race=False,
                     clears_full_tier3_release=False, chunked_state_retained_audit=True,
                     bulk_final_latency=True, compare_bulk_point=True,
                     concurrent_state_retained_audit=False, streaming_state_retained_audit=False,
                     batched_retained_audit=False, cached_latency_metadata=False,
                     upgrade_start_gap=False, journal_rollout="none", memory_limit="4GiB",
                     gomaxprocs="4", gc_percent="500", retained_audit_trace=True,
                     audit_wait_stack=True, explicit_route_seeds=True,
                     disk_admission=dict(additional_reserve_bytes=1024**3,
                                         minimum_free_bytes=17*1024**3, free_bytes=17*1024**3))
        env = dict(terminal.PROFILE, WF_TIER3_MATRIX_DURATION="24h", WF_TIER3_SYNC_INTERVAL="2m",
                   FAULT_SEED="1", TIER3_MATRIX_ARTIFACT_ROOT="/original/fixture",
                   WF_TIER3_RETAINED_AUDIT_TRACE="1", WF_TIER3_AUDIT_WAIT_STACK="1",
                   WF_TIER3_EXPLICIT_ROUTE_SEEDS="1")
        parent = dict(pid=42, args=["/original/integration.test", "-test.run=^"+terminal.TEST+"$",
                                   "-test.v=test2json", "-test.count=1", "-test.timeout=24h20m"],
                      profile_environment=dict(terminal.PROFILE))
        def check(s=state, e=env, p=parent):
            terminal.validate_profile(s, e, p, Path("/original"), "24h", "revision", 42)
        check()
        changes = [("duration", "10m"), ("source", "other"), ("seed", 2), ("row", "consumer"),
                   ("race", True), ("compare_bulk_point", False), ("bulk_final_latency", False),
                   ("chunked_state_retained_audit", False), ("cached_latency_metadata", True),
                   ("clears_full_tier3_release", True), ("explicit_route_seeds", False)]
        for key, value in changes:
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    check(dict(state, **{key: value}))
        for key, value in (("WF_TIER3_MATRIX_DURATION", "10m"), ("WF_TIER3_SYNC_INTERVAL", "100ms"),
                           ("WF_MATRIX_BULK_POINT_COMPARE", "0"), ("WF_TIER3_AUDIT_WAIT_STACK", "0")):
            with self.assertRaises(ValueError):
                check(e=dict(env, **{key: value}))
        p = copy.deepcopy(parent)
        p["args"][-1] = "-test.timeout=20m"
        with self.assertRaisesRegex(ValueError, "argv/deadline"):
            check(p=p)
        s = copy.deepcopy(state)
        s["disk_admission"]["free_bytes"] -= 1
        with self.assertRaisesRegex(ValueError, "disk admission"):
            check(s=s)


if __name__ == "__main__":
    unittest.main()
