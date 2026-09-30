import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("disk_trace", Path(__file__).with_name("summarize-disk-trace.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class DiskTrace(unittest.TestCase):
    def test_interleaved_resumes_keep_paths_and_start_times(self):
        records, incomplete = module.parse([
            '10 100.000000 pwrite64(30</store/streams/KV_WF_LEASE/msgs/1.blk>, "a", 1, 0 <unfinished ...>',
            '11 100.010000 pwrite64(31</store/streams/WF_JRN/msgs/1.blk>, "b", 1, 0 <unfinished ...>',
            '10 100.020000 --- SIGURG {si_signo=SIGURG} ---',
            '11 100.100000 <... pwrite64 resumed>) = 1 (DELAYED) <0.090000>',
            '10 100.110000 <... pwrite64 resumed>) = 1 (DELAYED) <0.110000>',
            '12 100.120000 fsync(42</store/$SYS/_js_/S-test/msgs/1.blk>) = 0 (DELAYED) <0.090000>',
        ])
        self.assertEqual(incomplete, {"unfinished_calls": 0, "unmatched_lines": 0})
        self.assertEqual([r["start"] for r in records], [100.01, 100.0, 100.12])
        self.assertEqual([module.group(r["path"]) for r in records], ["WF_JRN", "KV_WF_LEASE", "raft_group_unattributed"])
        summary = module.summarize(records, [{"At": "1970-01-01T00:01:40.100000Z", "LeaseUpdateDuration": 100000000, "LeaseUpdateAttempted": True, "Operation": "renew", "JournalIndex": 2, "Type": "a", "ID": "b"}])
        self.assertEqual(summary["slow_renewals"][0]["overlapping_delayed_calls"], {"KV_WF_LEASE": 1, "WF_JRN": 1})

    def test_jsz_mapping_distinguishes_streams_and_consumers(self):
        snapshot = {"account_details": [{"name": "$G", "stream_detail": [
            {"name": "KV_WF_LEASE", "stream_raft_group": "S-lease"},
            {"name": "WF_RUN", "stream_raft_group": "S-run", "consumer_raft_groups": [
                {"name": "WF_P_00", "raft_group": "C-zero"}]}]}]}
        mapping = module.raft_mapping([snapshot])
        self.assertEqual(module.group("/store/$SYS/_js_/S-lease/msgs/1.blk", mapping), "raft:$G/KV_WF_LEASE")
        self.assertEqual(module.group("/store/$SYS/_js_/C-zero/msgs/1.blk", mapping), "raft:$G/WF_RUN/WF_P_00")
        self.assertEqual(module.group("/store/$SYS/_js_/unknown/msgs/1.blk", mapping), "raft_group_unattributed")

    def test_incomplete_or_unmatched_calls_are_reported(self):
        records, incomplete = module.parse([
            '10 100.000000 pwrite64(30</store/file>, "a", 1, 0 <unfinished ...>',
            '11 100.010000 <... fsync resumed>) = 0 (DELAYED) <0.090000>',
        ])
        self.assertEqual(records, [])
        self.assertEqual(incomplete, {"unfinished_calls": 1, "unmatched_lines": 1})


if __name__ == "__main__":
    unittest.main()
