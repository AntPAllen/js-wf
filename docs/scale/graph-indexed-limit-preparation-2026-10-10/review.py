#!/usr/bin/env python3
"""Review fixture configuration and interface preservation, not full-entry scale."""
import gzip
import hashlib
import json
from pathlib import Path
import re

here = Path(__file__).resolve().parent
repo = here.parents[2]
for name, item in json.loads((here / "sources.json").read_text()).items():
    frozen = gzip.decompress((here / item["archive"]).read_bytes())
    assert hashlib.sha256(frozen).hexdigest() == item["sha256"], name
    assert (repo / name).read_bytes() == frozen, name
def passed(name):
    data = (here / name).read_text()
    assert re.search(r"^ok\s+js-wf/worker\s+", data, re.M), name
    assert "--- FAIL:" not in data and "WARNING: DATA RACE" not in data
    return data
for name in ("profiled-worker-race.log", "restored-worker-race.log"):
    data = passed(name)
    assert len(re.findall(r"--- PASS: TestNativeGraphIndexedProfileStoredRenewalFreshWorkerRecovery/R[13]-domain ", data)) == 2
    assert sorted(re.findall(r"NATIVE_PROFILED_WORKER replicas=([13]) owner_scan_calls=2 marker_calls=\d+ static_census_calls=0 preserved_incremental=true", data)) == ["1", "3"]
    assert len(re.findall(r"owner_census=\[1 1 0\] full_census=0", data)) == 2
mutant = (here / "hide-discovery.log").read_text()
assert json.loads((here / "mutation.json").read_text())["exit"] == 1
assert "[build failed]" not in mutant and "panic:" not in mutant
assert len(re.findall(r"--- FAIL: TestNativeGraphIndexedProfileStoredRenewalFreshWorkerRecovery/R[13]-domain ", mutant)) == 2
assert mutant.count("indexed worker did not use owner-filtered discovery [0 0 0] 2") == 2
limit = passed("limit20-race.log")
assert len(re.findall(r"--- PASS: TestNativeGraphContinuationGlobalLimitAndTerminalSlot/R[13]/archive=true ", limit)) == 2
assert limit.count("GRAPH_LIMIT_CONFIG owner_index=true compaction_ttl=3h0m0s default_append_ttl=true") == 2
receipt = "GRAPH_CONTINUATION_LIMIT budget=20 entries=20 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=19 prefix_stage_calls=1/1 production_cap=false padding_operations=2"
assert limit.count(receipt) == 2
legacy = passed("legacy-race.log")
assert len(re.findall(r"--- PASS: TestNativeGraphContinuationGlobalLimitAndTerminalSlot/R1/archive=true ", legacy)) == 1
assert "GRAPH_LIMIT_CONFIG owner_index=false compaction_ttl=0s default_append_ttl=true" in legacy
assert legacy.count(receipt) == 1
result = dict(profile_recovery_cases=2, restored_cases=2, discovery_bypass_failures=2,
              indexed_durable_limit_cases=2, legacy_profile_cases=1, private_budget=20,
              production_changed=False, actual_100000_entries_qualified=False)
(here / "review.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
