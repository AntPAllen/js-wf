#!/usr/bin/env python3
"""Review explicit indexed worker wiring, not full-capacity acceptance."""
import gzip
import hashlib
import json
from pathlib import Path
import re

here = Path(__file__).resolve().parent
repo = here.parents[2]
for name, item in json.loads((here / "sources.json").read_text()).items():
    frozen = gzip.decompress((here / item["archive"]).read_bytes())
    assert hashlib.sha256(frozen).hexdigest() == item["sha256"]
    assert (repo / name).read_bytes() == frozen, name
worker = (here / "worker-race.log").read_text()
journal = (here / "journal-race.log").read_text()
for log, package in ((worker, "worker"), (journal, "journal")):
    assert re.search(r"^ok\s+js-wf/" + package + r"\s+", log, re.M)
for test in ("TestNativeGraphIndexedStoredRenewalFreshWorkerRecovery", "TestNativeGraphStoredMaintenanceFreshWorkerRecovery"):
    assert len(re.findall(r"--- PASS: " + test + r"/R[13]-domain ", worker)) == 2
pattern = r"NATIVE_STORED_WORKER replicas=([13]) indexed=true fresh_workers=3 stage_batches=\[1 5 0\] verify_batches=\[0 10 0\] renew_batches=\[1 2 0\] owner_census=\[1 1 0\] full_census=0 initial_calls=1 stage_calls=1 pending_next=2 source_head_unchanged=true descriptors_after_recovery=0 readers=0"
assert sorted(re.findall(pattern, worker)) == ["1", "3"]
assert len(re.findall(r"--- PASS: TestNativeGraphOwnerIndexModeAdmission/indexed=(?:true|false) ", journal)) == 2
assert "--- PASS: TestNativeGraphJournalPublicAdmission " in journal
bypass = (here / "bypass.log").read_text()
assert json.loads((here / "bypass.json").read_text())["exit"] == 1
assert "[build failed]" not in bypass
assert len(re.findall(r"--- FAIL: TestNativeGraphIndexedStoredRenewalFreshWorkerRecovery/R[13]-domain ", bypass)) == 2
assert bypass.count("indexed worker did not use owner-filtered discovery [0 0 0] 2") == 2
pins = (here / "pinned.log").read_text()
assert re.search(r"^ok\s+js-wf/sim\s+", pins, re.M)
assert len(re.findall(r"--- PASS: TestPinnedRegressionCorpus/", pins)) == 853
result = dict(indexed_worker_cases=2, legacy_worker_cases=2, mode_admission_cases=2,
              prior_admission_controls=1, adapter_bypass_failures=2, pins=853,
              worker_wiring_verified=True, large_owned_setup_qualified=False,
              actual_100000_entries_qualified=False)
(here / "review.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
