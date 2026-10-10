#!/usr/bin/env python3
"""Review deferred validation; retain failed large-census timing qualification."""
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
for log, package in (("model-race.log", "internal/graphpublication"),
                     ("native-race.log", "internal/graphpublication"),
                     ("pagination-race.log", "internal/graphpublication"),
                     ("worker-race.log", "worker"), ("pinned.log", "sim")):
    assert re.search(r"^ok\s+js-wf/" + package + r"\s+", (here / log).read_text(), re.M), log
model = (here / "model-race.log").read_text()
for name, count in (("TestGraphCompactionOwnerWitnessScopeBudget", 7),
                    ("TestGraphCompactionOwnerScopeDiscovery", 11),
                    ("TestGraphCompactionIntentRenewalScopesAndFences", 17)):
    assert len(re.findall(r"--- PASS: " + name + r"/", model)) == count
assert "OWNER_WITNESS mode=reservations100000 census=100020 setup_witnesses=0 validated=2 examined=2 blobs=2 complete=false err=<nil>" in model
bypass = (here / "bypass.log").read_text()
assert json.loads((here / "bypass.json").read_text())["exit"] == 1
assert "[build failed]" not in bypass
assert len(re.findall(r"--- FAIL: TestGraphCompactionOwnerWitnessScopeBudget/", bypass)) == 6
native = (here / "native-race.log").read_text()
assert len(re.findall(r"--- PASS: TestNativeGraphOwnerScopeIndexRecovery/R[13] ", native)) == 2
worker = (here / "worker-race.log").read_text()
assert len(re.findall(r"--- PASS: TestNativeGraphIndexedStoredRenewalFreshWorkerRecovery/R[13]-domain ", worker)) == 2
page = (here / "pagination-race.log").read_text()
assert re.search(r"NATIVE_OWNER_PAGINATION reservations=100001 owned_grants=6 discovered=100007 pages=2 later_pages=1 setup_witnesses=0 three_second_setup=(true|false) .*batch_budget=2 batch_witnesses=2 examined=2 full_renewal_qualified=false", page)
assert "three-second setup failed context deadline exceeded" in (here / "failed-three-second-census.log").read_text()
assert "NATIVE_OWNER_SETUP_GATE target=3s passed=false" in (here / "pre-counter-control.log").read_text()
assert len(re.findall(r"--- PASS: TestPinnedRegressionCorpus/", (here / "pinned.log").read_text())) == 853
result = dict(witness_controls=7, prior_owner_controls=11, prior_safety_controls=17,
              native_recovery_cases=2, indexed_worker_cases=2, pins=853,
              witness_bypass_failures=6, pagination_keys=100007, pagination_pages=2,
              marker_work_bounded=True, stable_three_second_setup_qualified=False,
              full_large_renewal_qualified=False, actual_100000_entries_qualified=False)
(here / "review.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
