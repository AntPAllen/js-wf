#!/usr/bin/env python3
"""Review incremental discovery; not large complete renewal acceptance."""
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
                     ("worker-race.log", "worker"), ("pinned.log", "sim")):
    assert re.search(r"^ok\s+js-wf/" + package + r"\s+", (here / log).read_text(), re.M), log
model = (here / "model-race.log").read_text()
for name, count in (("TestGraphCompactionOwnerScanCompletenessAndBounds", 12),
                    ("TestGraphCompactionOwnerWitnessScopeBudget", 7),
                    ("TestGraphCompactionOwnerScopeDiscovery", 11),
                    ("TestGraphCompactionIntentRenewalScopesAndFences", 17)):
    assert len(re.findall(r"--- PASS: " + name + r"/", model)) == count
native = (here / "native-race.log").read_text()
assert len(re.findall(r"--- PASS: TestNativeGraphOwnerScanSequenceChurnCompleteness/", native)) == 5
assert len(re.findall(r"--- PASS: TestNativeGraphOwnerScopeIndexRecovery/R[13] ", native)) == 2
assert "--- PASS: TestNativeGraphOwnerScopeIncrementalSetupAt100000 " in native
assert re.search(r"NATIVE_OWNER_INCREMENTAL reservations=100001 owned_grants=6 expected=100008 setup_keys=0 count_requests=1 setup_barrier_witnesses=1 setup_wall=\S+ batch_budget=2 batch_scopes=2 full_renewal_qualified=false", native)
assert sorted(re.findall(r"NATIVE_OWNER_INDEX replicas=([13]) discovered=10 barrier_reservations=1 blob_reads=10 full_census=0", native)) == ["1", "3"]
assert "mode=healthy-churn seen=4 expected=4 complete=true" in native
for mode in ("undiscovered-churn", "barrier-churn", "malformed", "lost-read"):
    assert re.search(r"NATIVE_OWNER_SCAN mode=" + mode + r" .*complete=false", native)
mutations = json.loads((here / "mutations.json").read_text())
assert [m["exit"] for m in mutations] == [1, 1]
for name, pattern, count in (("omit-count", r"--- FAIL: TestNativeGraphOwnerScanSequenceChurnCompleteness/", 1),
                             ("omit-duplicates", r"--- FAIL: TestGraphCompactionOwnerScanCompletenessAndBounds/", 2)):
    log = (here / (name + ".log")).read_text()
    assert "[build failed]" not in log and "panic:" not in log
    assert len(re.findall(pattern, log)) == count
worker = (here / "worker-race.log").read_text()
assert len(re.findall(r"--- PASS: TestNativeGraphIndexedStoredRenewalFreshWorkerRecovery/R[13]-domain ", worker)) == 2
assert len(re.findall(r"owner_census=\[1 1 0\] full_census=0", worker)) == 2
assert len(re.findall(r"--- PASS: TestPinnedRegressionCorpus/", (here / "pinned.log").read_text())) == 853
result = dict(scan_model_cases=12, prior_controls=35, native_churn_cases=5,
              native_restart_cases=2, worker_cases=2, pins=853, bypass_failures=[1, 2],
              large_setup_count=100008, setup_key_list_materialized=False,
              full_large_renewal_qualified=False, actual_100000_entries_qualified=False)
(here / "review.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
