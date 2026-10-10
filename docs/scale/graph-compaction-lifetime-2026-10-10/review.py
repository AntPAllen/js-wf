#!/usr/bin/env python3
"""Review scoped lifetime evidence, without native scale acceptance."""
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
def log(name, package):
    data = (here / name).read_text()
    assert re.search(r"^ok\s+js-wf/" + package + r"\s+", data, re.M), name
    assert "--- FAIL:" not in data and "WARNING: DATA RACE" not in data, name
    return data
race = log("race.log", "journal")
for test, count in (("TestGraphCompactionSeparateIntentLifetime", 8),
                    ("TestGraphCompactionCheckpointBindingRenewalAndResumption", 40),
                    ("TestGraphCompactionStoredCheckpointCASAndRecovery", 36)):
    assert len(re.findall(r"--- PASS: " + test + r"/", race)) == count
assert "--- PASS: TestGraphCompactionNegativeIntentLifetime " in race
assert "--- PASS: TestNativeGraphCompactionLifetimeForwarding " in log("native-race.log", "journal")
restored = log("restored-race.log", "journal")
assert len(re.findall(r"--- PASS: TestGraphCompactionSeparateIntentLifetime/", restored)) == 8
assert "--- PASS: TestNativeGraphCompactionLifetimeForwarding " in restored
assert json.loads((here / "mutation.json").read_text())["exit"] == 1
mutation = (here / "append-ttl-bypass.log").read_text()
assert len(re.findall(r"--- FAIL: TestGraphCompactionSeparateIntentLifetime/", mutation)) == 4
assert "[build failed]" not in mutation and "panic:" not in mutation
worker = log("worker-race.log", "worker")
assert len(re.findall(r"--- PASS: TestNativeGraphIndexedStoredRenewalFreshWorkerRecovery/R[13]-domain ", worker)) == 2
assert len(re.findall(r"--- PASS: TestPinnedRegressionCorpus/", log("pinned.log", "sim"))) == 853
cost = log("cost-race.log", "internal/graphpublication")
assert "OWNED_RENEWAL seed=42 scopes=100020 latency=1ms elapsed=11m42.104752496s" in cost
assert "discovered=100020 examined=100020 renewed=100020 old_expiry=0 new_expiry=100020" in cost
assert "complete=true err=<nil>" in cost
result = dict(lifetime_cases=8, prior_recovery_cases=76, native_forwarding=True,
              mutation_failures=4, restored_worker_cases=2, pins=853,
              modeled_large_budget_sufficient=True,
              native_total_renewal_qualified=False, actual_100000_entries_qualified=False)
(here / "review.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
