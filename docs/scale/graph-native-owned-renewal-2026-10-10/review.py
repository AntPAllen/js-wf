#!/usr/bin/env python3
"""Review native 1,000-orphan renewal only; not full-entry acceptance."""
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
data = (here / "native-race.log").read_text()
assert re.search(r"^ok\s+js-wf/internal/graphpublication\s+", data, re.M)
assert "--- FAIL:" not in data and "WARNING: DATA RACE" not in data
assert len(re.findall(r"--- PASS: TestNativeGraphOwnedGrantRenewalCostAndRecovery/R[13] ", data)) == 2
def seconds(value):
    factors = {"h": 3600, "m": 60, "s": 1, "ms": .001, "µs": .000001, "ns": .000000001}
    parts = re.findall(r"(\d+(?:\.\d+)?)(ms|µs|ns|h|m|s)", value)
    assert "".join(n + u for n, u in parts) == value
    return sum(float(n) * factors[u] for n, u in parts)
rows = [dict(p.split("=", 1) for p in line.split())
        for line in re.findall(r"NATIVE_OWNED_COST (.*)", data)]
assert len(rows) == 2 and {r["replicas"] for r in rows} == {"1", "3"}
for r in rows:
    for key, expected in {"orphan_grants": "1000", "initial_grants": "6",
                          "total_grants": "1006", "examined": "1007",
                          "renewed": "1006", "partial": "128", "batches": "8",
                          "restarted": "all_peers", "lost_ack": "committed",
                          "portable_input": "true", "actual_100000_entries": "false"}.items():
        assert r[key] == expected, (key, r)
    assert seconds(r["recovery_renewal_wall"]) < min(1200, seconds(r["original_remaining"]))
    assert seconds(r["max_batch"]) < 15
    assert int(r["blob_writes"]) == 1006
    assert int(r["marker_witnesses"]) >= 1136
result = dict(native_recovery_cases=2, total_grants_per_case=1006,
              rows=rows, full_namespace_fallback=False,
              native_100000_grants_qualified=False, actual_100000_entries_qualified=False)
(here / "review.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
