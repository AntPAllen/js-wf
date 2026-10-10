#!/usr/bin/env python3
"""Accept only complete source-bound large renewal with two real exit records."""
import datetime
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess

here = Path(__file__).resolve().parent
repo = here.parents[2]
launch = json.loads((here / "launch.json").read_text())
child = json.loads((here / "process-state.json").read_text())
props = dict(line.split("=", 1) for line in subprocess.check_output(
    ["systemctl", "--user", "show", launch["unit"], "-p", "LoadState",
     "-p", "MainPID", "-p", "InvocationID", "-p", "ExecMainStatus",
     "-p", "ExecMainExitTimestamp", "-p", "ActiveState", "-p", "SubState"],
    text=True).splitlines())
result = dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              source_commit=launch["source_commit"], properties=props, child=child,
              accepted=False, actual_100000_entries_qualified=False)
def save():
    (here / "review.json").write_text(json.dumps(result, indent=2) + "\n")
save()
assert props["LoadState"] == "loaded" and props["InvocationID"] == launch["properties"]["InvocationID"]
if not child["terminal"] or int(props["MainPID"]) != 0 or not props["ExecMainExitTimestamp"]:
    print(json.dumps(result))
    raise SystemExit(0)
assert child["go_exit"] == 0 and props["ExecMainStatus"] == "0"
assert child["configuration"] == {"WF_GRAPH_NATIVE_OWNED_GRANTS": "100000", "WF_GRAPH_NATIVE_COMPACTION_LIFETIME": "3h"}
for name, item in json.loads((here / "sources.json").read_text()).items():
    frozen = gzip.decompress((here / item["archive"]).read_bytes())
    assert hashlib.sha256(frozen).hexdigest() == item["sha256"], name
    assert (repo / name).read_bytes() == frozen, name
data = (here / "native-race.log").read_text()
assert re.search(r"^ok\s+js-wf/internal/graphpublication\s+", data, re.M)
assert "--- FAIL:" not in data and "WARNING: DATA RACE" not in data
assert len(re.findall(r"--- PASS: TestNativeGraphOwnedGrantRenewalCostAndRecovery/R1 ", data)) == 1
rows = re.findall(r"NATIVE_OWNED_COST (.*)", data)
assert len(rows) == 1
r = dict(p.split("=", 1) for p in rows[0].split())
for key, value in {"replicas": "1", "orphan_grants": "100000", "initial_grants": "6",
                   "total_grants": "100006", "examined": "100007", "renewed": "100006",
                   "partial": "128", "batches": "782", "blob_writes": "100006",
                   "restarted": "all_peers", "lost_ack": "committed",
                   "portable_input": "true", "actual_100000_entries": "false"}.items():
    assert r[key] == value, (key, r)
def seconds(value):
    factors = {"h": 3600, "m": 60, "s": 1, "ms": .001, "µs": .000001, "ns": .000000001}
    parts = re.findall(r"(\d+(?:\.\d+)?)(ms|µs|ns|h|m|s)", value)
    assert "".join(n + u for n, u in parts) == value
    return sum(float(n) * factors[u] for n, u in parts)
assert seconds(r["max_batch"]) < 15
assert seconds(r["recovery_renewal_wall"]) < min(3600, seconds(r["original_remaining"]))
result.update(accepted=True, row=r, log_sha256=hashlib.sha256(data.encode()).hexdigest())
save()
print(json.dumps(result))
