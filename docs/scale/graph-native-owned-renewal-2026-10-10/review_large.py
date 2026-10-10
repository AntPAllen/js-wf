#!/usr/bin/env python3
"""Require terminal matching service and source before accepting a large run."""
import datetime
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess

here = Path(__file__).resolve().parent
repo = here.parents[2]
launch = json.loads((here / "owned10000-launch.json").read_text())
props = dict(line.split("=", 1) for line in subprocess.check_output(
    ["systemctl", "--user", "show", launch["unit"], "-p", "LoadState",
     "-p", "ActiveState", "-p", "SubState", "-p", "MainPID", "-p",
     "InvocationID", "-p", "ExecMainStatus", "-p", "ExecMainExitTimestamp"],
    text=True).splitlines())
assert props["LoadState"] == "loaded", "missing service is not terminal acceptance"
assert props["InvocationID"] == launch["launch_properties"]["InvocationID"], "service was replaced"
state = dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
             source_commit=launch["source_commit"], properties=props,
             terminal=int(props["MainPID"]) == 0 and bool(props["ExecMainExitTimestamp"]),
             accepted=False, native_100000_grants_qualified=False,
             actual_100000_entries_qualified=False)
if not state["terminal"]:
    (here / "owned10000-state.json").write_text(json.dumps(state, indent=2) + "\n")
    print(json.dumps(state))
    raise SystemExit(0)
data = (here / launch["log"]).read_text()
state["log_sha256"] = hashlib.sha256(data.encode()).hexdigest()
(here / "owned10000-state.json").write_text(json.dumps(state, indent=2) + "\n")
assert props["ExecMainStatus"] == "0", "terminal native run failed"
for name, item in json.loads((here / launch["source_manifest"]).read_text()).items():
    frozen = gzip.decompress((here / item["archive"]).read_bytes())
    assert hashlib.sha256(frozen).hexdigest() == item["sha256"], name
    assert (repo / name).read_bytes() == frozen, name
assert re.search(r"^ok\s+js-wf/internal/graphpublication\s+", data, re.M)
assert "--- FAIL:" not in data and "WARNING: DATA RACE" not in data
assert len(re.findall(r"--- PASS: TestNativeGraphOwnedGrantRenewalCostAndRecovery/R1 ", data)) == 1
rows = re.findall(r"NATIVE_OWNED_COST (.*)", data)
assert len(rows) == 1
r = dict(p.split("=", 1) for p in rows[0].split())
for key, expected in {"replicas": "1", "orphan_grants": "10000",
                      "initial_grants": "6", "total_grants": "10006",
                      "examined": "10007", "renewed": "10006",
                      "partial": "128", "batches": "79", "blob_writes": "10006",
                      "restarted": "all_peers", "lost_ack": "committed",
                      "portable_input": "true", "actual_100000_entries": "false"}.items():
    assert r[key] == expected, (key, r)
def seconds(value):
    factors = {"h": 3600, "m": 60, "s": 1, "ms": .001, "µs": .000001, "ns": .000000001}
    parts = re.findall(r"(\d+(?:\.\d+)?)(ms|µs|ns|h|m|s)", value)
    assert "".join(n + u for n, u in parts) == value
    return sum(float(n) * factors[u] for n, u in parts)
assert seconds(r["max_batch"]) < launch["batch_deadline_seconds"]
assert seconds(r["recovery_renewal_wall"]) < min(seconds(r["original_remaining"]), launch["renewal_budget_seconds"])
state.update(accepted=True, row=r)
(here / "owned10000-state.json").write_text(json.dumps(state, indent=2) + "\n")
print(json.dumps(state))
