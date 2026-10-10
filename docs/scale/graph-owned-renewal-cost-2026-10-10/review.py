#!/usr/bin/env python3
"""Review modeled owned-renewal cost; not native full-entry acceptance."""
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
race = (here / "race.log").read_text()
assert re.search(r"^ok\s+js-wf/internal/graphpublication\s+", race, re.M)
assert len(re.findall(r"--- PASS: TestGraphCompactionOwnedRenewalCost/", race)) == 11
rows = re.findall(r"OWNED_RENEWAL (.*)", race)
assert len(rows) == 11
seen = set()
for row in rows:
    d = dict(part.split("=", 1) for part in row.split(" err=", 1)[0].split())
    err = row.split(" err=", 1)[1]
    seed, scopes = int(d["seed"]), int(d["scopes"])
    seen.add((seed, scopes, d["latency"]))
    assert int(d["old_expiry"]) + int(d["new_expiry"]) == scopes
    assert int(d["renewed"]) == int(d["new_expiry"]) == int(d["writes"])
    assert int(d["examined"]) <= int(d["discovered"]) <= scopes
    if scopes > 1020 and d["latency"] == "1ms":
        assert d["complete"] == "false" and err == "blob publication revoked"
        assert 0 < int(d["renewed"]) < scopes and int(d["old_expiry"]) > 0
    else:
        assert d["complete"] == "true" and err == "<nil>"
        assert int(d["discovered"]) == int(d["examined"]) == int(d["renewed"]) == scopes
expected = {(s, n, l) for s in (2, 5, 42)
            for n, l in ((1020, "1ms"), (10020, "1ms"), (10020, "10µs"))}
expected |= {(42, 100020, "1ms"), (42, 100020, "10µs")}
assert seen == expected
fences = (here / "fences.log").read_text()
assert re.search(r"^ok\s+js-wf/internal/graphpublication\s+", fences, re.M)
assert len(re.findall(r"--- PASS: TestGraphCompactionIntentRenewalScopesAndFences/", fences)) == 17
result = dict(owned_cost_cases=11, safety_controls=17, seeds=[2, 5, 42],
              reproduced_owned_expiry=True, production_changed=False,
              native_total_renewal_qualified=False,
              actual_100000_entries_qualified=False)
(here / "review.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
