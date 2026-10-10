#!/usr/bin/env python3
"""Review the frozen latency experiment; not a production acceptance gate."""
import gzip
import hashlib
import json
from pathlib import Path
import re

here = Path(__file__).resolve().parent
repo = here.parents[2]
sources = json.loads((here / "sources.json").read_text())
for name, item in sources.items():
    frozen = gzip.decompress((here / item["archive"]).read_bytes())
    assert hashlib.sha256(frozen).hexdigest() == item["sha256"], name
    assert (repo / name).read_bytes() == frozen, name

race = (here / "race.log").read_text()
assert re.search(r"^ok\s+js-wf/internal/graphpublication\s+", race, re.M)
rows = re.findall(r"RENEWAL_LATENCY seed=(\d+) foreign=(\d+) latency=(\S+) elapsed=(\S+) scopes=(\d+) examined=(\d+) renewed=(\d+) roots=(\d+) blobs=(\d+) writes=(\d+) census=(\d+) complete=(true|false) err=(.*)", race)
assert len(rows) == 9
seen = set()
for seed, foreign, latency, elapsed, scopes, examined, renewed, roots, blobs, writes, census, complete, err in rows:
    seen.add((int(seed), int(foreign), latency))
    assert int(census) == 1
    assert int(scopes) == 54 + int(foreign)
    if int(foreign) and latency == "1ms":
        assert complete == "false" and err == "blob publication revoked"
        assert 0 < int(examined) < int(scopes)
    else:
        assert complete == "true" and err == "<nil>"
        assert int(examined) == int(scopes)
        assert int(renewed) == int(writes) == 20
assert seen == {(s, f, l) for s in (2, 5, 42) for f, l in ((0, "1ms"), (100000, "1ms"), (100000, "10µs"))}
fences = (here / "fences.log").read_text()
assert re.search(r"^ok\s+js-wf/internal/graphpublication\s+", fences, re.M)
assert len(re.findall(r"--- PASS: TestGraphCompactionIntentRenewalScopesAndFences/", fences)) == 17
result = dict(latency_cases=9, seeds=[2, 5, 42], safety_controls=17,
              reproduced_namespace_expiry=True, production_changed=False,
              actual_100000_entries_qualified=False)
(here / "review.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
