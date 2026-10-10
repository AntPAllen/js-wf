#!/usr/bin/env python3
"""Review this scoped discovery experiment, not native scale acceptance."""
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
race = (here / "race.log").read_text()
assert re.search(r"^ok\s+js-wf/internal/graphpublication\s+", race, re.M)
assert len(re.findall(r"--- PASS: TestGraphCompactionOwnerScopeDiscovery/", race)) == 11
assert len(re.findall(r"--- PASS: TestGraphCompactionIntentRenewalScopesAndFences/", race)) == 17
rows = re.findall(r"OWNER_DISCOVERY mode=(\S+) namespace=(\d+) indexed=(\d+) blobs=(\d+) writes=(\d+) elapsed=(\S+) complete=(true|false) err=(.*)", race)
assert len(rows) == 11
data = {r[0]: r[1:] for r in rows}
assert data["normal"][0:5] == ("54", "20", "20", "20", "95.309753ms")
assert data["foreign100000"][0:5] == ("100054", "20", "20", "20", "95.309753ms")
for mode in ("abandoned", "unknown-create"):
    assert data[mode][1:4] == ("21", "21", "21")
assert data["absent-reservation"][1:4] == ("21", "21", "20")
for mode in ("normal", "foreign100000", "abandoned", "unknown-create", "absent-reservation", "fresh-adapter"):
    assert data[mode][-2:] == ("true", "<nil>")
for mode in ("unknown-enumeration", "duplicate", "foreign-entry", "invalid-key", "source-change"):
    assert data[mode][-2] == "false" and data[mode][-1] != "<nil>"
assert data["unknown-enumeration"][2:4] == ("0", "0")
assert json.loads((here / "bypass.json").read_text())["exit"] == 1
assert "--- FAIL: TestGraphCompactionOwnerScopeDiscovery/foreign100000" in (here / "bypass.log").read_text()
pins = (here / "pinned.log").read_text()
assert re.search(r"^ok\s+js-wf/sim\s+", pins, re.M)
count = len(re.findall(r"--- PASS: TestPinnedRegressionCorpus/", pins))
assert count == 853, count
result = dict(owner_discovery_controls=11, existing_safety_controls=17, pins=count,
              namespace_bypass_failures=1, native_index_implemented=False,
              actual_100000_entries_qualified=False)
(here / "review.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
