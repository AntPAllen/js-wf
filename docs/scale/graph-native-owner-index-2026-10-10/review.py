#!/usr/bin/env python3
"""Review this native index experiment; not full-scale acceptance."""
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
counts = {"TestOwnerIndexedNativeNamespaceAdmission": 4,
          "TestNativeGraphOwnerScopeIndexRecovery": 2,
          "TestGraphCompactionOwnerScopeDiscovery": 11,
          "TestGraphCompactionIntentRenewalScopesAndFences": 17,
          "TestNativeGraphAuthorityPersistenceAndSchemaIsolation": 2,
          "TestNativeGraphMutationAcrossReadWitnesses": 8}
for name, count in counts.items():
    if name == "TestNativeGraphMutationAcrossReadWitnesses":
        pattern = r"--- PASS: " + name + r"/R[13]/(?:root|blob)/(?:witness|replacement) "
    else:
        pattern = r"--- PASS: " + name + r"/[^\s/]+ "
    assert len(re.findall(pattern, race)) == count, name
rows = re.findall(r"NATIVE_OWNER_INDEX replicas=([13]) discovered=9 blob_reads=9 full_census=0 orphan_uploads=2 absent_reservations=1 restart=all_peers", race)
assert sorted(rows) == ["1", "3"]
bypass = (here / "bypass.log").read_text()
assert json.loads((here / "bypass.json").read_text())["exit"] == 1
assert "[build failed]" not in bypass
assert len(re.findall(r"--- FAIL: TestNativeGraphOwnerScopeIndexRecovery/R[13] ", bypass)) == 2
pins = (here / "pinned.log").read_text()
assert re.search(r"^ok\s+js-wf/sim\s+", pins, re.M)
assert len(re.findall(r"--- PASS: TestPinnedRegressionCorpus/", pins)) == 853
result = dict(cases=counts, pins=853, registration_bypass_failures=2,
              native_domains=["R1", "R3"], owned_scopes=9, unrelated_scopes=16,
              indexed_worker_wiring=False, large_owned_census_qualified=False,
              actual_100000_entries_qualified=False)
(here / "review.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
