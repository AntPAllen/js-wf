# Verified peer-only clock smoke: behind

[Run36935069706](https://github.com/AntPAllen/js-wf/actions/runs/36935069706) succeeded at `a1e9387029a554eddcd5d52fed0545ef774aa714`. Original artifact guards and both event reviewers pass again on the download. Each sign has224 completions,2,468 journal entries,one journal-leader kill,15 actual clock readings and192 joined controller timer origins. All mixed histories,invariants,conservative p99 and physical drain pass.

Coverage limitation: server4 was actually shifted by60s in this direction, but timer lookups came from unshifted leaders. The killed journal node was0, rather than server4. This verifies peer skew and controller measurement, not deadline recovery across a skewed leader's clock-source change. It is excluded from the strengthened clock-role admission requirement and never counts as sustained or full-matrix release acceptance.

Behind test/package PASS98.45s/99.470s; worst terminal bound p99 7.368417632s and progress 7.870438551s. All27 repairs acknowledged (4start,11signal,12suspended). Zero typed fencing records match all worker counters. No server-side cause is inferred. Original downloaded server stores cannot be independently audited here. Large artifacts are losslessly compressed and hashes refer to original uncompressed bytes.
