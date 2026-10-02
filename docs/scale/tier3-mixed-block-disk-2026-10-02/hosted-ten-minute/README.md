# Independently verified ten-minute I/O row

[Hosted run 36949600724](https://github.com/AntPAllen/js-wf/actions/runs/36949600724),
source `e1712410ba1fb4f284df27bf151281d69c8652a5`, passes the native ten-minute R5 mixed row: 2800 terminal
invocations, 30867 journal entries and 19 confirmed faults. Current independent
row, event and fencing reviewers pass. Worst per-type terminal/progress p99 are
5.570849744s / 0.299056206s. All 88 repair records are acknowledged; no fences
are observed. Histories, retained invariants, immutable outcomes and physical
queue drain pass. The artifact guard verifies actual five-second dm-suspend stalls; dirty sync overlaps suspension until resume begins.

All 94 original artifact/API/log/review files are retained as gzip.
`original-sha256.json` records uncompressed hashes and sizes; every retained
file was decompressed and verified before committing. To repeat independent
review, decompress the tree and run:

```sh
python3 scripts/check-tier3-journal-row.py --root ARTIFACTS/tier3-mixed-journal/tier3-mixed-journal --row block_disk --events ARTIFACTS/tier3-mixed-journal/tier3-mixed-journal-events.jsonl --duration 10m --output result.json
python3 scripts/explain-tier3-events.py --root ARTIFACTS/tier3-mixed-journal/tier3-mixed-journal --output events.json
python3 scripts/review-tier3-fencing.py --root ARTIFACTS/tier3-mixed-journal/tier3-mixed-journal --output fencing.json
```

This clears one sustained seed for this individual row, not 200 seeds, combined
faults or the full five-node 24-hour release matrix. Source predates common timer
clock integration; it is not final-source acceptance.
