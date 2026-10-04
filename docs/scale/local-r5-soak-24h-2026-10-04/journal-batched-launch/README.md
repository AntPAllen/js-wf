# Actual24h journal row with batched checkpoint/final audits: live launch

Persistent user service `js-wf-soak-journal-batched-24h-20261004.service` executes
exact `b287e983872e310d92bfb9dab263f0a01875a43b`, journal/seed1/race/24h with
both `--batched-retained-audit` and `--retained-audit-trace`. Producer runs from
an isolated source checkout and retains its actual SDK and original stores.

Launch command:

```bash
systemd-run --user --unit=js-wf-soak-journal-batched-24h-20261004 --property=WorkingDirectory=/home/exedev/js-wf --property=StandardOutput=append:/tmp/js-wf-soak-journal-batched-24h-20261004.log --property=StandardError=append:/tmp/js-wf-soak-journal-batched-24h-20261004.stderr --setenv=PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin --setenv=GOCACHE=/tmp/js-wf-go-build-cache-20261004 /usr/bin/python3 /home/exedev/js-wf/scripts/run-tier3-soak.py --root /tmp/js-wf-soak-journal-batched-24h-20261004 --row journal --duration 24h --seed 1 --retained-audit-trace --batched-retained-audit
```

Authoritative live snapshot: supervisor52069, launcher52360, actual SDK52384,
five Docker containers. Named test started. First batch10/20 audit traces show
both streams creating/deleting consumers and completing pull batches without
method errors. This verifies bulk-mode activation in actual checkpoint reads,
not terminal integrity or long-run capacity. Latest64 call records do not retain
early creation metadata; no replica-observation claim is made from those traces.

Original audit20s per attempt /60s total, fault schedule, workload mix and latency
gates stay unchanged. About37GiB free at launch; actual disk growth must be
monitored. Previous failed attempts remain terminal and preserved separately.
This is one real requested24h row, not a full-matrix qualification.

All launch/live files here are snapshots and become historical when a later
terminal result exists. Query the named unit/processes and producer execution
state before claiming it remains live. Actual SDK/source/original-member hashes
and final reports require independent terminal review. Original root:
`/tmp/js-wf-soak-journal-batched-24h-20261004`.
