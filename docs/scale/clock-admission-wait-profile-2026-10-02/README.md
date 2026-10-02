# Clock-cut workload interval correction

The original failed native36969577235 artifact is retained under
../tier3-independent-checkpoints-2026-10-02/behind-failed-native/.
Its actual batch32 receipts/operations replay through the unchanged admission
selector at2000 snapshots,5ms apart, starting05:44:58.188530631. None admits a
pending timer with750ms removal lead. Replay uses63 receipts/123 operations,
filters by observation time and changes no native record. This is component
replay, not acceptance of the failed native row or a claim about every batch.

The initial2s waits carried healthy physical hints before leaders moved. Later
shifted hints carried250ms waits, which cannot satisfy750ms lead. The admitted
clock profile now gives all eight waits2s, leaving later positive requests after
a healthy first hint. Ordinary non-admitted clock rows retain eight250ms waits.
The ten-second selection bound,750ms lead, actual suspended-tail reread, shifted
fresh-publication proof, real removal-before-due check and latency/drain gates
remain unchanged. This increases timer exposure rather than reducing its count;
future measurements must identify this workload profile.

A deterministic virtual-receipt regression runs256 seeded delays for each±60s
offset against the real selector. With a healthy initial hint and a six-second
repair/takeover gap, the old first-only profile has no eligible later wait; the
new profile admits one. All existing stale/unknown/ambiguous/late rejection
controls and this regression pass under race in1.380s. Native artifact replay
passes2.373s. The new sustained native row is still required.

Replay command:

    WF_CLOCK_ADMISSION_REPLAY_ROOT=/path/to/original/tier3-mixed-journal GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./integration -run '^TestMatrixClockAdmissionNativeBatch32Replay$' -count=1 -v
