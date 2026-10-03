# Paged tombstone marker drain — 2026-10-03

Production `KV.Delete` acknowledges a logical deletion by writing a retained
`DEL` message. History 1 does not remove that last marker. The baseline scanner
skipped it because KV Get returned key-not-found. With 32 expired tombstones and
one protected result, three complete paged passes retained 33 physical messages
and subjects (4,354 bytes), despite all expired keys being absent.

## Correction and safety

The scanner recognizes valid identity subjects with raw `KV-Operation: DEL` or
`PURGE`. It calls the real stream purge API with both that exact subject and
`WithPurgeSequence(observedSequence + 1)`. The boundary is exclusive: later
revisions cannot be removed. Reserved cursor subjects remain excluded, dry runs
never purge, and unknown operation headers fail closed. An uncertain purge
stays at its failing sequence using the existing prefix certificate; a retry
can confirm an absent marker. Marker removals have a separate page counter and
contribute to the production loop removal count.

An initial single-message delete attempt failed with API 10057 because KV
provisioning sets DenyDelete. Its original log is retained; provisioning and
production NATS versions were not changed.

## Evidence

- Original real baseline: 0.083 s, expected failure, 33 retained messages.
- Corrected physical drain: one protected message/subject, 85 bytes. Reusing a
  purged key through KV Create succeeds with a later revision.
- Fixed 128 seeds and exact replays, 12 pinned cells: DEL/PURGE ×
  ack/drop-before-commit/lost-ack-after-commit × absent/concurrent reuse.
  Dry run is checked before every apply. The model retains raw History=1
  markers, unlike the earlier partial-cursor model which treats them as holes.
- Baseline scan overlay with only an unused output-field compatibility addition:
  fixed seed 1 fails in 0.009 s (`marker cleanup never attempted`). The original
  less precise assertion failure is also retained.
- All 391 pins and the marker model pass race in 7.383 s.
- Six real three-node DEL/PURGE reuse cases: a fresh value is created after
  observing the marker and before purging it. Ack, drop and hidden-ack paths
  all preserve that exact value and remove the obsolete marker. Normal focused
  run 3.090 s; final race log is retained.
- Full retention/reconciler package runs: 3.670 s / 43.766 s.

The archive contains baseline and corrected sources, all raw logs, pins, and
scope metadata. Every archived member was reopened and SHA256 verified before
publication. Focused test binaries and physical stores were not retained; these
are bounded contract checks, not independently reopened physical stores,
complete latest-source qualification, population-independent cleanup bounds,
or the original million-timer/24h gate.

Reproduce the focused correction with:

```sh
go test -p=1 ./retention -run '^TestTombstone.*(Markers|Reuse)$' -count=1 -v
go test -p=1 -race ./sim -run '^(TestPinnedRegressionCorpus|TestTombstoneMarkerDrainReplay)$' -count=1
```

The archived overlay contains original local paths. Recreate its path mapping
against the checked-out repository and extracted `tombstone_scan.model-control.go.txt`
to replay the deliberately failing baseline model.
