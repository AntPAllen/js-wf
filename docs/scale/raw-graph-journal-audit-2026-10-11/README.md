# Raw canonical journal audit — 2026-10-11

`integrity.CheckGraphJournals` extends the independent raw reference walk with
canonical invocation sources, Start input bytes/pointer headers, generation,
contiguous logical journal indices across archive and live forests, epoch/worker
consistency, step request/completion order, terminal outcome/result ownership,
and exact terminal WF_STATE projection bytes. It shares serialization and the
existing independent journal accumulator; it calls no production graph reader.
`CheckNativeGraphJournals` reads the isolated namespace's complete WF_INV cohort
and rejects invocation/projection high-water changes as well as the underlying
reference audit's authority/object changes. The caller must enforce quiescence.

## Focused evidence

```sh
WF_RETENTION_SIGKILL=1 go test -race ./retention ./integrity \
  -run '^(TestNativeGraphRetentionSDKBoundarySIGKILL|TestRawGraphJournalGenerationsOutcomesAndArchive|TestRawGraphReferencesAndIndependentCorruptionControls|TestNativeRawGraphReferenceAuditQuiescentReaders)$' \
  -count=1 -v -timeout=12m
```

Actual completed command exit: 0. Retention: 133.178 s; integrity: 4.385 s.
The development source manifest was captured during the run and every recorded
hash was rechecked unchanged. The retention binary receipt records actual PID
203847, executable SHA256, and `-race=true`. This is focused development-worktree
evidence, not a clean-source complete qualification.

Four valid cases cover JSON/protobuf-v1 entries, each with whole journals and an
archived prefix. Sixteen controls reject wrong envelope generation, missing
entry ownership, index gaps, decreasing epochs, conflicting workers, completion
without request, overlapping requests, post-terminal entries, wrong terminal
generation, unowned results, source sequence/missing source, differing/missing
projection, Start token mismatch and archive count mismatch.

All six native R3 retention worker SIGKILL cases now finish with both raw
reference and journal receipts: four ordinary cuts audit one current terminal
journal (six entries) plus one retired root; two ID reuse cases audit two
terminal journals (eight entries). All report pending=0. Existing four native
R1/R3 × ordinary/indexed reference controls and eleven reference corruptions
also pass. Both modified CI Python coverage guards were executed against the
actual combined log. Hosted CI execution is separate.

## Limits and remaining work

Checkpoint descriptors are opaque JSON here. The pure archived fixture uses a
minimal descriptor; it verifies logical prefix ordering, not a valid SDK frame
or checkpoint pointer. Protected readers receive reference audits, not semantic
replay. Signal binding/index contents, parent descriptor shape completeness,
orphan projection census, lease history, client linearizability, I4/I5, full
native scale census, and all-peer/disk persistence are unqualified. This is not
complete cursor schema validation or whole-phase acceptance. Retired roots must
represent completed purge; incomplete purge states can fail this audit.

The separately frozen f15a573 complete159 normal was independently accepted:
159,000 seed bodies, 860 saved traces, 226 top-level passes, eight disjoint package
processes, 3,522 Git-verified inputs, actual supervisor/child exit 0. Its receipts
are in ../graph-retention-workflow-replay-2026-10-10/. It predates this audit; no
current-source race or extended campaign is inferred from it.

The original actual 100,000-entry normal remains live under invocation
290b17b6df9e45629c853c10ed5cc772; PID185950 is verifying the second archive at
checkpoint index99993. The index is the fixed checkpoint position, not a progress
counter. Completion, independent review and race remain required.

## Current-source complete159 campaign launched

The normal1000 eight-part campaign now runs frozen source
00eeb1306ed0264a7f50e6eb3014b4a58780aebb under user service
`js-wf-current159-tier1-normal-20261011-qualified.service`, invocation
730878de8a6b43ff99e1f7c8d1c0fdc1, supervisor204393/driver204416. The launch
receipt binds these live processes, the supervisor hash, one-core CPU quota,
2 GiB memory limit and RemainAfterExit=yes. Race requires independently accepted
normal at exactly the same source. No result is claimed from launch.

The 102 MiB sparse checkout preserves all 3,532 required tracked Go/Python/YAML,
module and trace inputs. Bulky documentation evidence is omitted; the independent
reviewer still verifies the complete required input set against Git blob bytes.
Initial empty-index sparse preparation failed its missing-input preflight before
any test launch; populating the index resolved it, and the checkout was clean.

## Frozen complete159 normal accepted; race launched

Independent review accepts frozen source `00eeb1306ed0264a7f50e6eb3014b4a58780aebb`:
3,532 Git-verified inputs, 159,000 completed seeded bodies, 860 saved traces,
226 top-level passes and eight actual package completions. Supervisor and driver
closed with exit 0; the loaded normal user unit has MainPID=0, RemainAfterExit=yes
and the exact original invocation. See normal-state.json and normal-review.json.

The matching race stage is now running under
`js-wf-current159-tier1-race-20261011-qualified.service`, invocation
`f659459b2143405780de06e6d65159ee`, supervisor211216/driver211240. Launch checks
independently accepted normal at the same source and its actual terminal unit.
The receipt preserves one-core CPU quota, 2 GiB memory limit, eight sequential
parts, 1,000 seeds per family and the original watchdogs. Race acceptance remains
open until actual completion and independent review. This frozen source predates
subsequent checkpoint/metadata/signal audits; it is not latest-main qualification.

Earlier live-run observations above are historical; the original unclassified
100000-entry normal has since closed and failed full acceptance on 41 unclassified
CASRoot errors. The separate classified 9189259 campaign remains live.
