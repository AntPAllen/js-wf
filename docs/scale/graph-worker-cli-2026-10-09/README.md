# Canonical graph worker CLI integration — 2026-10-09

## Selection and deployment

The worker can explicitly select the experimental canonical Start/Signal runtime:

```sh
wf-worker -id owner-a -handler-plugin ./handlers.so \
  -timer-backend native -replicas 3 \
  -graph-authority-stream WORKFLOW_GRAPH_AUTH \
  -graph-authority-prefix wf.graph.workflow \
  -graph-object-bucket WORKFLOW_GRAPH_OBJECTS
```

Provision new isolated graph stores explicitly using `journal.NativeGraphStreamConfigs` and `CreateStream`. Worker admission opens existing stores and requires the selected replica count; it does not create graph storage, import legacy data or change an existing namespace. All three graph flags are required together. Every SDK client and worker for these workflows must select the same `journal.NativeGraphConfig`, with `CanonicalStarts` and `CanonicalSignals` both true. External reads must use `client.NewWithGraphJournal`. The separate `wf` client CLI still needs graph configuration support.

`-journal-encoding` selects graph entry encoding in this mode. It does not install the conflicting legacy journal option. JSON remains the default; the native fixture uses protobuf to verify the graph option reaches the worker. `-domain`, assignment modes, concurrency, timer clock, metrics and event logging remain available. Native timers require an explicitly selected, fully upgraded cluster. Fallback timers and the legacy `-retention-type` handler are rejected. Continuation plugins remain rejected by graph worker admission until their migration is implemented.

Reconciliation selects `graph-start`, `graph-signal`, `graph-terminal`, graph-aware `timer` and graph-aware `suspended` loops, with their existing fenced leases/checkpoints. It does not start the legacy tombstone loop for graph workflows. Production online collection stays disabled; use the existing graph-aware library retention path while its deployment integration remains open.

## Integration fixes and evidence

A source invocation can be visible before its canonical binding. The old timer/suspended history reader reports `ErrStale`, which previously stopped the process. Graph reconciliation now wraps that scheduling uncertainty as `ErrUnknown` while preserving `ErrStale`; the confirmed cursor prefix is retained and a fresh scan retries. It never treats the mismatched generation as empty history or falls back to a legacy journal. Both scanners have a deterministic published/unbound/bound control; the original helper fails both counterfactual cases.

Record inspection of an empty journal now reads only its validated quorum cursor, retaining the sequence base without writing a reader pin. It reads no journal objects. Public payload views still pin owned inputs at journal count zero. This removes avoidable interference with the first append. The empty-history control explicitly checks both behaviors.

Native graph timer/suspended scans additionally defer history pins while a delivery lease exists. This is a scheduling hint, not journal or payload authority. Release/expiry makes a later pass eligible; uncertain or malformed observations preserve the scan prefix for retry. Canonical Signal discovery continues. Deterministic controls verify no graph reads or pins while blocked, unknown observations cannot certify progress, and released deliveries resume pinned inspection. Public views and worker fencing retain their original authority checks.

The initial native normal run fails on the transient generation mismatch. `domain-diagnostic.jsonl` identifies the timer loop. The subsequent `retry-only-cli-normal.jsonl` and `empty-cli-race-without-lease-hint.jsonl` retain append-contention failures at a 100 ms scan cadence. The latter ended FAIL and remains retained. `empty-cli-normal.jsonl` passes both native cases before the scheduling hint; final controls are retained separately.

Ten pins are migrated. For `graph-reconcile-start_initialized.json`, seed/decisions/non-transport fields are unchanged; reader acquire/release disappears, changing dependent authority heads, tokens, receipts and collection observations. `pin-lineage.json` retains old/new hashes and event counts. Nine bound Start repair pins gain actual recovery dispatch/consumption instead of suppressed duplicate publications, with identical seeds/decisions/non-transport fields. `start-pin-lineage.json` retains their hashes. The other 738 pins remain byte-identical to `712a8d7`. Stale/retired graph model assertions now require both error identities and the unchanged retry boundary, rather than equality to a single unwrapped error.

Bound Start recovery now also bypasses enqueue deduplication after validating the captured token, invocation and source pointer. The deterministic old-code control reports an acknowledged repair without a fresh dispatch. The native timer CLI fixture deletes the original queue message inside its dedup window, then requires recovery and native timer completion; old normal/race failures remain in `timer-cli-before-dedup-*.jsonl`. Original Start request idempotency and source publication remain unchanged. Recovery may publish duplicates, which retain the worker's exact binding and lease/journal fencing.

## Scope

These are component controls, not complete current qualification. The accepted full normal 148-family result at `5aafc29` excludes these later changes. Complete current normal/race and extended suites, fallback timers, continuation/snapshots/import, state/lifecycle atomic fencing, present corrupt projection repair, client CLI/deployment/retention integration, native fault/scale/matrices, actual 24h soak, million physical drain, default adoption and release remain required.

## Final executed component review

`executed-review.py` checks final native CLI controls in both modes (including R1/R3 domain routing, timer completion and repeated terminal restoration), deterministic graph repair controls, twelve graph journal race groups, both affected shared families with all 1,000 contiguous seed bodies and exact replay, and all 748 pins in normal/race. It checks ten pin lineages and the other 738 pins against Git. Selected Go/configuration input observations are unchanged during the final CLI race run; nine trace migrations occurred during that run and are explicitly recorded because CLI tests do not read the corpus. This is development evidence, not a pre-compilation frozen full-suite qualification.

## Frozen full-suite qualification in progress

The complete normal and race default suites run at frozen `9a1ccdc` in `/home/exedev/js-wf-worker-cli-qualification`. Roots: `/home/exedev/js-wf-tier1-full148-cli-normal1000-20261009` and `/home/exedev/js-wf-tier1-full148-cli-race1000-20261009`. Both require every current family/pin. Each supervisor starts in its own process session. Terminal evidence and independent review remain pending.
