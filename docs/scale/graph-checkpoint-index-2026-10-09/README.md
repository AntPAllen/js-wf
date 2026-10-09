# Versioned canonical checkpoint index — 2026-10-09

Explicit `GraphConfig.CheckpointIndex` or `NativeGraphConfig.CheckpointIndex`
selects the v5 canonical application cursor. It requires canonical Start and
Signal storage and new isolated namespaces. Default configurations keep their
existing cursor version. v4 readers reject v5 history; opening a namespace does
not import or downgrade it. CLI selection/deployment for v5 remains open.

`PublishCheckpoint` confirms the owned frame, exact pointer/tail and reader
release, then publishes the completion and request indices under the same root
CAS as generation/history metadata. Concurrent journal changes fence the write.
The underlying publication layer can confirm a lost acknowledgement by exact
quorum readback; failed readback or an uncommitted write stays uncertain. A fresh
retry of the same pointer is idempotent. Pointer publication does not increment
the logical journal tail.

`GraphView.ReadCheckpoint` validates the indexed request/completion pair and
owned frame, then discovers newer completed boundaries in the captured suffix.
It rejects invalid pointers rather than treating them as initial replay. The
request index is recorded separately: recovery can insert runtime entries
between a request and completion. Earlier pins retain their earlier cursor.

The internal continuation publisher selects this publication path when the
store explicitly enables the index. **Public continuation admission remains
closed.** Full history is still retained. Only checkpoint lookup avoids prefix
entry reads: `openGraphDelivery` still loads history/payload references, so this
does not establish bounded overall worker memory or archival prefix compaction.
Materialized payload ownership through collection, autonomous resume discovery,
audit/offline replay, process-kill/limit qualification and deployment remain open.

## Executed development evidence

- `final-model-race.log`: five seeded publication cases pass: ordinary,
  lost ack with confirmed readback, lost ack with failed readback, drop before
  commit, and an append racing pointer CAS. Each publishes/retries the exact
  pointer, blocks all 129 encoded prefix records, and requires indexed frame/
  suffix lookup and idempotent retry to succeed. The earlier pin cannot bypass
  those blocked bytes, and a v4 reader rejects v5. Normal-case mutations of SDK
  position and request index fail closed. Missing canonical store configuration
  is rejected. Twenty existing JSON/protobuf checkpoint cases also pass.
- `final-race.log`: the preceding model cases and native R1/R3-domain continuation
  stage controls pass, with v5 enabled in the fixture. The stage controls retain
  handoff/lease/retry/admission checks, large state/result restoration, one effect,
  terminal duplicate behavior and zero legacy journal writes. Existing legacy
  continuation global-limit/terminal-slot behavior also passes.
- `legacy-cursor-race.log`: existing canonical Start pending/binding/bounds and
  Signal indexed identity/reopen, retirement/replacement and schema/expiry
  component regressions pass with their original cursor selection.

`final-allocation-race.log` repeats the normal indexed case after removing a
prefix-sized scratch allocation from the indexed path.

The initial model compiler failure used a nonexistent fault constant and is
retained in `initial-model-compile.log`. The next model run incorrectly required
uncertainty even when the transport confirmed the lost CAS by quorum readback;
`initial-model-lost-ack-assertion.log` retains that failure. The corrected tests
require success only with confirmation and explicitly reject success when its
readback fails. `initial-race.log` retains the first v5 native stage run.

These tests ran in the development checkout; source hashes are observed at review,
not a frozen compilation snapshot. Prefix-read checks establish this component,
not a full tier-1 family/extended campaign or the original snapshot/purge scale
gate. The live frozen full race at `9a1ccdc` excludes v5. Every original remaining
full/current/extended/runtime/native/scale/actual24h/physical-drain/adoption/release
requirement remains open.
