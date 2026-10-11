# Worker SDK envelope admission — 2026-10-11

Worker.execute now checks every retained StepRequested/StepCompleted envelope
with production stepwire before terminal repair, signal draining or handler
admission on this execution path. This protects user code before per-step SDK
replay checking. Legacy journal, canonical graph, and resumed suffix records use
the check; graph recovery paths that finish maintenance without execute admission
are outside this change. The independent raw auditor remains separate.

## Worker controls

The test uses actual production worker dispatch/client/journal/lease decisions
with in-memory transports. Both legacy and canonical journals retain Started,
request and completion records. Canonical starts own the real input edge.

Each backend has two positive controls (ordinary result and opaque JSON with
null, unknown keys and duplicate user keys) and28 null-metadata negatives:
11 request fields, seven case fields and ten completion fields. Negative
controls require zero handler calls, zero SDK effects and unchanged journal
length after a failed delivery. Positive controls require one handler and zero
SDK callbacks because the result is already journaled. Canonical controls also
check physical reference consistency. This is60 worker controls, not60 new
seeded families or evidence of native transport fault behavior.

Baseline a7bfa9c with the final test exits1: all56 malformed envelopes enter the
handler, while all four positives pass. Original production source, baseline
command and logs are retained. Baseline executable observation was missed.

Final normal/race logs pass the exact current worker CI assertion block, including
all60 names. All actual final normal/race/native executable commands, hashes and
build metadata are captured. The three commands' Go/module/workflow inputs match
the prelaunch source manifest. Hosted CI execution is not claimed.

Native R1/R3 child and buffered-child continuation flows run with independent raw
SDK auditing enabled. Review requires four successful start/terminal witness
pairs and eight checkpoint frames. These are compatibility checks, not native
corrupt-envelope injection or whole worker suite qualification. Their ephemeral
stores do not establish fixture restoration or power-loss safety.

## Preserved setup and observer failures

- setup-compile-failure.log: GraphView.Read called with the wrong signature.
- setup-stop-condition-aborted.log: test only stopped on NAK, while the buggy
  worker could acknowledge a failed invocation. Actual child254433 was terminated
  after confirming it live; this aborted run is not a semantic negative proof.
- setup-missing-input-edge.log: canonical fixture omitted the owned input edge,
  so its healthy controls failed and its negatives were not meaningful.
- observer-mismatch-normal-* records a successful initial normal command whose
  observer captured the concurrent native race executable. That binary binding
  is rejected. The observer now requires the exact admission selector; the final
  normal command was repeated to resolve this concrete provenance failure.

## Review

```sh
python3 docs/scale/worker-step-admission-2026-10-11/review.py
```

The corpus remains162 families/922 pins. Five older campaigns were revalidated
active. Full current qualification, original I4 program/causal equivalence,
complete record ordering/kind semantics and original fault/scale/soak/power/
client/lease/rollout requirements remain open. Public admission/import/online
collection remain disabled.
