# Child signal consistency before checkpointing — 2026-10-11

Every consumed child annotation is now matched to a preceding parent call/call_async
request at consumption. Requested child arrivals cannot omit their annotation.
Type/ID/generation and result-reference/hash pairs are checked independently.
Owned signal bodies must match their declared hash and cannot coexist with inline
body bytes. The body's strict outcome metadata must match the annotated child
generation and result descriptor. Failed/result and rejected-limit metadata rules
also apply. External child results require their own journal-owned grant and
bounded hash-verified bytes. These checks run even without a later checkpoint or
promise cache. Ordinary unrequested signals without child annotations stay valid.

This closes local parent-call/annotation/owned-outcome consistency. It does not
prove the original child publication, parent-start linkage, lease history or old
generation source history after retirement/ID reuse.

## Evidence

Baseline7d035fb (runtime identical to004e339) plus the new fixture/test:

```sh
go test ./integrity -run '^TestRawGraphChildSignalHistory$/^json$/^outcome-generation$' -count=1 -v
```

Actual exit1: an owned outcome from generation4 was accepted with a child
annotation for generation3 in a parent history with no checkpoint.

```sh
python3 docs/scale/raw-child-signal-history-2026-10-11/run.py
```

Actual exit0 in31.465s for all37 TestRawGraph top-level tests plus four native
checkpoint/orphan/tombstone flows.34 new physical JSON/protobuf controls comprise
6 positives (inline, failed, external child outcome) and28 negatives: missing
parent declaration/child annotation, wrong type/id/zero generation, outcome
generation mismatch, hash-only child, failed result, null error, unknown outcome,
changed external reference, missing/wrong external bytes and inline-plus-owned
signal body. Fixtures use the production GraphStore writer on modeled ports for
real reserve/bind/append grants, then capture raw roots/fences/objects for the
independent auditor. They contain four parent entries and no checkpoint. Every
negative first passes CheckGraphReferences; failures are semantic contradictions
in physically coherent storage. The fixture does not create a real child source,
so it is not original publication/causal-history proof. No user effects run.

Additional native command:

```sh
python3 docs/scale/raw-child-signal-history-2026-10-11/native-run.py
```

It selects R1/R3 child/buffered-child SDK flows with WF_GRAPH_SDK_RAW_AUDIT=1.
Actual command, executable, exit and receipts are retained in native-command.json,
native-executable.json/native.log; review.json records final runtime/scope.
430 source inputs were hashed before both commands. Both actual test executables
were captured. The extracted34-case CI guard passed locally and YAML parses;
hosted CI is not claimed. Native media are ephemeral and cleaned by tests, so
no storage restoration or physical-power proof is claimed.

Full child/parent source causality, retired history, lease/clock/power, client
linearizability and original fault/scale/soak/rollout gates remain open. Four long
frozen campaigns predate this code. Full current161-family/894-pin qualification
remains outstanding; accepted frozen160 normal remains source-bound.
