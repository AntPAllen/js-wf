# Runtime terminal envelope admission — 2026-10-11

wf.DecodeOutcome uses complete typed strict admission and rejects present null
runtime fields, including limit-entry kind/payload. Generation, terminal kind,
required fields and program semantics remain the caller's responsibility.
Omitted legacy identity stays readable; encoded user result JSON and rejected
request/payload contents remain opaque. User null is encoded result bytes, not a
null runtime result field. Independent raw auditing retains its own decoder.

The graph terminal reader, offline replay terminal envelope and legacy held-lease
shortcut now use this shared admission. Graph clients/parent retention/canonical
held-terminal users inherit the graph reader check. This does not preflight the
entire journal in those fast paths, or add outcome admission to every separate
active-worker/maintenance decoder.

## Controls

Owned graph terminals are appended through GraphStore and read through a pinned
terminal view, in JSON and protobuf-v1. Tests require exact retained payload bytes
and consistent physical references before semantic admission. Actual legacy
worker dispatches run with a held modeled lease, matching source generation and
persisted outcome. Positive controls must ACK; negatives must NAK; no handler may
enter and no projection bytes may change. This is not parent-notification or
real-cluster fault injection.

Each path has21 cases: four positive inline/user-null/opaque-user-duplicate/
explicit-empty-error controls, plus17 duplicate/alias/escaped/unknown/null
metadata negatives. There are63 controls (12 positive/51 negative) across both
graph codecs and the legacy held path.

The focused baseline overlay restores exactly graph_outcome.go and
terminal_delivery.go from sourcec0ebe3e onto final fixtures/current remaining
code. This is a two-decoder semantic control, not a full baseline checkout.
It exits1 with all34 malformed graph terminals accepted and all17 malformed held
outcomes ACKed; all12 positive controls pass. Original decoder bytes, exact
mapping, command and semantic failures are retained. Baseline binaries were not
captured.

Final normal/race commands pass exact current CI assertions, plus existing offline
terminal typed/before-handler and parent-ownership tests. Both actual race
executables and normal wf were observed; the short normal worker executable was
missed. Normal worker results have source/log/actual command evidence, not binary
binding. Hosted CI execution is unclaimed. All1151 final Go/module/workflow
inputs remained unchanged during the final modeled commands.

Native R1/R3 child/buffered-child compatibility passes in74.471s with independent
raw SDK auditing: four start/terminal witness pairs, eight checkpoint frames.
Its actual race executable was observed. These are compatibility flows, not
native malformed-outcome injection, restored fixtures or power-loss proof.

## Preserved setup correction

Initial fixtures referenced nonexistent encoding constants. setup-compile-*
retain the actual failed baseline/normal/race outputs and receipts; the initial
baseline legacy path still ran and demonstrated17 bad ACKs. No full baseline
claim is based on a build failure. After actual native closure, only the graph
fixture constant names and exact CI codec strings were corrected. Native source
manifest/before bytes and correction timestamp are retained; review requires
those exact two nonproduction changes and unchanged production inputs. Focused
baseline and final modeled runs then executed successfully as described above.

## Review

```sh
python3 docs/scale/runtime-terminal-admission-2026-10-11/review.py
```

The corpus remains162 families/922 pins. Five frozen race/scale campaigns remain
live and predate this code. Complete active-worker terminal admission, full current
qualification, program/clock/promise/lease/protected-reader causality and original
fault/scale/soak/power/client/rollout requirements remain open. Public admission,
import and online collection remain disabled.
