# Worker step order admission — 2026-10-11

Worker.execute checks request/completion balance before handler admission,
terminal repair or signal draining on its execution path. Overlapping requests,
orphan/duplicate completions, and successful terminals with unresolved requests
are rejected. A pending request or failed terminal may remain unresolved.
A verified resumed checkpoint's first completion anchor belongs to the compacted
prefix; every suffix completion still requires its retained request.

## Controls

The shared modeled worker fixture now accepts variable histories, checks that
rejected deliveries do not create outcome projections, and records the expected
one SDK callback when a pending request legitimately resumes execution.

Ten histories per legacy/canonical backend cover balanced pairs, an interleaved
suspension, pending request execution, orphan completion, overlapping requests,
completion-before-request, duplicate completion, successful unresolved terminal,
balanced terminal repair and failed pending terminal repair. These20 controls
contain ten positives and ten negatives. Existing60 metadata and22 epoch owner
controls run together with the new cases; this is102 worker controls, not new
seeded families or native fault-injection evidence.

Baselinef754d1f plus the retained regression/helper exits1. Eight malformed-order
histories enter the handler. Two unresolved successful terminals populate the
outcome KV despite no handler admission. All ten positives pass. Baseline
production source and actual command/log are retained; executable capture was
missed. The retained native-step-metadata-test.go.txt is also the baseline helper.

Final normal/race commands pass exact current workflow assertions for all102
controls. Actual final executable commands/builds/hashes are captured and parent
PIDs match the Go drivers. All1149 prelaunch Go/module/workflow inputs match the
final manifest. Hosted CI execution and full current qualification are open.

Native R1/R3 child/buffered-child continuation compatibility passed in72.290s
with independent raw SDK auditing: four start/terminal witness pairs and eight
checkpoint frames. The native binary used the preceding helper version; no
production source changed afterward. Its own manifest and helper bytes are
retained. After native closure, the test-only empty-KV assertion was corrected
and both modeled commands repeated. The reviewer binds both helper versions and
requires exactly the ErrNoKeysFound correction plus its imports/formatting.

## Preserved failed verification

The first normal/race commands (empty-kv-assertion-*) exited1 because the new
negative assertion treated the valid empty-key census ErrNoKeysFound as a
failure. That condition did not run during native tests. All other KV errors and
all nonempty key lists remain rejected. The correction timestamp follows actual
native closure; no concurrent source mutation occurred.

## Review

```sh
python3 docs/scale/worker-step-order-admission-2026-10-11/review.py
```

Native stores are ephemeral. This does not establish corrupt-history native
injection, restoration, power-loss safety, complete request-kind semantics,
program/result equivalence or protected-reader/lease causality. Terminal-held
fast paths and separate maintenance paths that bypass execute are outside this
change. Corpus remains162 families/922 pins. Five older qualification campaigns
were revalidated active and predate this change. Original fault/scale/soak/power/
client/rollout gates remain open; public admission/import/online collection stay
disabled.
