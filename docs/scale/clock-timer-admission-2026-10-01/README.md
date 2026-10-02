# Pending timer cut candidate selection

The clock-row fixture has a pure candidate selector for a durable positive
Sleep request observed at the latest suspended tail. It requires one matching
successful timer-clock lookup from the request's worker/index, the expected
shifted source, matching retained absolute deadline,matching waiting_on,and
controller time remaining beyond the supplied cut lead budget. Candidate choice
is deterministic by invocation subject, independent of receipt arrival order.

Eighteen controls cover both clock directions: valid selection and rejection
of an expired cut window,completed tail,wrong wait,wrong owner,unknown clock,
unshifted clock,ambiguous matching origins and an unobserved future receipt.
Normal checks pass0.005s and race checks1.019s. No production behavior changes.

This is preparation, not an admitted fault proof. Independent receipts can lag
behind a subsequent journal write. The fault injector must refresh the actual
retained tail and match its sequence/entry to the candidate before SIGKILL;
actual removal must precede the earliest conservative controller duration
boundary. Later final retained data must corroborate the selected prefix at the
cut. Admission must fail closed when those observations are missing or overlap.

The selector is not yet wired into the real R5 fault row or artifact guard.
It does not certify any native pending-timer cut, all effect/continuation cuts,
sustained/200-seed coverage or the full24-hour release matrix. Existing clock
role evidence still explicitly reports incomplete in-flight combination scope.

## Opt-in native injection and artifact corroboration

WF_TIER3_CLOCK_TIMER_CUT=1 now wires selection into each clock-row fault.
The workflow exposes clock_timer_cut (default false). After normal clock
role/profile admission, selection waits at most10s for a candidate with150ms
lead,then freshly reads the exact last retained subject message and checks
its sequence/entry with100ms lead remaining. Actual SIGKILL removal must
precede the conservative duration boundary,or the row fails closed.

Each cut records fault-N-clock-timer-cut.json. The guard independently joins
selected request/suspension to final receipts and retained entries,verifies
shifted source and absolute deadline,checks actual removal against process
operations,and rejects later journal windows beginning before removal.
The opt-in log marker or explicit CLI flag requires all cut proofs.
A successful check still reports incomplete overall in-flight combination
scope and cannot certify full release.

Six refresh controls reject advanced/changed/corrupt/late or unreadable tails.
Focused Go race checks (selection,refresh,existing election waits) pass1.386s.
All32 Tier3 Python guard tests pass,including20 two-direction admission
subcases with missing/corrupt proof,origin,deadline and overlap controls.
The native opt-in path has not yet been executed and is not accepted evidence.

At clean source7101d48, ahead smoke [36943700912](https://github.com/AntPAllen/js-wf/actions/runs/36943700912) and behind smoke [36943703099](https://github.com/AntPAllen/js-wf/actions/runs/36943703099) launch with clock_timer_cut=true and35s inputs. Both are queued; exact launch source/inputs are retained. No admitted native timer cut is claimed before terminal artifacts are verified.
