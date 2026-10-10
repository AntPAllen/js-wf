# Native owned-grant renewal cost and recovery — 2026-10-10

`TestNativeGraphOwnedGrantRenewalCostAndRecovery` creates a four-record graph
and a complete unpublished two-record prefix compaction in an explicitly indexed
native namespace. It adds 1,000 real uploading orphan grants through the native
CAS adapter, checking every result. These grants are outside the final forests.
The default fixture runs R1 and R3; `WF_GRAPH_NATIVE_OWNED_GRANTS` selects an
explicit 1,000–100,000 grant diagnostic without changing production defaults.

The staging checkpoint is written to disk before renewal. After one 128-scope
batch, the next blob CAS commits but its acknowledgment is deliberately lost.
The old operation must fail, perform no further I/O, and emit no checkpoint.
Every test peer is stopped and restarted on the same store. Fresh native
adapters load only the disk checkpoint and the same requested expiry, then start
a complete fresh scan. No private verification progress or scan prefix is reused.
Every grant is individually audited at the target expiry; the barrier remains
an absent reservation and the source head remains unchanged. Only a subsequent
independent commit may publish the small compaction.

Setup uses three-second contexts and renewal uses 128-scope batches with
15-second contexts. A 20-minute fixture watchdog bounds provisioning, restart,
renewal and audit. The authority intent starts at one hour; measured recovery
renewal must finish both before its remaining original lifetime and inside the
20-minute window corresponding to a one-hour one-third renewal trigger.
Provisioning and audit times are outside the measured renewal window. The latter
includes the interrupted attempt, all-peer restart, fresh reconstruction and full
renewal. Maximum batch wall time covers the successful fresh renewal batches.
Counters cover port calls in that same measured window, not native wire requests.

This is component coverage using embedded native servers. Peer stop/restart is
not an OS SIGKILL, VM power loss, storage loss, or worker lease recovery gate.
There are 1,000 orphan grants plus six compaction grants, not 100,000 SDK journal
entries. Larger native renewal and the original full-entry gates remain open.
No inference about older server-side failures is made.

The initial run is retained as `development-counter-window.log` and excluded:
its counters included final commit work, and it did not separately enforce the
20-minute budget. The accepted rerun uses the frozen final source.

## Accepted default run

R1/R3 pass race 127.995 seconds. Recovery plus complete renewal takes
34.207177406 / 59.124050535 seconds; maximum fresh batches take 4.197786178 /
6.865749819 seconds. Each fresh scan examines 1,007 scopes and renews 1,006 grants.

## Reproduce

```sh
go test -race ./internal/graphpublication -run '^TestNativeGraphOwnedGrantRenewalCostAndRecovery$' -count=1 -v
WF_GRAPH_NATIVE_OWNED_GRANTS=10000 go test -race ./internal/graphpublication -run '^TestNativeGraphOwnedGrantRenewalCostAndRecovery/R1$' -timeout=25m -count=1 -v
python3 docs/scale/graph-native-owned-renewal-2026-10-10/review.py
python3 docs/scale/graph-native-owned-renewal-2026-10-10/review_large.py
```

`native-race.log` records terminal default-fixture status and measured costs;
`review.json` summarizes accepted rows and source checks. Larger runs have their
own logs and terminal status; a launch is not acceptance. Admission and online
collection remain off.

The 10,000-grant launch is bound to source commit f471c1a and service invocation
c0cc3ba9852a4c54a76154b87a0a1403 in owned10000-launch.json. review_large.py reads
that exact service and records a timestamped state. While MainPID is nonzero,
the result is explicitly unaccepted. Terminal acceptance requires a matching
loaded invocation, actual exit status zero, terminal package PASS, unchanged
frozen package sources, all 10,006 grants renewed across 79 fresh batches, and
the existing batch/renewal budgets. Missing or replaced services never count as
successful completion. Live ExecMainStatus=0 is not an acceptance signal.

## Completed 10,000-grant diagnostic

The log records full renewal and audit of 10,006 grants, 10,007 scopes, 79 fresh
batches, recovery plus renewal 4m46.723596664s, maximum batch 4.391456815s and
package PASS in race 393.559 seconds. Systemd automatically unloaded this
transient unit before terminal status capture. The original invocation journal
records start and resource use but has no explicit exit record. Consequently
owned10000-state.json retains accepted=false with terminal status unverified.
The result informs capacity planning; it is not service acceptance. The next
large run saves actual child exit status and retains its terminal service.
