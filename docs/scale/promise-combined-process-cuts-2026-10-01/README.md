# Resolved child promise: worker SIGKILL and complete server restart

A separate worker process runs actual parent and child handlers. The child
returns 614,402 bytes at the production spill threshold; the parent resolves
its promise and requests finish_v1 with promise identity in saved locals.
Publication wrappers expose six exact cuts: before/after frame put, before/after
manifest creation, and after journal/signal purge. The process blocks at a cut,
then the test verifies its SIGKILL exit. All three NATS processes are killed
before restarting any on their retained stores/ports. Confirmed route counts
and current replicas for all ten workflow stores precede replacement execution.

The pre-kill logical journal must remain byte-identical immediately after
restart and as a prefix of the completed successor journal. Replacement writes
must have a higher epoch; the original child handler runs once. Published
manifests resume without initial handler or archive reads and read the child
object once. Before manifest publication the initial handler replays once.
An uncompleted frame candidate is replaced and collected. A completed checkpoint
before manifest publication already records a frame reference; manifest repair
correctly reuses that frame and original anchor instead of replacing it.

The resolved promise must not cause a new suspension for its child signal.
Exactly one call_async declaration and child signal consumption are required.
Both parent/child terminal invocations pass the retained-state audit; all pinned
clients see the same result. With loops stopped, child retirement and GC must
preserve the parent's three references, including exact child bytes. Offline
complete-history replay uses the retained frame/result. Parent retirement then
reclaims all three remaining objects. The full process-kill delay and delay from
the final enabling signal must each stay below 30 seconds; heal delay is also
reported. No gate is relaxed for these process kills.

## Evidence

Two-cut normal run passes in 36.777 seconds and race run in 39.386 seconds.
The initial expanded six-cut race passes five cases and fails only the test's
incorrect assertion that before-manifest repair must abandon an already-recorded
frame. Production publishContinuation uses the completed frame/anchor, as required
by prefix preservation. The fixture was corrected; the final six-cut race passes in 113.920 seconds.
Kill-to-terminal times are reported per cut in the retained final log. The
initial failing log is retained rather than counted clean.

Compiled missing frame-promise GC marking fails in 19.159 seconds with two
references and one premature deletion. The first missing-restoration mutation
ended at the 90-second context deadline and is explicitly NOT an accepted
control. Adding a journal-state invariant detects a successor suspended on
already-resolved child_0; that compiled mutation fails in 18.181 seconds with
semantic evidence. Both production patches/logs are retained. Vet passes.

The workflow continuation-promise-restart runs all six cuts under the race
detector on relevant main pushes and manual dispatch, retaining its log.
Hosted confirmation remains pending. These are fixed publication cuts with a
single parent/child, not the complete chaos matrix, TTL proof, online GC or
five-node 24-hour soak. The corresponding seeded shared-promise retirement
response faults live in sim/worker_promise_retirement_replay_test.go; it does not
model NATS persistence or byte-identical SIGKILL/restart traces.
