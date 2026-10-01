# Continuation suspension and handoff SIGKILL — October 1, 2026

Two new real R3 cuts extend the existing publication/process fixture. A test-only
operation observer holds the child immediately after acknowledged Suspended
append, or after successful handoff enqueue and acknowledged lease release but
before the original delivery ACK. The parent independently inspects durable
state before sending and verifying actual SIGKILL.

Both cuts have nine reconstructed records, ending at index 8 in
Suspended{waiting_on:continuation:finish_v1}, with the verified frame already
published. The live journal has only anchor plus suspension, and WF_SIG has no
remaining messages: the frame must supply its buffered signal on resume.
The durable run consumer still has unacknowledged deliveries at both cuts.

After suspension, no message carrying the expected continuation dedup ID may
exist and the lease must remain present. After handoff release, precisely one
run carries continuation:<invocation-sequence>:<anchor-sequence> and the correct
invocation body; the lease key must be deleted. These observations distinguish
the two actual cuts independently of the child's marker.

After SIGKILL the suspended scanner must enqueue one named continuation candidate
for the Suspended sequence. A replacement pinned to another peer rejects archive
reads and never reenters the prefix handler. Original redelivery, the confirmed
handoff (second case), and the independently asserted scanner wakeup are available;
the fixture does not attribute recovery to a particular queued delivery.
Prefix/suffix effects each run once, buffered signal/state/locals and result 46
match, all peers agree, raw integrity passes, and the entire pre-kill logical
prefix remains byte-identical. Full offline staged replay verifies all 12 SDK
entries with no effect callbacks. A terminal scan must enqueue nothing.

## Evidence

- `race.log`: both new cases pass under race in 21.693 seconds. Recovery samples
  are 12.932 seconds after suspension and 127.391 ms after handoff/release. Both
  have zero archive reads and no initial-handler reentry after replacement.
- `enqueue-mutation.log`: compiled production overlay skips continuation enqueue;
  the parent finds zero expected handoffs instead of one, failing in 3.280 seconds.
- `repair-mutation.log`: compiled scanner overlay removes the continuation-wait
  case; after actual SIGKILL it rejects the durable wait, failing in 3.199 seconds.
- `skip-enqueue.patch`, `skip-repair.patch`, `SOURCE_SHA256SUMS`: tested controls
  and sources. No worktree production runtime was modified.
- `vet.log`: integration vet exits zero without diagnostics.

These close focused suspension and post-handoff/release process-death cuts.
The previous frame/publication cuts were verified under race at 25330a9; the new
cut branches have separate real race evidence here. Combined faults, concurrent
server faults, unknown replies at handoff/release/ACK and seeded kill schedules
remain open. Individual samples do not establish release p99 or full matrix/soak.

Fresh mixed CI at 25330a9 passed seed 1 then failed seed 2 terminal p99 at
47.748 seconds (run 36811347097). `mixed-25330-seed2-failed.log` retains that failure.
It predates this test-only change, and does not establish a new causal explanation.
It remains a failed release gate alongside seed 12 and seed 65. Full Tier 1 and
million-timer jobs remain live without restart; online GC remains unsupported.
