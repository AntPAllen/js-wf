# Initial journal metadata reply recovery

Serial journal reads, snapshot manifest reads and consumer creation already
bound individual requests. The first lazy WF_JRN metadata lookup in Read used
the whole caller context. A lost reply could therefore stall before those
bounded reads even began. The mixed start-repair bootstrap failure exposed
this class of stall but did not isolate its original request or server cause.

Read now applies the existing three-attempt, two-second-per-attempt read policy
to the initial stream lookup. The cache still coalesces concurrent loaders,
allows independent waiter cancellation, retains successful handles and avoids
caching failures. Shorter caller deadlines remain authoritative. Append keeps
its existing bounded lookup path.

The new real R3 contract retains four journal entries and holds actual NATS
responses through ClientProxy on a fresh reader's first Stream request. The
second actual request must start within four seconds. Resuming replies must
return the original records and physical tail unchanged; another read must
reuse the cached metadata handle. The race-instrumented suite passes all four
held-reply cases (initial lookup, serial read, snapshot manifest and consumer
creation) in24.323s package time, with no race warning.

A Go overlay restores only the old unbounded lookup. The exact new test then
fails with `lost initial journal metadata response occupied the read deadline
without a retry`, after7.09s including cluster setup. Actual named execution is
verified; a compilation failure, skip or timeout cannot substitute for this
counterexample. Original positive and negative JSON events are losslessly
compressed; the independent execution record includes source and event hashes.
The source is based on the `82494c5` worktree before commit.

The dedicated journal-read-recovery workflow checks all four actual named race
passes and the semantic negative control, preserving original events, source
hashes and the overlay source. Hosted acceptance is pending. This is a transport
adapter/read-recovery contract; it does not prove a server defect or clear the
full mixed fault rows. The existing116-workload simulation campaign has no
initial metadata reply actor and remains evidence for its recorded model graph.
