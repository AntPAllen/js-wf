# Ten-minute R5 consumer-leader race proof

Native race binary from clean `225f04a` passed the named test in 682.45 s;
the retained public Go `test2json` package event reports 683.497 s. The wrapper
runs the precompiled race binary; no synthetic `go test` package line is claimed.
Service launch and binary/tool identities are retained in the parent directory.
The service is terminal with exit status 0, independently checked after completion.

83 batches completed 2,324 invocations and 25,631 journal entries. All six
workloads passed history checks, retained-state invariants, physical WF_RUN and
all 64 consumer drain checks. Largest per-type terminal p99 was 13.797457546 s.
19 actual selected-consumer-leader server kills occurred; 18 selection snapshots
had pending/ack-pending work and one was idle. Activity is observed at selection,
not guaranteed at the later kill instant. All selected consumers and stores
recovered current R5 replicas.

The strengthened verifier cross-checks all 19 original selection snapshots with
fault records: actual leader/node/consumer identity, four peers, activity counts,
and selection/kill/heal ordering. All 34 Python guard tests pass, including
false snapshot controls; the existing real smoke and this run both verify.

All 11 fencing records match worker counters. Eight are heartbeat ownership
confirmation failures and three execution lease failures. Their retained exact
delivery traces overlap confirmed faults 2, 11 or 13. All 11 invocations reach
observed terminal state after fencing; subsequent invocation acknowledgements
are retained separately. None of these later acknowledgements is attributed to
the original fenced delivery. All 141 acknowledged repair attempts (44 start,
80 signal, 17 suspended) have checked source/decision explanations. These
observations establish runtime responses, not a precise server-side cause.

This is one seed, one ten-minute consumer-leader row, five worker objects sharing
one client. It does not clear the full five-node 24-hour fault matrix, separate
worker-process coverage or release attribution gates. Original server stores
remain outside Git; raw large logs/JSON are losslessly compressed here.
