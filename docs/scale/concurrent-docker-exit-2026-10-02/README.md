# Concurrent observation of a killed Docker server

The original ahead200 seed27 cut remains rejected: its source-absence observation
was395ms past earliest due. That is an upper bound, not evidence of actual exit
after due. The observer previously waited for `docker kill` to return before
querying stopped state, coupling the bound to client reply/cleanup latency.

The fixture now polls the exact container concurrently with the context-bound
kill command. Only exited, dead or absent confirms source exit; running/removing
is insufficient. Timestamps remain unshifted controller-clock upper bounds.
The helper joins the kill command and confirms absence before permitting name
reuse, preventing a delayed request from targeting a successor. Failures in the
kill or state read still reject the operation. The receipt explicitly marks
concurrent observation; the verifier checks both timelines before restart and
preserves the legacy chronology for older receipts. Exit before earliest due
is still mandatory. Nothing is backdated or inferred from daemon clock time.

## Accepted local contract

Five actual race-instrumented positive tests pass, including real NATS Docker
containers with and without automatic removal, and a real successful Docker
kill whose reply is held until after the stopped state is observed. A channel
ordering test proves exit is recorded before reply without sleeping to force
the order. The verifier's three Python tests accept marked concurrent exit with
late reply/cleanup and reject unmarked chronology, late exit, restart before
reply, wrong container, unknown state and altered retained-prefix evidence.

The compiled source control removes only `go` from the kill request's goroutine,
forcing sequential reply-before-observation. The actual named channel-ordering
test then fails with `kill reply prevented observing stopped server`. The held
reply is bounded by its operation context; this is not a global test timeout,
build failure, skipped test or unrelated failure.

Source hashes are recorded before/after the final contract, based on
d0f947951924e6097c1f039990cb7b7cd54e31b4 with the new source in the worktree.
The subsequent commit contains those tested bytes. The image ID is retained.
`originals.tar.gz` preserves the initial and both runner invocations, JSON events,
stderr, source overlays, verifier log, source inventory and reports. Every member
is SHA256-compared on readback with its original and listed in `manifest.json`.
The original temporary directories remain.

The dedicated `docker-exit-contract` workflow builds the module-pinned server and
repeats both positive and semantic negative gates. The mixed-row workflow now
accepts `start_seed` (default1) for direct replay of failures such as27. Individual
row checks verify that requested seed; non-1 ranges do not satisfy the existing
full-range campaign checker. Fresh admitted and sustained clock validation is
still required. This contract alone does not explain the original server's exit
or clear full matrix/soak gates.

## Hosted ten-minute ahead-clock seed27

[Run37030391587](https://github.com/AntPAllen/js-wf/actions/runs/37030391587)
at exact0f979d0 completes the actual race-instrumented ten-minute seed27 row:
644 terminal invocations,7,079 entries and19 admitted skewed-owner cuts. Every
cut satisfies the strict source-exit-before-earliest-due rule. All histories,
raw controller timing, per-type p99, final retained state, checkpoint cohorts,
common clock and physical drain pass. Worst terminal/progress p99 are13.876s
and14.934s. The checker regenerates the uploaded report byte-identically.
`hosted-ahead27-ten-minute` preserves every downloaded artifact plus terminal
run/job metadata, rechecked report and independent review, with SHA256 readback
comparison for every member. CI source provenance comes from terminal checkout
metadata; this row did not upload a complete Go source inventory.

This accepts the focused current-observer seed27 only. It does not retroactively
accept the original rejected ahead200 seed27 cut, certify seeds1..200, all timer
combinations, the full matrix, or24-hour runtime soak.
