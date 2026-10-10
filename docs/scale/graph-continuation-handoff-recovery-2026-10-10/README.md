# Repair a pending canonical continuation before stage admission

A completed owned frame previously admitted its next stage even when the worker
had stopped before publishing the canonical pointer, suspension or configured
archive relocation. A seed23 in-memory transport cut at archive publication
demonstrates this on `5b1e061`: a reconstructed worker runs the stage while archive
CAS is still blocked. The healthy sibling passes. This is a confirmed worker
recovery defect, independent of NATS behavior.

Checkpoint discovery now reports handoff work pending in its exact pinned view:
the indexed pointer must match the verified frame/request, a strictly decoded
continuation suspension must follow that completion, and an archive-enabled
cursor must retain from that checkpoint request. A terminal duplicate still
repairs its outcome without stage entry. Offline frame discovery remains valid;
this flag governs online worker admission.

The worker repairs a pending handoff through the existing publisher and returns
without running the next stage. Publication rechecks the lease, exact invocation,
tail, original and relocated grants. The delivery releases its original forest;
the next delivery reopens relocated receipts. Recovery retains the original
15-second publication deadline and global entry cap. It adds no production
registration or collection permission.

## Directed development evidence

Seven seed23 controls cover healthy/pointer-cut/suspension-cut in both archive
and non-archive stores, plus archive-cut. Each delivery reconstructs the worker
and reacquires its lease. A blocked handoff repeatedly fails with zero next-stage
entries; healing completes publication without running through old receipts;
a later delivery runs the next stage once, with the initial handler called once.
In archive-cut, controlled model collection physically deletes all19 original
tree/payload receipts before that successful stage. Production collection remains
off. These are directed models, not a new seeded family or native latency gate.

Final race controls pass in4.163s. Removing only the worker recovery guard causes
all five cut cells to fail by entering the next stage; both healthy cells pass.
Exact bypass source/output and the initial original-source archive failure are
retained. Two initial fixture build errors are also retained.

Checkpoint/frame race controls pass102.792s. All853 unchanged saved simulations
pass18.008s. Native R1/archive20 passes under race41.065s with two checkpoints,
terminal slot19 and zero forbidden effects. These are development checks, not
frozen source qualification or native kill/unknown-outcome coverage of these cuts.
Run `python3 docs/scale/graph-continuation-handoff-recovery-2026-10-10/review.py`
to verify retained results and inspect [development review](development-review.json).

## Remaining gates

This repairs handoff recovery ordering; bulk verification/archive is still one
large operation with lifetime and deadline constraints. The actual100000 native
gate remains failed. A bounded/resumable publication design must preserve exact
generation/tail, reader and intent lifetime, independent receipts, crash/unknown
outcome handling, physical reclamation tests and the reserved terminal slot.
No multi-hour cap rerun was started. Both older frozen campaigns remain running
on their original sources and exclude this fix (and the recent node-grant fix).
Original full/extended simulation, native fault, scale/soak, retention/import,
collector/admission and rollout requirements stay open. Public continuation
admission remains closed and production collection stays off.
