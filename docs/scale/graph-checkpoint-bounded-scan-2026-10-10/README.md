# Bounded checkpoint verification against an exact pinned view

GraphView.NewCheckpointScan creates process-local verification state bound to
that exact forest, invocation and tail. Advance accepts a positive new-record
budget and a fresh caller context. A deadline/cancellation preserves only fully
verified progress; other errors latch a rejection. Each batch renews the existing
reader and uses fresh range reads. Closed/expired authority cannot be revived.
Only a complete scan with a verified owned final frame returns a checkpoint.
Returned frame/record bytes are caller-owned copies of the private verified state.

The scanner retains parser state and the latest checkpoint's necessary suffix.
It discards the preceding records, avoiding the previous whole-prefix buffer.
Suffix storage still grows when the caller needs a long suffix. Existing
ReadCheckpoint uses this scanner in a single complete batch, preserving its
synchronous behavior. No persistent scan certificate, new wire format, worker
maintenance loop or public continuation admission is introduced here.

## Confirmed earlier-completion admission gap

The original discovery path decoded a completion header only when its request
was a checkpoint. A valid later frame could therefore bypass duplicate, aliased,
escaped-duplicate, unknown, missing or null ordinary completion headers. Corrected
fixtures produce12 actual acceptance failures at fc0fa6d (JSON/protobuf × six
cases); four plain/opaque-user-result siblings pass. The scanner now strictly
decodes every completion it scans. Existing indexed pointers still skip their
previously published prefix; this is not an old-store migration or comprehensive
all-record-kind admission claim.

## Directed development evidence

Twelve batch/authority controls cover JSON/protobuf × pause, renewal, tail
change, retirement/reuse, close and expiry. A36-record scan does37 entry reads,
including one final anchor ownership recheck. A forced deadline adds one failed
attempt (38 total), retains next index8 and resumes without rereading verified
prefix records. New record progress stays within each requested batch budget;
final frame verification has its explicit additional anchor read. A timed4-second
reader completes37 virtual seconds through18 renewals. Captured old results
remain readable across permitted lifecycle changes, but fresh confirmation
rejects a changed tail or generation. Closed/expired scans read zero entries and
leave authority byte-for-byte unchanged.

Final directed header, batching and result ownership race controls pass7.007s.
Removing the record limit causes all12 batch controls to fail. Returning the
private result object instead of a copy fails the eight result ownership cases;
close/expiry controls still pass. Exact mutants and output are retained.

The broader checkpoint/frame/index/archive race run passes123.433s, all853
unchanged regressions pass20.378s, and native R1/archive20 plus handoff recovery
pass41.020s (two checkpoints, terminal slot19, zero forbidden effects,19 original
receipts reclaimed in the directed recovery model). These broader runs precede
the final result ownership copy; the final directed checks exercise that copy.
This is development evidence, not frozen source qualification.

Initial errors are retained: an unused import after the refactor, a fixture that
incorrectly classified a lone escaped known key as ambiguous, and a budget fixture
that omitted the required final ownership recheck. Final ambiguity evidence uses
an escaped duplicate, and final work counts include the recheck.

Run `python3 docs/scale/graph-checkpoint-bounded-scan-2026-10-10/review.py` to
verify source hashes and retained controls; see [development review](development-review.json).

## Required continuation work

Progress is held by this live GraphView and is lost with its process. Publication
still makes its own fresh confirmation and uses the unchanged15-second worker
context. The actual100000 gate remains failed; this API does not clear it.
Next required work is integrating bounded verification with publication while
preserving exact tail/generation and uncertain release/CAS handling, then durable
handover and resumable archive staging with reader/intent lifetime control,
independent grant/receipt checks, reclamation and the global terminal slot.
Both older frozen jobs remain on their original sources. Every original extended,
native fault/scale/soak/retention/import/collector/admission/rollout gate remains
in scope. Public continuation admission stays closed; production collection
stays off.
