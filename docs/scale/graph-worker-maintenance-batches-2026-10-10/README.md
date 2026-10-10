# Worker checkpoint and archive maintenance batches

Indexed graph continuation publication now verifies checkpoint records in private
batches, renewing the delivery lease before every batch. Archive maintenance
separates checkpoint confirmation, unpublished staging and final record/node
verification. Each phase batch checks cancellation and renews ownership. The
default limits are 128 newly verified/staged records and 256 target node visits.
Constructors, indexed checkpoint anchors, finalization and individual callbacks
can perform additional RPCs; these are item limits, not total RPC limits.

`CheckpointCompaction` owns the complete process-local lifecycle. Its source
head and intent expiry remain fixed; final publication uses the existing
original-head verifier. Abandonment releases the confirmation reader. A failed
release is retained rather than silently retried by deferred cleanup. The first
full saved-corpus run found that retry regression in two release cases; its log
is retained here. The corrected implementation passes all 853 unchanged pins.

The whole worker handoff retains its 15-second parent deadline. Each request
inherits that parent, with 15-second operation and 3-second lease/release caps.
Cancellation, lease loss or an unknown mutation ends the current maintenance
operation. No progress persistence, intent renewal or deadline retries are added.

## Development controls

Twenty-two JSON/protobuf model controls use a two-record/four-node budget:
healthy continuation, and lease loss or cancellation at pointer verification,
archive confirmation, staging, record verification and node verification.
Independent root inspection proves no partially verified pointer or abandoned
archive relocation is published, and confirms reader cleanup. Healthy execution
uses six pointer batches, one indexed confirmation batch, six staging batches
and ten verification batches. The next delivery executes the relocated stage
once, without repeating the original stage.

Removing per-batch lease renewal fails precisely the ten owner-loss controls:
the old owner publishes a pointer or archive despite replacement ownership.
The twelve healthy/cancellation controls still pass. Ignoring the private test
budget fails all 22 controls. Exact unsafe sources and outputs are retained.

The final native selection runs the 20-entry R1 archive boundary and all seven
handoff repair controls with the restored implementation, alongside all 22
maintenance controls under race. The journal checkpoint/compaction/archive race
selection and complete unchanged simulation corpus provide compatibility checks.
Run `python3 docs/scale/graph-worker-maintenance-batches-2026-10-10/review.py`
to inspect source hashes, exact terminals and the retained evidence.

## Remaining work

This is component development evidence. Durable runtime binding/persistence,
maintenance across bounded request deadlines, reader/intent lifetime management,
and the failed actual 100,000-entry native gate remain open. The broader original
simulation/native/fault/scale/soak/retention/import/admission/collector/rollout
gates remain open. Public graph continuation admission stays closed and
production collection stays off. The older sharded qualification job remains
on its original source and cannot qualify these changes.
