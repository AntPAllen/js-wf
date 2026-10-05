# Context option reuse controls

Pinned nats.go1.54.0 NextContext constructs a closure assigning its captured
context to local nextOpts.ctx. It has no mutable per-call state. byteIteratorBatch
reuses that immutable option within each bounded record window; deadlines and
cancellation remain the same. Original profile has59MiB sampled flat allocation
at NextContext; no measured capacity speedup is claimed for this small change.

Executed existing race controls:

    go test -race -p=1 ./integrity -run '^(TestByteBoundedFetch|TestByteIteratorBatch|TestByteReplay)' -count=1

PASS1.019s. These cover cancellation, record limits, semantic errors, heartbeat
recovery, retained prefetch and confirmed/unconfirmed replay decisions. They are
unit controls, not native400k performance or full matrix qualification.
