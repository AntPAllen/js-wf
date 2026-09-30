# SDK checkpoint capture and restoration

`go test -race ./wf ./internal/checkpoint -v` passed (SDK 10.720 s), the complete
simulator suite passed in 81.171 s, and final focused checkpoint controls passed
under race in 1.024 s. Vet and diff checks passed. The tested tree was modified
from `9131a39`; source hashes below identify its implementation.

The SDK captures locals, materialized state, consumed signal IDs, resolved
promise outcome references, canceled timers and the absolute SDK position for
a prospective checkpoint request/completion pair. Capture makes no journal
write and does not advance its cursor. Restoration verifies frame generation,
anchor/hash and ordered SDK suffix shape before returning a context. Detached
state and payloads are restored with empty derived promise caches.

The uninterrupted control consumes one of two identically named signals,
resolves a child promise, sets state and cancels a timer. A prefix-free restored
continuation produces identical state observations, selects the second signal,
reuses the resolved promise without another signal, and produces the same
RunOnce key and suffix entries. A second checkpoint preserves prior segment's
cancellation and consumption facts. Separate controls verify referenced promise
bytes through the normal outcome hash checker and reject corrupt objects.

Capture rejects unfinished/suspended boundaries and all live handles, including
a timer whose signal branch won. Awaited/canceled handles permit capture.
Nested contexts/handles and cyclic locals fail before any write. An overlapping
slice control ensures pointer-based cycle detection cannot hide a handle in a
longer view. Restore rejects reused generations, SDK entries before the anchor,
duplicate or unordered indices, wrong step kinds and malformed payloads without
returning partial state or calling an appender.

This is SDK capture/restore proof, not durable checkpoint execution. There is
no production worker caller yet. Publication, manifest CAS, logical suffix
reads, stage dispatch, offline continuation replay, reconciliation and real/model
crash-cut gates remain open. The control simulates the committed pair locally;
it does not prove that NATS committed it.

A later nullable-empty-state control overlays the production restore source
from `02e5b08` and fails with `assignment to entry in nil map` when the restored
continuation sets its first state value. Restore now initializes a writable
empty map for a valid `state:null` frame. The same control passed under race
in 1.012 seconds. Before/after logs and final source hashes are retained. This
is a local SDK bug/control, not a NATS failure or a durable publication proof.
