# Continuation frame codec proof

`go test -race ./internal/checkpoint -v` passed in 1.764 seconds; vet passed.
The package defines and validates version 1 bounded frames and hashes exact
stored bytes. The round-trip retains user locals, state, signal consumption,
promise outcome references, timer cancellations, absolute SDK position and
panic attempt count. Decoded state does not alias the input buffers.

Controls reject wrong content hashes, invocation ID/type/generation changes,
anchor index/epoch changes, pending SDK positions, future/odd timer identities,
invalid state/metadata, unsupported versions, unknown fields, trailing JSON,
conflicting/missing promise result references, and frames over 16 MiB. Failures
return a zero frame. Stage registration remains the worker's responsibility.

This is a codec, not a checkpoint execution proof. SDK capture, content-addressed
Object Store publication, manifest CAS, anchor/suffix validation, worker dispatch
and crash repair still need integration and their independent model/real proofs.
