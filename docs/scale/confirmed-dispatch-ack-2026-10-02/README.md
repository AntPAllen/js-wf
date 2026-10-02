# Bounded consumer ACK confirmation

Pinned nats.go1.54.0 Ack publishes locally; DoubleAck uses RequestWithContext and
marks its local message acknowledged only after a successful reply. A held-reply
R3/file contract passes race4.736s: plain Ack returns while server replies are
blocked; DoubleAck times out after250ms even though another connection sees the
consumer's committed ACK; retry after resuming replies succeeds and physical
stream drain is separately observed. Existing late-ACK/retention contracts remain
relevant: consumer confirmation is not proof of completed stream deletion.

The worker now calls DoubleAck with a two-second parent-bound context at both
normal and terminal-held ACK points. Operation dispatch_ack_confirmed records
its timing/error. Handler cancellation does not supply the ACK context; parent
shutdown still bounds it. An error remains ambiguous and retained workflow state
supports safe redelivery; no success is inferred from a local publish. Parent
cancellation/deadline and successful-confirmation provenance checks pass race.
Full worker race package passes41.418s. The ACK contract plus actual forward/
reverse protobuf and visibility lifecycles pass race24.260s.

This supplies confirmed-reply provenance for future runs. It does not retroactively
confirm the ahead36960942468 asynchronous ACK/NAKs or prove the cause/fix of that
physical-drain failure. NAKs remain asynchronous; all prior latency/drain targets
are unchanged. Changed-source full simulator and native validation remain required.
