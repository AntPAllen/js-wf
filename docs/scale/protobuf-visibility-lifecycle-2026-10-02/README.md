# Visibility and purge lifecycle in both journal encodings

R3 race test passes10.138s package/9.09s test, with JSON4.54s and protobuf4.55s.
Both run real workflows with journaled search attributes, verify physical terminal
encoding, projection catch-up/Lag0, attribute/status indexes, snapshot compaction
and byte-identical rebuild. After purge, a projection restarted on another peer
rebuilds with no stale row or completed-status entry. This is a lifecycle component
proof, not the50k projection scale or10k concurrent purge/reuse acceptance gate.
Protocol CI now explicitly requires this compiled test name before execution.

The original plan specifies ordered durable purge, not an online blob sweeper.
Existing quiescent blob GC remains available. Online writer-coordinated GC is
an extension and is not an additional release requirement inferred from that plan.
