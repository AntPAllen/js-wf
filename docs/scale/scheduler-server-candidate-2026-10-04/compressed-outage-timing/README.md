# Retained compressed diagnostic: exact outage/receipt timing

All300 fixed40-byte receipt slots, checksums, sequences and controller/server
non-early times verify against the published original manifest. Integer nanosecond
decoding regenerates the original p99=10.482509603s and maximum13.232558335s.
Sixty receipts (20%) exceed2s; every deadline-to-receipt interval intersects one
recorded kill/restart window,30 each. The windows are9.930810682s/10.010438456s.
Exact inputs, decoder, per-receipt CSV and aggregate are retained.

Intersection is an observation, not proof of sole causality. The compressed
90-second run remains failed under its original2s p99 gate. No limit or receipt
is altered. The next comparison uses the full original million/24h population
and unchanged lateness limits, while retaining diagnostic candidate scope.
