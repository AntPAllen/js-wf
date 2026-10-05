# Full 50,000 PostgreSQL projection recovery — qualified

Executed source: `9fbfa1637264a9314da65c297d34332b0f01bdcb`.
Native **PASS, 1,030.97 seconds**, original 20-minute fixture/default 50,000,
GOMAXPROCS=2/GOMEMLIMIT=2GiB. Actual parent and killed projection SDK hashes match.

All 50,000 Starts acknowledged; all expected results checked while the observed,
reaped-SIGKILL projection process was down. Stopped projection lag: 100,000.
Workflow queue drained and six workers joined before the projection fault.
PostgreSQL confirmed termination of the advisory writer backend at 81 partial
rows; the actual journal leader was stopped/restarted as an embedded library
server with a new identity. Pinned clients refreshed and all four R3 source roles
current before replacement. The faulted writer exited with a lost-session error.

The replacement uses a continuous ordered WF_INV iterator. Lag drained to zero;
a full rebuild preserved every exposed SQL row and actual indexed type/id/status/
attribute column byte-for-byte. Independently parsed all 50,000 canonical rows,
including unique invocation identities, completed status, schema/index/timestamps.
Random internal rebuild generation tokens intentionally change and are excluded.
Canonical before/after SHA256:
`02c50f3836990cc0ffd39e76f53ba318753e06ed8be6d318775d597aae6c604a`.

## Complete evidence

Independent review binds actual SDKs, 664 selected Git inputs, 3,287 selected
external Go inputs, actual PostgreSQL executable and 1,284 closed SQL media files.
All 7,617 archive members/four parts read back and verified. Native log, source,
binaries, original NATS stores and closed SQL media are in the archive. Concatenate
numbered parts in order to recover the gzip tar.

NATS servers are embedded in the actual SDK and run without logs; no separate
NATS process hashes or NATS server SIGKILL claim. PostgreSQL was gracefully stopped
after SDK exit, not killed. Selected inputs exclude exhaustive compiler/assembly/
embed provenance. Original NATS stores remain closed; a [separate complete copied
retained-state audit](../copied-retained-audit/) verifies integrity and queue drain.
Two live campaigns shared the VM; no pressure-cause claim. Earlier failed proofs
remain unchanged: this pass does not establish their server-side causes or qualify
full matrices/24h/additional combined faults.
