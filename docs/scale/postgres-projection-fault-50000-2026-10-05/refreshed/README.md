# Refreshed full 50,000 PostgreSQL projection fault — failed

Executed source: `e6c124fe3ff8a561b0807d31332a29bc87143d2c`.
Native **FAIL, 494.71 seconds**, original 20-minute fixture/default 50,000,
GOMAXPROCS=2/GOMEMLIMIT=2GiB. The earlier failed proof remains unchanged.

All 50,000 Starts acknowledged and all expected results checked with the
observed/reaped-SIGKILL projection process down; stopped lag was 100,000.
The workflow queue physically drained and all six workflow workers joined before
fault injection. SQL backend termination was confirmed at 78 partial rows; the
actual library journal leader (node 2) was stopped/restarted with a new identity.
Pinned JS handles were refreshed. The faulted projection exited with JetStream
API 503/10008. All three replicas of each of WF_INV, WF_JRN, KV_WF_STATE and
WF_PURGE were current before replacement construction.

Replacement rebuild later failed: **WF_INV ordered consumer Fetch(256) returned
`nats: no responders available for request`**, observed in the bounded dependency
trace. Subsequent journal-source calls were canceled. There are no worker
cleanup errors. Zero lag and rebuild equality were not reached. The primary
server-side cause remains unconfirmed; this failure establishes the application
call site and survives correction of the earlier fixture lifecycle issue.

## Next source decision

The captured nats.go v1.54.0 ordered consumer implementation resets/recreates a
consumer on every Fetch call; its comment recommends Messages/Consume instead.
This is a source observation, not evidence of the server-side cause. Investigate
continuous iteration with bounded context/cleanup and recovery controls before
another full run. Do not rerun this unchanged failure or add a recovery pass
claim based on a smaller population.

## Evidence scope

Independent review binds actual parent/child SDK executables, selected Git and
external inputs, actual PostgreSQL executable, fault chronology and byte-identical
closed SQL media copies. All 7,610 archive members and three parts were read back;
see archive-verification.json. Scripts, original native log, NATS stores and SQL
media are inside the archive. Concatenate numbered parts in order for the gzip tar.

NATS runs as library instances in the captured SDK, without logs; no separate
NATS process hashes or NATS server SIGKILL claim. SQL was gracefully stopped after
SDK exit. Source capture excludes exhaustive assembly/embed/compiler provenance.
Native integrity/queue assertions have named-test scope; original NATS stores
remain closed and were not independently reopened. Two live campaigns shared
the VM; no resource-pressure attribution. Full matrix/24h remain unqualified.
