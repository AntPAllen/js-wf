# Full 50,000 PostgreSQL combined projection fault — failed

Executed source: `f87c4223a075db29ab99a19981fe3059109c5871`.
Native result: **FAIL, 405.60 seconds**, under the original 20-minute fixture
budget, default 50,000 invocations, GOMAXPROCS=2 and GOMEMLIMIT=2GiB.

All 50,000 Starts acknowledged and all 50,000 expected results were checked while
an observed projection SDK process was down after a reaped SIGKILL. Stopped
projection lag was exactly 100,000 journal messages. During catch-up at 31 SQL
rows, PostgreSQL confirmed termination of the advisory writer backend and the
actual journal leader was stopped/restarted as an embedded library server. The
faulted writer exited with a lost-session error; all three journal replicas
subsequently reported current. The replacement projection later exited with
`nats: no responders available for request`. Zero lag and rebuild equality were
**not reached**. Two worker cleanup errors reported `nats: connection closed`.

The fixture restart replaces the killed node's pinned client, leaving old
JetStream handles closed. This explains a fixture lifecycle risk, but does not
establish the primary no-responder cause: the replacement used a survivor client
and had written rows before failing. The primary cause remains **unconfirmed**.
Next work is client refresh, stopping completed workflow workers before the
projection-only fault, readiness checks for all projection source roles and
bounded dependency tracing. The failed run remains failed.

## Retained evidence

`independent-review.json` binds the actual parent and killed child SDK, 662
selected Git inputs, 3,287 selected external Go inputs, the actual PostgreSQL
executable and 1,281 closed PostgreSQL media files copied byte-for-byte from the
retained volume. All 7,609 archive members and three parts were read back and
verified; see `archive-verification.json`. Scripts and native media are inside
the archive. Concatenate the numbered parts in order to recover the gzip tar.

NATS servers are embedded in the captured SDK and configured without logs;
there are no separate NATS process executable hashes. PostgreSQL was stopped
gracefully after SDK exit, not killed. Selected input capture is not exhaustive
assembly/embed/compiler provenance. Original stores remain closed and retained.
This run shared the VM with two live campaigns; no resource-pressure cause is
claimed. This evidence does not qualify recovery, full matrices or 24 hours.
