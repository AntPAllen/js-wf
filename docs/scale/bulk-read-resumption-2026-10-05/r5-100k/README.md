# R5 large retained audit with admitted client pull loss

`TestStreamingAuditR5LargeInterruptedPull` passes on five native Docker NATS
v2.15.0 file replicas, explicit all-peer route seeds, default 2-minute sync,
normal executable, GOMAXPROCS=2 and GOMEMLIMIT=2GiB. It directly publishes a
completed retained cohort of 100,000 invocations and 1,200,000 journal records.
This is checker qualification, not live workflow execution.

| Audit | Elapsed | Completed report |
| --- | --- | --- |
| Baseline streaming journal/state | 18.776959284 s | 100,000 invocations/journals/terminals; 1,200,000 entries |
| Interrupted pull with bulk resumption | 19.372032037 s | Same exact report |

The first real journal pull deliberately drops its suffix after 128 messages
and returns a synthetic client timeout. The new R5 cursor starts at sequence
129, all 1,200,000 sequences are visited exactly once, zero journal point reads
are used, and both audited streams have zero consumers afterward. This forces
the previously unproved resumption branch. It does not reproduce a natural
server failure or spend two seconds waiting for a timeout. The original audit
attempt budget remains 20 seconds; neither result has a large margin.

The actual live test executable SHA-256, complete build information, all five
live container inspections and copied executable bytes/build information,
634 captured Go/module inputs (tracked files plus the new test), unchanged
throughout execution,
source patch/new-file bytes, producer, logs, and original five file stores are
preserved. The archive manifest and publication parts are independently read
back. The server bytes copied from all five running containers match the built
binary retained with the stores. This captures the container executable path,
not live server `/proc/exe` hashes. The test executable is captured through
`/proc/exe`. Compiler dependencies are not exhaustively inventoried, and the
physical stores have not been independently reopened.

The native million/24h diagnostic was concurrently delivering during this
control. It was not restarted. Its eventual timing evidence must retain this
resource-load overlap. No OS SIGKILL, natural NATS cause, distributed 24-hour
workflow soak, or full/final-source matrix qualification follows from this case.

The baseline's proximity to 20 seconds leaves longer-soak audit scaling open.

A follow-up code review of installed `nats.go@v1.54.0/jetstream/pull.go`
(`fetch`, lines 950–1019) confirms that timeout/no-message status responses can
close a short batch without setting `MessageBatch.Error()`. The reader still
resolves an entire short successful tail through leader point reads. This path
is not exercised by this admitted non-nil timeout control and needs a separate
bounded recovery change before another soak. It is not established as the
cause of the original failed run.
