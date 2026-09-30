# Live inline inputs at one/five-million-subject checkpoints

Clean binary source: `151735c22700562ca49df8f3674a947781db0c17`.
Three separate NATS 2.15.0 processes, file storage, three replicas.

```sh
go build -o /tmp/wf-scale ./cmd/wf-scale
/tmp/wf-scale -counts 1000000,5000000 -live-workflows 1000 \
  -live-input-bytes 64 -min-available-mib 2048 -root /tmp/wf-scale-inline-5m
```

Both phases completed 1,000 production workflows, each with a 64-byte inline
input, four journal entries, and immutable results through all pinned peers.
The first cohort remains retained at the second checkpoint. The final counts
are 5,002,000 invocation messages/subjects, 5,008,000 journal messages and
5,002,000 journal subjects through every node. All 2,000 saved terminal results
were independently decoded and compared with expected input lengths/digests.
Production retained-state integrity checks and queue drain passed in both phases.
Opaque background messages are excluded from cohort integrity claims.

The separate million-timer campaign was live throughout; these measurements
are no-fault cardinality observations under that concurrent load. They do not
clear 10M live traffic, large-input 5M/10M, or the fault release gates. The stores
remain at `/tmp/js-wf-scale-live-inline-5m-20260930` on the measurement VM.
