# Million-subject live input-spill proof

Clean binary source: `151735c22700562ca49df8f3674a947781db0c17`.
NATS 2.15.0, three separate processes, file storage, three replicas.

```sh
go build -o /tmp/wf-scale ./cmd/wf-scale
/tmp/wf-scale -counts 1000000 -live-workflows 1000 \
  -live-input-bytes 1100000 -min-available-mib 2048 -root /tmp/wf-scale-live-1m
```

The report records successful completion and unmodified source. All 1,000
spilled inputs completed through production workers; each result matched its
input length and SHA256 and was immutable across all pinned peers. The saved
cohort audit contains the actual journal records and terminal KV bytes, plus
expected results. The production invariant checker passed before serialization;
the inline/spill contract separately verifies saved-evidence round trips.
An independent decode of every saved terminal result matched the expected
length/digest, and terminal journal payloads matched terminal state.

Opaque background subjects are capacity data excluded from the cohort audit.
Exact aggregate subject/message counts are checked through all three peers.
The 24-hour million-timer campaign was running concurrently on this VM. These
are no-fault timings at one-million-subject cardinality, not a 10M or chaos gate.
Full stores remain at `/tmp/js-wf-scale-live-1m-20260930` on the measurement VM.
