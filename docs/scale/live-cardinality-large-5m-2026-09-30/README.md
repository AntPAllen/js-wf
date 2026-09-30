# Five-million-subject live input-spill proof

Clean measurement binary: `151735c22700562ca49df8f3674a947781db0c17`.
Three separate NATS 2.15.0 processes, file storage and three replicas.

```sh
go build -o /tmp/wf-scale ./cmd/wf-scale
/tmp/wf-scale -counts 5000000 -live-workflows 1000 \
  -live-input-bytes 1100000 -min-available-mib 2048 -root /tmp/wf-scale-large-5m
/tmp/wf-scale -verify-live -root /tmp/wf-scale-large-5m
```

All 1,000 exact 1,100,000-byte inputs spilled and completed. Each immutable
result matched length/SHA256 through all three pinned peers. The production
cohort integrity audit passed with 1,000 journals, 4,000 entries and 1,000
terminals. Every peer reported 5,001,000 subjects in each stream, 5,001,000
invocation messages and 5,004,000 journal messages. The queue drained. The
saved report and audit pass the offline verifier, including recomputation of
expected input digests and comparison against saved terminal-result bytes.

Start-to-result p99/max were 1.422/1.687 seconds; the live phase including
final audit took 28.40 seconds. Final server RSS was 3,852/3,362/3,511 MiB,
compared with 1,900–1,992 MiB at the index-only checkpoint. Stores together
occupy 4.6 GiB. The memory guard did not fire. The million-timer campaign was
live concurrently; these are no-fault observations under that load, not a
10M or chaos gate. Timing, RSS, effect execution and spill counts remain
recorded observations; offline verification checks their consistency and
retained state, rather than independently re-observing the original execution.

The retained race log verifies the real inline/spill cohort contract and ten
negative offline controls, including a coherent but wrong terminal result.
The stores remain at `/tmp/js-wf-scale-live-large-5m-20260930` on the VM.
