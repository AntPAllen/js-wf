# Live inline workflows at ten million background subjects

Clean binary: `a1995cc5de070842ae775bbac32fd2b1abd4f46d`.
Three separate NATS 2.15.0 processes, file storage, three replicas.

```sh
go build -o /tmp/wf-scale ./cmd/wf-scale
/tmp/wf-scale -counts 10000000 -live-workflows 1000 \
  -live-input-bytes 64 -min-available-mib 2048 -root /tmp/wf-scale-inline-10m
/tmp/wf-scale -verify-live -root /tmp/wf-scale-inline-10m
```

All 1,000 production workflows completed with 64-byte inline inputs, matched
input digests, immutable results through every pinned peer, and a drained queue.
The retained-state audit found 1,000 journals, 4,000 entries and 1,000 terminals.
Every node reported 10,001,000 subjects per stream, 10,001,000 invocation messages
and 10,004,000 journal messages. Offline verification passed afterward.

Client start-to-result p99/max were 420/1,137 ms, including initial consumer
startup. The live phase including final audit took 10.67 seconds. Final server
RSS was 3,159–3,270 MiB. The memory guard did not fire; the million-timer campaign
was running concurrently throughout. These are no-fault live inline observations
at 10M cardinality, not spilled-input evidence at 10M or a chaos/release gate.
Timing, effects, spill and RSS are recorded observations. The retained stores
remain at `/tmp/js-wf-scale-live-inline-10m-20260930` on the measurement VM.

## Storage archival (2026-10-02)

The broker-store files at the measurement root are now losslessly compressed
under `broker-store-archive/`. Reports/audits/logs remain unchanged. Per-file
hash verification, manifests and the fresh-directory restore command are in
[archive evidence](../terminal-scale-store-archives-2026-10-02/).
