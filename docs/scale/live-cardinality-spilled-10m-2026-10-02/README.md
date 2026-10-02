# Ten-million-subject live spilled-input proof

Clean binary source160cf8d3a0ec3f8c4a2f17dad8e6f6b0d89ab810; NATS2.15.0,
three separate server processes, file storage, three replicas. Unit is terminal,
inactive/success/exit zero. Exact retained report/audit/logs pass the offline
verifier, which recomputes input digests and compares saved result bytes.
Source-sensitive scale/client/worker/journal/integrity files remain equal to the
recorded commit. Original artifacts are compressed and byte-verified with hashes.

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go build -p=1 -o /tmp/wf-scale ./cmd/wf-scale
GOMEMLIMIT=4GiB GOMAXPROCS=2 /tmp/wf-scale -counts 10000000 -workers 96 \
  -live-workflows 1000 -live-input-bytes 1100000 -min-available-mib 2048 \
  -root /tmp/wf-scale-spilled-10m
/tmp/wf-scale -verify-live -root /tmp/wf-scale-spilled-10m
```

All1000 exact1,100,000-byte inputs spill and complete. Input length/SHA256 and
immutable terminal results match through all three pinned peers; queue drain
passes. Live retained audit is1000 invocations/journals,4000 entries,1000
terminals. Every peer reports10,001,000 invocation and journal subjects,
10,001,000 invocation messages and10,004,000 journal messages. The background
10M subjects per stream are capacity records;1000 live workflows are executed.

Start-to-result p99/max are1.695865211/2.800916200s; live phase including final
audit33.501s. Background fill8m12.637s. Index-only RSS is3108–3239MiB per node;
post-live RSS is3879–3952MiB. Stream-info median169–259us, max201–311us at the
index checkpoint. The2048MiB available-memory guard does not stop the run.
Per-process Go heap target is4GiB; RSS is an observation, not an enforced cap.
The million-timer service remains live throughout. An additional old-store
archive runs during part of fill, so publish time is not an isolated benchmark.

This closes the outstanding10M spilled-input live-cohort measurement at this
source. It does not prove10M active executions, chaos,24h soak, or a universal
throughput/memory ceiling. Raw broker stores remain at
`/tmp/js-wf-scale-spilled-10m-20261002` on this VM.
