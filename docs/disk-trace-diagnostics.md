# Correlating mixed-chaos disk delays with lease calls

The mixed fixture keeps its disk delay active while results recover. Lease call
latency therefore needs to be compared with actual delayed store syscalls,
in addition to local gate waiting and journal append timing.

`ProcessCluster.SlowDisk` now enables strace Unix timestamps, syscall durations
and decoded file descriptor paths (`-ttt -T -y`). Its existing exact-path filter
and injected delay remain unchanged. The mixed fixture retains the flushed
trace on success and failure as `<schedule>-server-<node>-disk.log`; the CI
server-log artifact glob includes it. The trace can contain interrupted calls
at detachment, so completed totals are explicitly a subset.

Before faults, the fixture records bounded HTTP JetStream snapshots with stream
and consumer Raft group IDs. This uses the documented [JSz monitoring options](https://docs.nats.io/reference/system/monitor/jsz),
independently of the NATS client connection. Snapshots let the analyzer map
opaque `$SYS/_js_/...` file names to their stream or consumer. Missing mappings
remain unattributed. The two-second diagnostic request bound prevents a stalled
monitor from blocking fault injection; unavailable snapshots are logged.

```sh
python3 scripts/summarize-disk-trace.py path-server-2-disk.log \
  --operations path-operations.json \
  --jsz path-server-0-jetstream-before.json \
  --output disk-summary.json
```

The parser joins unfinished/resumed calls by thread and syscall, preserving
the original timestamp and file path. It reports incomplete and unmatched
calls rather than assigning them fabricated durations. The summary groups
completed delayed syscalls and counts which overlap each slow renewal's
approximate client update window. Operation `At` is the whole-call finish;
subtracting the KV duration approximates its final measured stage. Return and
observer overhead make this an approximation. Concurrent syscall sums are not
elapsed critical-path time, and temporal overlap is not RPC attribution.

## Retained seed-17 replay

[Raw trace, operation events, JSz snapshot, schedule and summary](scale/mixed-disk-trace-seed17-2026-09-30/summary.json)
come from the local race replay after adding this instrumentation. It passed
in 50.68 seconds, with terminal p99 17.87 seconds across 28 invocations and a
90 ms disk injection on node 2. Of 2,013 completed delayed calls, 159 touched
lease message files and 248 touched the lease stream's Raft files. Journal
message files had 113, and its stream Raft files one. Fifty Raft-file calls
could not be mapped by the pre-fault snapshot; five calls were unfinished at
detachment. The completed lease message/Raft sums were 14.35/22.39 seconds,
with maximum individual durations 96.57/100.20 ms. These sums overlap other
threads and do not represent a single invocation's wait.

The earlier [CI campaign at `b25b889`](https://github.com/AntPAllen/js-wf/actions/runs/36773536279)
passed seeds 1–16 and failed seed 17 with terminal p99 58.88 seconds. Its operation
events showed late signal append renewals taking roughly 0.37–0.54 seconds
inside KV updates, and child updates near 0.71–0.73 seconds. That campaign did
not retain the disk trace or group mapping, so this later passing interleaving
cannot explain it or establish a server bug. The new artifacts give a direct
way to investigate the next failure without relaxing the latency gate.
