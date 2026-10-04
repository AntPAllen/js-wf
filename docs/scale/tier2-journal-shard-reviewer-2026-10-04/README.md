# Reusable independent Tier2 journal shard review

`scripts/check-tier2-journal-shard.py` replaces the copied range-specific review
scripts for completed journal shards. Supply raw GitHub API run, job and artifact
JSON, the complete job log, the extracted original artifact directory and the
requested first/last seed. `--output` must be fresh and outside that directory;
`--temporary-root /dev/shm` keeps compilation scratch off constrained root disk.

The reviewer requires exact job/run/artifact/source/range binding and ordered
ten-minute execution headers. Every named test and package must pass. It compares
job-log results to raw test events, checks all 19 scheduled leader faults per
seed and their timestamps, and regenerates all aggregate/type/progress latency
counts and p99 values from raw nanoseconds. Original artifact hashes must remain
unchanged during review. Explicit exceptions keep these checks active under
Python optimization.

All three production history models are independently compiled and executed on
every uploaded raw history. `go list -deps` enumerates the actual local model
dependency graph, including `retention`, which the earlier manually copied
script's directory list omitted. Each compiled local Go input and module file
must match exact executed Git source before compilation and remain unchanged
after review. The report records these hashes, the helper/binary hashes and the
complete model verdict output. The temporary model executable is not retained;
the helper source is retained. External module versions use the unchanged module
files. The original workflow does not upload a workload executable, pre/post
source ledgers or physical stores; this command does not claim those proofs.

## Executed calibration

Unchanged run **37149506857**, terminal successful job **111287264420**, artifact
**11286892660**, exact source `c4fed061bc614488d4f89b53b216b756490f7da0`:
all twelve already accepted seeds reproduce **31,472 invocations, 346,713 journal
entries and 228 leader kills**, with all three independent history models passing.
The original complete raw inputs remain in
[the earlier accepted shard archive](../current-tier2-matrix-2026-10-03/).

Three unit controls cover terminal shard binding with a failed/active parent,
substituted/incomplete metadata and execution headers, and missing/symlink raw
evidence. An actual first latency sample changed by **one nanosecond**, with its
timestamps unchanged, is rejected under `python -O`; no qualification output is
created. The modified input and rejection are retained. These are reviewer
controls, not new workload executions.

`calibration.tar.gz` retains 13 independently read-back SHA-verified members:
API metadata, complete job log, final review/model verdicts, controls, modified
input and exact reviewer/helper sources. `manifest.json` records all hashes.

The report qualifies only the complete requested journal shard. Parent, full
200-seed row, full Tier2 matrix and 24-hour soak qualification flags remain false.
Live campaigns were neither restarted nor changed. No production runtime code
or new workload acceptance gate changed.
