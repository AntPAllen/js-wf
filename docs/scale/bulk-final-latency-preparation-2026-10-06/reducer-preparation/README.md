# Shared latency reducer preparation

The final point audit's existing causal rules now have a shared evaluator for
complete logical journals and raw server timestamps. The production runtime is
unchanged. Point reads still use the existing journal reader and fresh timestamp,
child, signal and terminal queries. The reducer normalizes clock offsets once,
preserves event/sample order and rejects missing timestamp census, failed lookups
and cancellation without returning partial samples.

Three race control groups pass (`1.024s`): exact start/timer/child/signal/terminal
samples with zero and ±60-second offsets, nine malformed/impossible-evidence
cases, lookup failures and pre-canceled/late-canceled completion. Command:

```
GOCACHE=/tmp/js-wf-go-build-cache-20261004 GOMAXPROCS=2 GOMEMLIMIT=2GiB go test -race -p=1 ./integration -run '^TestMatrixLatencyReduction' -count=1 -timeout=3m
```

The frozen legacy oracle function body is byte-identical to `36f82cf` after its
function name is changed. Body SHA256:
`6ef4e48938482f94e9f6ad6943c295aea13446b64bc724222ddd74d5506c1408`.
The existing retained 160-real-workflow native test now also compares all samples
from that independent legacy implementation against the shared reducer. Actual
fresh native qualification is pending; no original store is reopened.

This prepares the bulk final latency audit needed for larger soak populations.
Bulk acquisition, memory bounds, snapshot handling, differential scale/fault
checks and adoption are still pending. The existing six-minute final-stage,
20-second per-point and 20/60-second retained audit limits remain unchanged.
Neither synthetic samples nor this refactor qualify full-scale or 24-hour gates.
