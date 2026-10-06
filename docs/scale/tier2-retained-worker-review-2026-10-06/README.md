# Reusable retained Tier-2 worker review

`review-tier2-retained-workers.py` reviews closed original normal ten-minute pause
and reply-isolation rows for arbitrary recorded seeds. It binds native duration,
all six workload cells, selected Git and captured external inputs, producer/helper
bytes, actual SDK/worker/server bytes and observed closure. Ten active faults must
follow the original one-minute scheduled cadence and have exact matching fencing
within the recorded fault observation interval. Pause ownership is keyed by
invocation/epoch; isolation is keyed by worker/type/id/run sequence/delivery and
requires held replies, forwarded requests, zero overflow and fresh PING recovery.
Timestamps retain nanoseconds. Unsupported rows fail rather than inherit a gate.

The command does not open NATS stores. Independent copied integrity/history/drain
and complete archive preservation remain separate. Native paused fixture d893bb0
passes the reusable review; four control tests exercise both valid patterns and
35 wrong timing/identity/ownership/transport/fencing variants. This does not
establish hermetic compiler inputs, full matrix or current-source blanket acceptance.

```sh
python3 scripts/review-tier2-retained-workers.py --root /absolute/closed-native-root --output /tmp/native-worker-review.json
```
