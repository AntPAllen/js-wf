# Four-core full400k copied capacity comparison

Native test at 1f536536cafaf70bdb309a0176e027f687587912 failed in77.25s.
GOMAXPROCS4 /GOGC200 /GOMEMLIMIT2GiB are explicit recorded changes.
R5 file stores retain the full400k invocations /4.8M journal entries /400k states.

| Attempt | Elapsed | Allocated bytes | GC cycles | Verdict |
| --- | --- | --- | --- | --- |
| SDK concurrent | 20.000691521s | 4,524,165,336 | 25 | Deadline exceeded |
| Compact concurrent | 20.000073977s | 3,395,710,760 | 11 | Deadline exceeded |
| SDK recheck | 20.000036348s | 4,312,528,176 | 19 | Deadline exceeded |

Partial reduction counters are not full integrity reports or proof of missing
stored data. Allocation differences across incomplete attempts are not per-record
or throughput comparisons. The changed configuration alone has not qualified
capacity; no reader default adoption, server defect or24h qualification is claimed.

Independent review checks673 selected Git inputs, actual SDK executable, five
observed server binaries/modules/mounts, closed processes and1058 unchanged original
store files. This source capture does not cover every external compiler input.
Complete closed evidence is preserved in a read-back-verified1707-member /24-part archive (605,617,086bytes). The tracked reviewer service exits successfully; archive metadata records every part and concatenated SHA.

Next work should measure iterator heartbeat and adapter delivery overhead.
Another unchanged configuration rerun would not resolve the capacity failure.
