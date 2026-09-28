# Ten-thousand-sleep measurement

`WF_TIMER_SLEEP_SCALE=1 go test ./integration -run '^TestTenThousandRandomSleeps$' -timeout=12m -v`
starts 10,000 distinct invocations on a three-node JetStream cluster. A fixed
seed selects an integer sleep from 1–60 seconds for each invocation. Six workers
own all 64 partitions. The test reads each journal's recorded `fire_at`, checks
the terminal result, and measures the time from `fire_at` until the handler
returns from `wf.Sleep`. It also checks that every timer was scheduled and
fired, `WF_RUN` drained, no sleep returned before its deadline, and no
invocation completed more than five minutes after its own deadline.

The VM had 4 CPUs and 15 GiB RAM. `worker.WithPartitionConcurrency` bounds
the number of run messages handled at once by each partition loop. One is the
default. The invocation lease prevents two handlers for the same workflow from
running together; the test also keeps an atomic in-process marker per ID.

| Per-partition concurrency | Start duration | Drain and result audit | Handler lateness p99 | Maximum |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1.34 s | 72.95 s | 7.76 s | 11.80 s |
| 4 | 3.04 s | 70.04 s | 5.45 s | 7.07 s |
| 8 | 4.49 s | 68.52 s | 3.49 s | 4.54 s |
| 16 | 5.96 s | 68.08 s | 1.99 s | 2.57 s |
| 32 | 9.39 s | 69.93 s | 854 ms | 1.20 s |
| 32, repeat 1 | 9.29 s | 69.93 s | 760 ms | 956 ms |
| 32, repeat 2 | 9.80 s | 70.28 s | 861 ms | 1.17 s |
| 32, after shutdown fix | 9.01 s | 69.45 s | 855 ms | 1.07 s |

All four 32-concurrency runs met the plan's no-fault p99 target of under two
seconds. The median handler lateness in those runs was 175–177 ms. Every run
scheduled and fired exactly 10,000 timers, and the last three runs recorded
zero early or five-minute-late completions. A 100-invocation, 1–3-second race
run also passed. A focused three-node test proved two handlers can enter on one
partition and both stop when the loop is cancelled; five normal runs and one
race run passed. The regular `go test ./... -count=1 -timeout=25m` suite passed
before the final shutdown cancellation change (integration: 673.7 seconds);
the focused shutdown and 10,000-sleep tests passed afterward.

The worker's existing wakeup buckets placed every scheduled message within
500 ms of `fire_at` in these runs. Those buckets use the message timestamp;
the handler measurement above includes time queued for a worker and is the
relevant figure for the plan's completion target. These rows are separate
end-to-end runs, so the changing start duration also changes how densely
deadlines overlap. The chaos target of p99 under 30 seconds remains unmeasured.
