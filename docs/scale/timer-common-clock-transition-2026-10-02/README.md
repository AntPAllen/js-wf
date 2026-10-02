# Canonical clock transition and actual suspended repair model

`worker_common_clock_transition` schedules the first of eight sequential timers
on a ±60s leader, removes scheduling quorum, then moves to an unshifted leader
and restores quorum either halfway to due or 500ms after due. The production
worker, SDK, journal reads and `SuspendedScan.Scan` run over virtual transports.
All four timer APIs and both durations produce 32 combinations.

Ahead native hints remain stored far into the future under the model's absolute
deadline assumption; production suspended repair wakes the canonical due timer.
Behind native hints arrive at heal. Before-due hints only replay the suspension;
they cannot complete the wait. Failed common providers stop repair without any
enqueue. An hour-ahead worker clock may choose a retry dedup window but cannot
make a tagged deadline due. Future tagged requests produce no early repair.
After canonical due, repair makes progress; the remaining seven waits use the
unshifted scheduling leader. Late native hints leave terminal journal bytes and
handler call counts unchanged, with the durable consumer drained.

The final retained-state audit finds one terminal invocation/journal and the
expected ordered entries. Durable request clock domains remain preserved.
Canonical completion takes eight durations, plus the 500ms heal delay where
applicable. Early hints add one replay without adding a duplicate suspension.

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 SIM_WRITE_COMMON_TRANSITION_PINS=1 go test -race -p=1 ./sim -run '^TestSeededWorkerCommonClockTransitionRepair$' -count=1 -v
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -race -p=1 ./sim -run '^TestPinnedRegressionCorpus/worker-(common|fresh-timer)' -count=1 -v
```

The 1,000 schedules pass 36.26s / package37.274s, including all32 cells and exact
replay of the first10. All32 new pins plus16 earlier common-clock and4 legacy
fresh-timer pins replay exactly in package3.138s. Total corpus inventory is232;
this focused run does not certify all232. Complete logs/source/pin hashes are
retained. The workload honors SIM_SEEDS and generic failure-trace replay.

Scope: the common provider is ideal, and repair is invoked directly at due/heal.
This does not prove sampler availability, fenced repair-loop cadence, real broker
leadership behavior, final-source100k or native latency. Retained absolute native
hints surviving leader transitions are a transport assumption, not a NATS contract.
R5 common-clock fixture wiring and admitted native cuts remain required; the
ahead60.27s failure and incomplete behind sustained gate remain open.
