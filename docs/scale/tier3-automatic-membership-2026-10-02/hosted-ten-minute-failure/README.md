# Automatic membership sustained failure retained

[Run 36951027143](https://github.com/AntPAllen/js-wf/actions/runs/36951027143),
source `c6ca22660675af7ea8cd3e873a6d1d38eef05f19`, fails after 131.14s test time,
about 99s of workload. Three journal-leader cuts heal; the batch-10 checkpoint
reports 280 invocations/journals/terminals and 3,084 entries. Batch 14 completes
at 98.572s; the next timer result read reports `context canceled`. No final
latency, history, queue-drain or full workload acceptance is available.

All 115 retained assignment writes succeeded, but they precede the faults and
do not identify the cancellation cause. The fleet launcher queues errors and
cancels the parent; a batch result failure calls Fatal before the later fleet
error read. Thus a controller or assignment loop error can be hidden by the
secondary cancellation. The retained artifacts do not prove which loop or
operation failed; a server-side cause remains unconfirmed.

The fixture now names each launched loop, logs its error before cancellation,
and retains timestamped `fleet-failures.json` after joining the fleet. This
diagnostic change preserves controller fail-closed semantics, deadlines and all
acceptance gates. A new instrumented sustained run is needed to expose the cause.

All 30 original downloaded/API/log files are retained as gzip with uncompressed
size/hash in `original-sha256.json`; every retained byte was verified. The prior
healthy smoke remains a smoke only; sustained automatic-membership gate is open.
