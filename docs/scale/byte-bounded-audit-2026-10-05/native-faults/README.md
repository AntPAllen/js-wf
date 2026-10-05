# Native byte-bounded delivery fault and invariant controls

Real in-process NATS2.15.0 file replicas (R3), normal executable, GOMAXPROCS=2,
GOMEMLIMIT=2GiB. The source before/after ledger matches all639 selected Go/module
inputs. The SDK live `/proc/PID/exe` hash, complete build information, source bytes,
producer, logs and original stores are archived; servers share this executable.
Not an exhaustive compiler-input inventory; stores have not been independently
reopened. All archive members and parts read back.

## Actual transport faults

- 1500 invocations /6000 entries per case, with and without state snapshots.
  More than one4096-record window preserves a live server-side tail at visit128.
  Actual consumer Info proves R3/memory/AckNone identity and positive pending
  records before the matching native server leader is killed.
- Without snapshot: leader0 /4189 pending at kill;6000 visits and complete terminal
  report recover2.410851624s. With snapshot: leader1 /1904 pending; complete recovery
  2.407533201s. Both stay inside original20s; zero audit consumers remain.
- Cancellation in both state modes stops at128 visits with ContextCanceled and
  the expected partial report; zero audit consumers remain.
- Original default large cohort:12k invocations /144k entries. Actual leader2 kill
  at128 visits with139904 pending; all144k visited and full report completes
  1.960033399s. Named case15.80s including population/setup. No synthetic cut here.

These cases admit actual leader loss, not necessarily bulk-resumption execution:
buffered delivery or the same cursor can survive the cut. Precise resumption
admission is supplied by the separate full R5 synthetic interrupted-window gate.
This does not claim a full100k natural R5 fault gate or full matrix coverage.

## Invariants

Compaction/captured cohort, exclusion of later malformed journals, fresh changed
and missing terminal state, corrupt snapshot digest, restored snapshot and orphan
journals match point-read reports/errors. Five protocol corruptions (epoch owner,
index gap, descending epoch, unresolved terminal and completion without request)
and duplicate invocation detection also match. Named compaction/corruption cases
pass38.74s/8.62s. Both streaming state modes use the byte candidate; point and
non-streaming bulk baselines retain their original transports.

The ongoing million-timer diagnostic shares this VM. This is focused native
transport/invariant acceptance, not exclusive-host performance, final-source
matrix or actual24h qualification. The subsequent public-adoption proof records
the actual streaming API selection change separately.
