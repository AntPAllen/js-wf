# Indexed full-entry fixture preparation — 2026-10-10

The existing native continuation-limit fixture now accepts explicit test options:

- WF_GRAPH_CONTINUATION_OWNER_INDEX=1 selects newly provisioned indexed authority
  streams and the matching authority adapter. Existing default runs stay legacy.
- WF_GRAPH_CONTINUATION_COMPACTION_TTL selects only CompactionIntentTTL; ordinary
  append lifetime and production entry cap remain unchanged.
- WF_GRAPH_LIMIT_PORT_PROFILE=1 preserves incremental owner discovery when the
  selected store is indexed. The legacy profiler retains its original interface
  set and cannot advertise indexed completeness.

The indexed profiler delegates owner census, marker validation and incremental
scan opening without altering arguments, results, errors or retry policy.
Existing completed-call timing counters now include those operations. The
native cursor itself remains responsible for bounded discovery and completeness.
This closes a fixture gap that would otherwise send the eventual full-entry
run through legacy namespace enumeration despite enabling the indexed store.

## Evidence

A new R1/R3 stored-renewal recovery selection wraps the native journal with the
same profiler used by the limit fixture. Controlled authority time triggers
renewal, saves pending input before updates, interrupts after a bounded batch,
and reconstructs across three fresh workers. It requires two incremental scans,
marker validation, zero static/full census, unchanged source head, independent
verification, descriptor deletion, released readers and exactly one next-stage
call. Restored race logs and a deliberately hidden discovery capability check
are preserved with source fingerprints.

The indexed/durable/profiled limit selection uses private budget 20 with explicit
three-hour compaction lifetime. Two real padding calls cross two SDK checkpoints;
it audits ordered entries, terminal slot 19, zero forbidden effects and both
prefix handlers called once. It is a small configuration/control fixture.
Production budget 100,000 is still available with 49,992 real SetState calls;
no actual full-entry run is launched by this preparation.

## Accepted controls

Profiled R1/R3 recovery passes race 23.235 seconds; hiding discovery fails both
cases. Restored recovery passes 36.451 seconds. Indexed durable budget-20 R1/R3
passes race 58.021 seconds; legacy profiled R1 passes race 17.300 seconds.
The reviewer accepts these scoped controls and no full-entry scale result.

## Reproduce

```sh
go test -race ./worker -run '^TestNativeGraphIndexedProfileStoredRenewalFreshWorkerRecovery$' -count=1 -v
WF_GRAPH_CONTINUATION_LIMIT_BUDGET=20 WF_GRAPH_CONTINUATION_DURABLE=1 WF_GRAPH_CONTINUATION_OWNER_INDEX=1 WF_GRAPH_CONTINUATION_COMPACTION_TTL=3h WF_GRAPH_LIMIT_PORT_PROFILE=1 go test -race ./worker -run '^TestNativeGraphContinuationGlobalLimitAndTerminalSlot/R[13]/archive=true$' -count=1 -v
python3 docs/scale/graph-indexed-limit-preparation-2026-10-10/review.py
```

The source-bound 100,000-native-grant diagnostic continues separately. These
worker-only test changes do not alter its frozen package or supervisor sources.
Its acceptance and actual 100,000-entry qualification remain open, along with
original broad gates. Public admission and collection remain off.
