# Seeded canonical start source admission — 2026-10-11

A separate Tier1 workload exercises production client recovery, bound-start repair
and worker admission against an in-memory transport. Ten source modes cross
pending/bound cuts: valid, unrelated transport header, duplicate token,
conflicting hash, token alias, empty/missing hash, empty legacy reference and
empty/aliased absent parent fields. Existing workload decision tables and saved
traces remain unchanged.

Malformed sources must return ErrStartUnknown without enqueuing or changing the
binding. Bound repair must also reject. An explicitly injected bound delivery
must NAK without journal entries, handler calls or effects. The fixture then
restores only headers of the same physical invocation, advances virtual time,
discards client/store handles and recovers from retained state. Original/redelivered
and repaired runs produce one handler call/effect. Legacy journal stays empty;
owned graph objects and dispatch drain after explicit fixture purge/retire/sweep.
Healing is a fixture corruption repair, not production automatic metadata repair.
Parent fields here are absent; full parent causal history remains separate.

## Verification

```sh
SIM_COVERAGE_SUMMARY=1 SIM_PROGRESS=1 FAULT_TRACE_OUT=/home/exedev/js-wf-start-header-model-20261011/normal-failure-trace.json go test ./sim -run '^(TestSeededGraphStartSourceHeadersReplay|TestGraphStartHeaderFailureTracePersistence|TestPinnedRegressionCorpus)$/^(graph_start_source_headers_|default|explicit)' -count=1 -v -timeout=4m

GOMAXPROCS=2 GOMEMLIMIT=512MiB SIM_COVERAGE_SUMMARY=1 SIM_PROGRESS=1 FAULT_TRACE_OUT=/home/exedev/js-wf-start-header-model-20261011/race-failure-trace.json go test -race ./sim -run '^(TestSeededGraphStartSourceHeadersReplay|TestGraphStartHeaderFailureTracePersistence|TestPinnedRegressionCorpus)$/^(graph_start_source_headers_|default|explicit)' -count=1 -v -timeout=5m
```

Final actual exits0 in16.197s normal/165.043s race. Each command completes1000
seed bodies, exactly replays every generated trace and replays all20 new pinned
scenarios. The cleanup counter proves bodies completed, not merely a configured
limit. Both default and explicit nested failure-trace destinations round-trip
exactly.1360 source/fixture hashes prepared before final commands remain unchanged.
Actual final race executable capture is retained; final normal capture was missed.
The source-derived suite inventory is now160 seeded families/880 saved traces.
Only the new family and20 new pins were run here, not all160 families/880 pins.

The exact focused CI guard was extracted and executed against both final logs.
The normal/race workflow job requires1000 completed bodies, all20 pinned receipts
and both persistence controls. Existing full/extended suite discovery includes
the new family and pins automatically. No hosted pipeline execution is claimed.
The workload is registered with the generic replay/minimizer dispatcher.

## Preserved harness development

initial-normal.log passed before the legacy journal reader/writer fixture was
made coherent; its exact test source is in initial-test.go.txt. The subsequent
development normal/race runs passed15.030s/162.234s, with their code/source hashes
and available executable captures retained. Initial normal executable capture
was missed. These are development checkpoints, not final-source qualification.

A failing normal run17.372s then exposed that the new explicit failure-trace path
could not create a missing parent directory. Its log and before-run source hashes
are preserved. The helper now creates parent directories; default traces use
persistent mktemp directories rather than test-owned temporary directories.
Generated or replay failures save the trace before reporting its seed/path.
The final two commands are at that fixed harness source. No failed broker or
qualification run was silently retried.

## Remaining scope

Full160 normal/race qualification, extended10k/100k seeds and original native
fault/scale/soak/rollout gates remain open. Three older-source campaigns remain
live. This does not prove native power durability, clock readiness, complete
parent/source/lease history, orphan projection/retired result ownership or full
rejected-prefix equivalence. Public continuation admission/import/online
collection remain disabled.
