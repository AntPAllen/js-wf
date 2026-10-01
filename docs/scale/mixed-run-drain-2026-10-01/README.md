# Bounded mixed run-queue metadata reads

Base commit: c56bda5c22a5a95c22f24e7150c5a55d0bde4e8b. No production
runtime changes. `source-sha256.txt` identifies the final fixture and helper.

## Failure and change

[Hosted mixed seed 3](https://github.com/AntPAllen/js-wf/actions/runs/36812809142)
returned all 28 outcomes, passed integrity, and exhausted its four-minute
context in the initial restarted-node WF_RUN Stream lookup. The 30-second
queue-drain loop had not started. Monitoring reported zero messages but does
not replace a successful client read or explain the failed request.

The initial lookup and subsequent Info reads now share one 30-second context.
Each attempt, including lookup, has at most three seconds. Only named transport
timeouts/no responders/no stream response/API 10008 retry. Permanent errors,
including a permanent cause joined with a transient cause, fail immediately.
Parent cancellation interrupts reads and polling. Success still requires a
successful client metadata read with exactly zero stream messages before the
deadline. Monitoring is never consulted for success.

## Verification

- Final focused race contracts: PASS, 1.095 seconds. Lost first lookup reply,
  retained-then-drained queue, permanent retention, continuously missing
  metadata, joined hard errors, parent cancellation and named-error controls.
- Unmodified real seed-3 race fixture: PASS, 36.073 seconds, terminal p99
  10.787764825 seconds across 28 invocations.
- First compiled lost-lookup overlay: PASS, 43.553 seconds, p99 15.75165281
  seconds and 34 metadata reads. Its helper preceded the named-error fix.
- Final compiled overlay: first metadata attempt returns ErrNoStreamResponse;
  second waits for its three-second context expiry; later attempts perform real
  pinned Stream/Info reads. PASS, 33.365 seconds, p99 6.88687974 seconds,
  23 metadata reads, all 28 outcomes and final integrity/drain checks.
- Final compiled mutant accepting one retained message: semantic FAIL in
  0.013 seconds (`retained queue accepted`, Msgs=1). Build errors and unrelated
  timeouts are not counted as this control.
- `go vet ./integration` and `git diff --check`: PASS.

Run commands:

```sh
go test -race ./integration -run '^TestMixedRun(DrainReadContract|ReadTransientErrors)$' -count=1 -v
WF_MIXED_CHAOS=1 FAULT_SEED=3 go test -race ./integration -run '^TestMixedWorkflowsRecoverFromFourServerFaults$' -count=1 -v
go vet ./integration
```

Overlay source files are retained as `.go.txt`. To reproduce, map the absolute
repository source path to the corresponding overlay file in a Go `-overlay`
JSON Replace map. The final transient overlay replaces mixed_process_chaos_test.go;
the retained-message mutant replaces mixed_run_drain_test.go.

The retained final schedule selected delay node 0, kill/isolate node 2 and
pause node 1. Hosted seed 3 selected different physical nodes because fault
roles depend on the actual initial journal leader. These are retry controls,
not an exact reproduction or performance comparison with the hosted failure.
The three-second delayed lookup happens after terminal latency measurement.
Monitoring, disk traces, operation events and the final schedule are retained.

The underlying metadata-request failure and mixed latency misses remain open.
This focused pass does not clear the final-source 200-seed full-matrix gate or
the five-node 24-hour soak. Online GC and remaining plan requirements remain
open. The original million-timer campaign and full simulation jobs were not
restarted or canceled.
