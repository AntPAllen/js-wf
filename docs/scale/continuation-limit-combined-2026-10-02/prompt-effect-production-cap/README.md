# Corrected production-cap combined recovery accepted

[Run 37045827952](https://github.com/AntPAllen/js-wf/actions/runs/37045827952)
passes at exact `2a301654e7a57c1f66dacdf14b2f36d3bcb10ef6` using the unchanged
production constructor's 100,000-entry cap. Independent `review.py` checks the
Git source hashes, exact compiled guard substitutions, actual race test/package
verdicts and all raw journal/checkpoint/offline/lease/audit proofs.

| Cut after | Preserved prefix entries | Recovery from worker kill |
| --- | ---: | ---: |
| SignalConsumed | 99,998 | 12.840 s |
| StepCompleted | 99,999 | 12.835 s |
| limit Failed | 100,000 | 12.937 s |

Each positive actually SIGKILLs the worker, gracefully shuts down and restarts
all three NATS servers from retained file stores, and recovers through a peer
using the original dispatch. The production lease stays held initially. Each
final journal has exactly 100,000 entries and the limit failure; every prefix
remains identical. Checkpoint index 99,993 and SDK step offset 99,992 verify.
Two staged continuations and 99,995 offline played/recorded steps pass without
forbidden effects. Raw integrity reports one invocation, one journal and one
terminal result. Archive reads stay zero; the frame is read once. Successor
appends use higher lease epochs. All recoveries stay below the unchanged
30-second gate with production TTL12s.

The actual compiled suffix-budget mutation executes exactly one forbidden effect
after takeover. Its retained 100,000-entry journal ends at StepRequested index
99,999; the fixture promptly reports the intended effect failure. No timeout or
compilation failure substitutes for detection. This corrects the earlier
production control's negative timeout; that rejected original is still preserved.

The published originals are archived losslessly with SHA256 readback before
atomic publication. See `manifest.json` and `independent-review.json`. Originals
remain in `/tmp/js-wf-continuation-prompt-production-37045827952`. Hosted physical
stores/binaries were not uploaded and are not included in this preservation claim.

This closes the production-cap combined recovery fixture qualification. It does
not qualify a full fault matrix, current 120-workload 100k suite, million-message
physical drain or full-runtime 24-hour soak.
