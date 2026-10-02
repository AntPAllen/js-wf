# Continuation budget across worker kill and cluster restart

Three actual race-instrumented cuts pass at a private 20-entry test budget:
after SignalConsumed, after StepCompleted, and after Failed. Each uses a real
worker subprocess SIGKILL while its production 12-second lease remains held,
then shuts down all three NATS servers and restarts their retained file stores.
Server shutdown here is graceful; only the worker receives OS SIGKILL.

Replacement consumes the original unacknowledged dispatch and restores the
checkpoint frame without archive reads or prefix handler reexecution. The
journal remains exactly 20 entries with its original prefix and one terminal
ErrTooLong, matching immutable state on every peer. No forbidden effect runs.
Offline replay follows both continuations, consumes all recorded SDK steps,
and stops at the retained limit request without executing that effect.

The compiled negative control replaces only the three absolute-index budget
checks with restored-suffix-length checks. The actual after_signal test fails
with `limit outcome changed: <nil>`: its journal has 22 entries, ends Completed,
and the forbidden effect executes exactly once. This proves semantic detection,
not merely a failed compiler or timeout.

`originals.tar.gz` retains all 55 non-store files from the final runner, including
raw journals, cut prefixes, lease/frame evidence, Go JSON events, exact mutant,
source hashes, runner result, and independent review. Every member was compared
by SHA256 after compression; `manifest.json` records the hashes. Original stores
remain at `/tmp/js-wf-continuation-limit-combined-contract-20261002`.
Worker vet passes. The final source hashes match the pending implementation.
Earlier fixture failures and original stores remain in their temporary roots.

This closes the local combined contract at budget20 only. The opt-in workflow
also accepts the production default100000 without overriding the worker cap;
that qualification is still required. This does not clear the full matrix,
24-hour runtime soak, or Tier1 modeling of these combined cut boundaries.

## Hosted budget20 acceptance

[Run37037252985](https://github.com/AntPAllen/js-wf/actions/runs/37037252985)
completed successfully at exact sourcee507a96. Downloaded source hashes match
every corresponding Git file, and the compiled control is byte-identical to
the specified three budget substitutions. Original positive/negative events,
all three retained-prefix journals, final ErrTooLong outcomes, higher takeover
epochs, and zero live/offline effects validate independently. The control
produces22 entries and exactly one forbidden effect. All published originals
and terminal run/job metadata are losslessly archived under `hosted-budget20`,
with every member SHA256-compared after compression. The production100000 run
37037256768 remains active; budget20 does not certify that gate.

## Production-cap run: all positives pass, negative control times out

[Run37037256768](https://github.com/AntPAllen/js-wf/actions/runs/37037256768)
is terminal failed at exacte507a96. All three intact-worker production100000
cuts actually pass, preserving99998/99999/100000-entry cut prefixes and exactly
100000 final entries, terminal ErrTooLong, zero forbidden effects, frame-only
recovery and production lease fencing. Recovery is12.590s/12.383s/12.395s.
Source hashes and all raw prefixes/final journals were independently checked.

The suffix-budget mutant actually executes the forbidden effect1120 times after
replacement. The journal's separate hard100000-entry cap prevents the terminal
completion that the small-budget negative-control verifier expects. The fixture
waits for a terminal result and ends at its25-minute context deadline; the runner
correctly rejects that verdict as `wrong detection`. There is no final mutant
journal artifact. This is not a green overall qualification and is not an
intact-worker failure. It establishes that the negative test needs to detect the
first forbidden effect promptly rather than require impossible completion beyond
the production journal cap. Preserve the original negative timeout and rerun the
corrected control before accepting the combined production-cap qualification.

`production-cap-partial` preserves all54 downloaded/metadata/review files,
including the original positive and rejected negative events, handler logs,
prefixes, frame/lease evidence and terminal failed-job log. Every member was
SHA256-compared on readback. Hosted physical stores/binaries were not uploaded
and are not claimed to be preserved in that archive.
