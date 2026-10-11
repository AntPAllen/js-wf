# Native append commit phase diagnostic — 2026-10-11

## Terminal closure — 2026-10-11

Actual SDK exit1 after1204.492seconds. All10 old invocations completed; the
20-minute parent deadline expired waiting for active population, reporting
`only 0/10 handlers active: context deadline exceeded`. This endpoint count does
not establish earlier counts, missing IDs or a cause. Four worker-exit messages
in shutdown report `nats: invalid jetstream option: context deadline already
exceeded`; without event timestamps they are not assigned as a prior stall cause.

Active population, purge/reuse, signal commit subphase and final raw audit were
not reached. No new signal-timeout phase or native pass is claimed. Independent
closure review checks2915 Git inputs in the preserved source snapshot, actual
SDK hash/exit/process termination and all2346 native files/10,652,843bytes.
`frozen-inputs.json` records the unchanged source-byte freeze before main's fixture
change; future review uses it instead of assuming currentmain matches oldsource.
The previously active log is now closed and retained. Next investigation uses a
verified store copy to inspect the live invocations without opening the original.

## Historical launch evidence
One R3 canonical active10/10/1 race diagnostic runs on source
`3aa3550d9cb42e54be3928344e94fb379e33ceb0`, with the qualified append commit
phase/readback labels. Original deadlines, retries, active mode and diagnostic
count are retained. It is not the full10k/10k/1k gate.

## Live launch evidence

`live-validation.json` independently binds all 2915 tracked Go/Python/YAML/module
inputs to both Git source and unchanged worktree bytes, the actual live SDK
process/birth identity, retained executable hash, race instrumentation, and its
selected runtime environment. SDK PID317568/start ticks14014973 was confirmed
live at the recorded observation. Its retained executable SHA256 is
`63af6032c190c12cd82b5515674894b125e284bc894a7ac4ffe758e21446dac2`.
`live-observation.json` records the process command and 20-minute scenario /
22-minute SDK bounds. No observation timeout is treated as process termination.

The log reports all10 old invocations complete in28.816seconds. At the live
observation, it has not reported all10 active handlers. No purge/reuse, signal
commit phase, raw final audit, closed-store census or terminal result is claimed.
`review.py` explicitly refuses terminal acceptance while this SDK remains live;
`live-review-refusal.log` retains the actual rejection (exit1).

`run.py` writes the final actual driver/SDK exits and closed native census only
when the process ends. The executable, capture helper and native store remain
under `/home/exedev/js-wf-native-append-commit-phases-20261011`. The active test
log remains untracked until terminal closure; do not stage a changing log or
hash active store files. Unified exec session74676 observes the same driver.
The launch/review scripts added here are evidence tooling, separate from the
frozen SDK inputs. The live observation/validation commands are retained in
session tool history; no standalone creator script was retained.

Next work observes this exact run and applies the terminal reviewer once its
receipt exists. A terminal failure must retain its failed verdict and incomplete
stages. Source/binary/store remain retained; neither a small native pass nor
labelled failure can close the original full-scale requirement or prove a NATS
cause without supporting operation evidence. Three older-source full seeded
race campaigns are separately confirmed running.
