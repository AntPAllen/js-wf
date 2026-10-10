# Current compiled operator qualification — 2026-10-10

Frozen clean source **73d1dfa** passes all five standalone operator test groups
under race in **108.639 seconds**, actual child and retained service exits0.
`WF_OPERATOR_STANDALONE=1` enables the previously skipped controls. The isolated
sparse checkout retains all module Go/testdata inputs and a clean Git state;
all1,924 source hashes before/after match exact Git blobs. This freezes code
including the terminal visibility repair; later documentation does not qualify
later code edits. [Command/service receipt](state.json), [race log](race.log),
[executed review](review.py), [result](review.json).

The retained CLI binary has race instrumentation, exact vcs.revision73d1dfa,
vcs.modified=false and SHA256
`f15603379a9aff2abfd6629f2520275af4343b689ad575103a7facfa1e82eb20`.
Its NATS client is1.54.0; it does not embed nats-server. Both real leaf broker
binaries/actual process receipts independently verify nats-server2.15.0, cleanup
reaped=true and exit0. An initial reviewer incorrectly required the server
library in the CLI binary; that dependency assertion was corrected before
acceptance. No native test was rerun for the reviewer correction.

## Executed coverage

-68 separate CLI command processes:22 default,23 direct-domain and23 through
 a real leaf. These are the existing legacy operator fixtures.
-8 controlled handshake shutdown processes: project/tombstone-loop × held
 INFO/PONG × SIGTERM/SIGINT, with no unexpected traffic and exit0.
-5 compiled leaf daemon processes: two held pre-forward startups, two running
 shutdowns and one fatal missing-source rejection with expected exit1.
-2 real leaf broker processes, each connected to three identified WFOPS hubs,
 with separate WFEDGE local domain and zero local streams before/after.

Independent framing-aware validators check raw exclusive leaf wire:
[command wire](commands-wire-review.json), [daemon wire](daemons-wire-review.json).
Command captures include21 online connections, two offline replays with zero
network connections and one explicit missing-domain rejection. All forwarded
API subjects match their requested domain. Daemon captures prove the held
startup barrier, running API activity and fatal rejection. Validators are
frozen and checked against Git; review copies must equal their original raw
files. Actual argv, executable hashes, build metadata and disappeared/reaped
PIDs are cross-checked. The1,614-file artifact manifest binds162,223,022 bytes
of retained binaries, stores, process receipts and captures.

Artifacts are under `/home/exedev/js-wf-operator-standalone-artifacts-20261010`
(~164 MiB), source checkout under `/home/exedev/js-wf-operator-standalone-20261010`,
wire review copies under the artifact root with `-wire-review` suffix (~59 MiB).
`js-wf-operator-standalone-current-20261010.service` uses RemainAfterExit=yes,
matching invocationbbe80b000ea5497a947be3bfdfdd9446, MainPID0 and ExecMainStatus0.

## Limits and next work

The existing compiled command fixtures operate legacy history. The full current
canonical graph CLI passes the in-process race suite recorded separately, but
compiled graph-specific commands/projector require their own coverage. This
run does not qualify PostgreSQL, natural server/route/storage faults, broader
matrices or release. It does not resolve the earlier graph signal timeout.
Actual100,000-entry and full current simulation/soak/retention/rollout gates
remain open. The native100,000-grant benchmark continues its original
invocation; admission and collection remain off.
