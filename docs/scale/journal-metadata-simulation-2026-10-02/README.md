# Deterministic initial journal metadata recovery

The live-read port now optionally supplies metadata preparation. Production
Read runs both JetStream preparation and modeled preparation through the same
bounded request policy. The modeled handle uses the same handle-cache code;
successful preparation is retained, failures are not, and canceled callers
cannot start a load. Existing ports omit preparation and keep their traces.
This is a narrow lookup operation, not an emulation of the JetStream interface.

The new `journal_initial_metadata_recovery` workload runs one cooperative read
actor over a retained four-entry journal. Its seeded modes are normal, one/two
timeout responses, no responders, transient unavailable, three exhausted
attempts, permanent failure and caller cancellation. Timeout faults advance
virtual time by2000ms without sleeping. Retry backoff uses production's25ms
delay through the virtual transport. Exhaustion reaches6050ms; a later caller
recovers. Failed preparation cannot expose records or a tail. Successful reads
preserve every stored entry/sequence, and cached reads leave a newly queued
metadata fault untouched.

The transport verifies that each preparation request has the production's
bounded deadline. Actual deadline timestamps are never recorded or used for
seeded scheduling; timeout responses are explicit injected outcomes. The real
R3 proxy contract separately proves elapsed-time behavior. This slice does not
model Raft, concurrent metadata loaders or the server cause of prior stalls.

All1000 seeds and eight modes pass under race in0.45s. Seeds1..10 replay
exactly; seed42's exhausted-attempt trace is pinned and replays under race.
Two separate processes produce byte-identical trace files matching the pin.
The refactored real metadata contract passes under race in5.28s.

A shared-source overlay removes only the bounded lookup call. The model fails
its named test with `metadata request is unbounded`; the actual R3 proxy test
fails its named four-second retry gate. The first bootstrap overlay produced an
unused-variable build failure, which is preserved and not counted. The corrected
control retains the unused callback variable explicitly and both tests actually
execute and fail semantically. Its model failure trace is retained outside the
test's temporary directory. Original control logs/sources/trace and positive
race events are losslessly archived/compressed with verified hashes.

This adds the117th modeled workload and262nd pinned trace. Focused evidence is
based on the `4c7601f` worktree before commit. The complete1k gate at `82f323a`
is accepted: actual1..1000 completed bodies for every117 source-inventoried
workloads,262 pins,161 top-level passes and two permitted trace-only skips.
It records119033 schedules,1788011 choices and26857656 transport events in
123.68s package time. Current sim/journal source files match that exact Git
revision; original events, compiled/source inventories and report are archived
with verified member hashes in [full1000-pass](full1000-pass/).

Hosted shared-model/real run37011695790 at `82f323a` is independently accepted:
all five named race tests pass, the metadata loop completes actual1..1000,
and the exact shared unbounded-source overlay makes both model and real tests
fail semantically. The model failure trace is retained and source hashes match
Git. Original events, overlay source, full logs and terminal metadata are
losslessly archived with independent checks in [shared-ci-pass](shared-ci-pass/).

The new graph's100k release campaign remains open. Run37012116265 is live at exact source `82f323a` for
100000 seeds per workload. The live116-workload run37002783868 remains evidence
for its exact prior graph and is not restarted or relabeled.
