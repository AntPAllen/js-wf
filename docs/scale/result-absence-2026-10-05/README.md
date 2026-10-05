# Result-object absence confirmation — 2026-10-05

## Change and scope

Worker result reads and client `Await` retain the modern Object Store chunk
reader. An `ErrObjectNotFound` is confirmed through the administrative
`STREAM.MSG.GET.OBJ_WF_BLOB` API, with an explicit request context. Missing or
deleted leader metadata remains `jetstream.ErrObjectNotFound`; confirmed
existing metadata permits another modern payload read after 100ms, within the
existing caller budget. Malformed or mismatched metadata fails closed. Normal
successful reads incur no additional request. Content hash verification remains
required by the existing worker/client callers.

Pinned nats.go v1.54.0 modern Object Store metadata uses a direct-capable stream
read. Its legacy `GetInfo` ignores the supplied GetInfo context, and the legacy
administrative option resolver does not inherit the factory context. Therefore
this change calls legacy `GetLastMsg` with `nats.Context(ctx)` explicitly, rather
than replacing the context-bounded chunk reader with legacy `GetBytes`.

Snapshot, manifest, input and other object read paths are unchanged. This is
absence confirmation, not leader routing of every successful metadata read.
It neither forces native follower lag nor establishes any historical failure's
server-side cause. Full matrices/final-source qualification remain open.

## Executed controls

- Baseline source `c247d53` plus the captured new test: FAIL3.71s. Both public
  worker and client reads reject a real committed object after one synthetic
  weak absence. Deletion and content corruption controls pass.
- Corrected normal: PASS4.89s. Each public path makes exactly one administrative
  confirmation followed by a real SDK payload read. Real deletion, hash
  corruption, malformed leader metadata, permanent weak absence deadline,
  blocked leader request deadline and cancellation pass. Both API prefix and
  domain routes are exercised.
- Corrected race: PASS6.02s, same controls.
- Existing 5MiB step spill/replay/corruption: PASS4.71s; terminal
  spill/Await/corruption: PASS5.79s.
- Existing worker `step_get_drop`: PASS21.91s, one effect, two lease heartbeat
  renewals, one bounded timeout and complete terminal integrity. The first
  existing-controls command did not select this subtest; its separate exact
  selection and log are retained.

Baseline and corrected tests share the worker/client weak-absence control;
additional oracle cancellation/malformed controls were added before compiling
corrected binaries. No claim that the complete test files are identical.
All native fixtures are R3 file stores with NATS2.15.0 linked into the actual
recorded SDK test processes. No legacy-server compatibility run is claimed here.

## Preserved evidence

Each archive retains the actual executed binary/build information/live identity,
source archive and matching before/after source hashes, logs, execution verdict,
original native fixture files and post-shutdown file hashes. Baseline1173 and
corrected/race1174 selected repository inputs were read back from source archives.
Each fixture has357 retained files and no visible open descriptors at review.
These selected sources exclude `docs/scale`; they are not an exhaustive external
module/compiler/environment closure. Existing spill/budget controls used their
ordinary temporary fixtures, so those stores are not retained.

`baseline/`, `corrected/` and `race/` contain verified split archives. Concatenate
parts in numeric order to reconstruct `proof.tar.gz`; compare the aggregate and
part hashes in `archive-verification.json` before extracting. Every archive
member and part was independently read back. Original roots remain at
`/tmp/js-wf-result-absence-{baseline,corrected,race}-20261005`.

The native test requires a fresh `WF_RESULT_ABSENCE_ROOT` parent directory to
retain stores and refuses an existing test fixture directory. The exact producer
and review script are included, with executed review output. The baseline
producer used tracked sources plus the new test; the included later producer
also captures untracked helper files.

`live-observation.json` and `live-million-report.json` are point-in-time
observations of the two still-running campaigns. They are not terminal results.
The journal soak has exceeded the prior batch1050 failure; the million candidate
has nonzero transient fetch/ack errors and redeliveries. Final drain fields in a
running report do not establish physical drain. Campaigns and these controls
share the VM; no causal conclusion is drawn from that overlap.
