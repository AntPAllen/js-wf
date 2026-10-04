# Scheduler cleanup server candidate

The accepted fresh two-message fixture isolates a NATS 2.15.0 scheduler-index
cleanup persistence defect. Its reviewed dirty-count change can now be compiled
into a real server binary, with the original module cache and pinned dependency
unchanged:

```sh
python3 scripts/build-nats-scheduler-candidate.py \
  --root /tmp/js-wf-nats-scheduler-candidate-fresh
```

The builder requires the original pinned v2.15.0 module and a fresh directory
outside the repository. It copies every upstream file, modifies only
`server/filestore.go`, captures selected Go/module compiler inputs, checks them
before/after the build and retains the actual executable SHA256/build info.
These inputs are not exhaustive assembly/embed/hermetic provenance. The output
is a diagnostic candidate; no workload result is inferred from successful build.

The existing native timer-volume command accepts it explicitly:

```sh
go run ./cmd/wf-timer-volume \
  -root /tmp/js-wf-timer-candidate-fresh \
  -server-candidate /tmp/js-wf-nats-scheduler-candidate-fresh/nats-server \
  -count 300 -publishers 8 -horizon 90s -lead 30s
```

Every node initially runs the retained candidate copy and both all-node SIGKILL
restarts reuse that exact binary. The report records its digest; offline smoke
verification requires the retained binary and rejects missing or changed bytes.
Release verification rejects the diagnostic candidate profile even with a
complete million-record ledger. This preserves the original million-timer
physical-drain requirement while enabling a public-API, real-process test of
the proposed server fix. The actual 300-timer diagnostic is terminal failed on its unchanged 2-second
raw p99 gate: 10.482509603 seconds, maximum 13.232558335 seconds. Both full
SIGKILL outages occur in its compressed 90-second population. All 300 receipts
and zero messages/pending on all three physical replicas are retained
subobservations; the campaign is not accepted and is not rerun. No original
release limits changed.

The candidate does not repair original campaign stores or explain every original
missed-retirement event. The original million-timer release gate, final-source
matrices and actual 24-hour full-matrix soak remain open.


The corrected candidate build captures 1,435 selected Go/module inputs and
changes only `server/filestore.go` among 598 upstream files. Candidate SHA256:
`ff7335643d02125ec50b8abaa0ca80fe4da36473aa14402c2dbcf029e63c9fe8`.
The actual timer program and all three live candidate server executable hashes
and every build-info field were checked through their live process handles.
Program input copies match pre/post hashes and local Git at81bc1d4. Both
three-process SIGKILL restarts and per-replica zero retained messages/pending
verify; the actual retained program's offline verifier rejects the failed report.
Original stores are retained, not independently reopened. The first build
failed VCS discovery before any binary/server execution; corrected builds disable
VCS discovery in the copied module and use module/source identities explicitly.

[Complete build and failed-native proof](complete-proof/) retains both build
attempts, all upstream/captured input bytes, actual executables, failed reports,
raw receipts, physical metadata, original stores, producer/reviewer and archive
readback checks. Delivery/drain subobservations do not qualify the candidate
server for production or replace the original million/24-hour gate.
