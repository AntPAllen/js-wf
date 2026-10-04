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
  -count 300 -horizon 90s -lead 30s
```

Every node initially runs the retained candidate copy and both all-node SIGKILL
restarts reuse that exact binary. The report records its digest; offline smoke
verification requires the retained binary and rejects missing or changed bytes.
Release verification rejects the diagnostic candidate profile even with a
complete million-record ledger. This preserves the original million-timer
physical-drain requirement while enabling a public-API, real-process test of
the proposed server fix. This diagnostic remains prepared, not qualified.

The candidate does not repair original campaign stores or explain every original
missed-retirement event. The original million-timer release gate, final-source
matrices and actual 24-hour full-matrix soak remain open.
