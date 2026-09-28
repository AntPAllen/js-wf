# Projection recovery at 50,000 invocations

The opt-in `TestProjectionRecoversFiftyThousandInvocations` starts a projection
in a separate process and sends it SIGKILL before the workload. Six workers
then complete 50,000 invocations on a three-node JetStream cluster. The test
checks every returned result, 50,000 retained journal subjects, and 100,000
journal messages. The stopped projection's durable consumer reports 100,000
messages of lag. A new projection process rebuilds from retained invocations
and journals, then drains `Lag()` to zero.

The test reads all 50,000 completed rows and their matching status indexes
from `WF_VIEW`. It verifies row identity, status, timestamps, invocation and
journal sequences, and index presence. It then runs a full `Rebuild()` and
compares a digest of every active key, value, and KV revision. The digest is
unchanged, so the rebuild made no projection writes and preserved the exact
stored state. A 100-invocation race-instrumented run also passed.

The full run passed on the 4-CPU, 15-GiB VM. Starts finished in 38.7 seconds,
all results were checked by 46.2 seconds, lag drained by 67.5 seconds, and
the second rebuild and comparison finished by 84.7 seconds. The test took
89.7 seconds including cluster startup and cleanup. An earlier full run with
an in-process stopped projection also passed; the separate-process run proves
recovery after an actual process kill.

`Rebuild` uses 32 bounded workers for independent invocation rows. `Run`
captures the journal's last sequence before rebuilding. A journal message at
or below that watermark was committed before every invocation was scanned,
so its row already reflects that message or a newer one. The durable consumer
acknowledges those messages directly and processes later messages normally.
This avoids rereading every journal for the stopped interval while retaining
the consumer's durable progress.

Reproduce with:

```sh
WF_PROJECTION_SCALE=1 go test ./integration \
  -run '^TestProjectionRecoversFiftyThousandInvocations$' \
  -count=1 -timeout=25m -v
```

Set `WF_PROJECTION_COUNT=100` for a smaller diagnostic run. The proof covers
the KV projection; custom search attributes and a large-deployment query
sink remain open.
