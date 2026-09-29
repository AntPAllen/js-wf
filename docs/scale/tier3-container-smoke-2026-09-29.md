# Five-container cluster smoke

The opt-in `TestFiveContainerWorkflowSurvivesIsolationAndRestart` uses five
NATS servers built from the repository's pinned `nats-server` module version.
It builds a local scratch image and runs each node in its own Docker container,
with a separate file store. A client network keeps every pinned host port
available; a second network carries all server routes. Disconnecting one node
from the route network isolates that server without hiding its client port.
The fixture reads `/routez` on each server and waits for established route
sockets to close before it starts the majority workload; Docker can leave those
sockets alive for about 90 seconds after network disconnection.

The test provisions the workflow stores at five replicas, starts a worker,
isolates node four, and races eight `Start` calls from clients pinned to the
four-node majority. It requires one start and seven matching duplicates, then
checks the recorded start history with Porcupine. The workflow suspends on
`AwaitSignal`; the majority sends one signal, retries its key with the same
payload, and rejects a retry with a different payload. Porcupine checks that
signal history, and the final journal must contain one `SignalConsumed` entry.
The workflow completes on the majority. The test checks for two seconds that
the isolated node cannot observe the new result.
After route healing it reads the immutable result through that node, kills
its container, waits for Docker to release that container name, restarts it
on the same file store, reads the result again, and checks the retained
invocation, journal, and terminal outcome.
It checks the successful result-read history across healing and restart with
Porcupine. The manual CI workflow uploads the client history as JSONL; set
`WF_TIER3_HISTORY_OUT` to save it during a local run.

Run locally with Docker available:

```sh
WF_TIER3_CONTAINER=1 go test ./integration -run '^TestFiveContainerWorkflowSurvivesIsolationAndRestart$' -count=1 -timeout=8m -v
```

The manual `tier3-container-smoke` workflow runs the same proof on a clean CI
runner. This covers one network isolation and one server kill/restart, with
client histories for starts, signals, and results. It does not cover the full
chaos matrix, clock skew, disk stalls, or the 24-hour Tier 3 soak.

The same manual workflow also runs `TestFiveContainerAckedPublishesSurviveLeaderKill`.
That test creates a five-replica file stream, launches 1,000 concurrent publish
attempts from a surviving node, and SIGKILLs the current stream leader at a
seeded attempt. It treats unacknowledged publishes as unknown, requires
acknowledgments after the kill, and checks every retained sequence and
acknowledged payload through a survivor and through the killed node after a
same-store restart. Local seeds 1–4 passed, with 956–964 acknowledged writes
and 414–525 acknowledged after the kill. The [combined clean CI run](https://github.com/AntPAllen/js-wf/actions/runs/36606118005)
passed both container tests; its seed-1 leader kill acknowledged 960 writes,
including 493 after the kill, and uploaded the 14-operation workflow history.
The fixture mounts a NATS config that explicitly sets `sync_interval` to
`2m`, the pinned server's default. Set `WF_TIER3_SYNC_INTERVAL` to the
production server value to run the same proof with that interval; positive Go
durations and `always` are accepted. Local leader-kill runs passed at `2m`,
`1s`, and `always`. The [clean `2m` CI run](https://github.com/AntPAllen/js-wf/actions/runs/36609558149)
passed both tests with 964 acknowledged writes, 511 after the kill. The
[clean `always` CI run](https://github.com/AntPAllen/js-wf/actions/runs/36609575682)
also passed both with 964 acknowledged writes, 536 after the kill. The
production deployment value and block-device delay remain to be exercised in
the full Tier 3 matrix.

The manual CI workflow accepts the same value as its `sync_interval` input,
for example `gh workflow run tier3-container-smoke.yml -f sync_interval=always`.

The first clean CI run passed in 124 seconds. After a three-run local repeat
exposed a Docker `--rm` cleanup race on restart, the fixture began waiting for
container-name removal. Three corrected local runs passed, followed by a
[clean CI pass](https://github.com/AntPAllen/js-wf/actions/runs/36601536302)
in 128 seconds. An [observed-history CI run](https://github.com/AntPAllen/js-wf/actions/runs/36602929845)
passed in 121 seconds and uploaded 11 operations covering eight starts and
three result reads. The subsequent signal-wakeup extension passed locally in 109 seconds, then
passed three fresh-cluster repeats in 112.68, 111.93, and 111.82 seconds. Its
[clean CI run](https://github.com/AntPAllen/js-wf/actions/runs/36604281917)
passed in 125 seconds and uploaded 14 client operations, including the signal
retry and payload mismatch.
