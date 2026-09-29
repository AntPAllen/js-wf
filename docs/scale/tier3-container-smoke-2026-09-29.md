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
isolates node four, and completes a workflow on the four-node majority. It
checks for two seconds that the isolated node cannot observe the new result.
After route healing it reads the immutable result through that node, kills
its container, waits for Docker to release that container name, restarts it
on the same file store, reads the result again, and checks the retained
invocation, journal, and terminal outcome.

Run locally with Docker available:

```sh
WF_TIER3_CONTAINER=1 go test ./integration -run '^TestFiveContainerWorkflowSurvivesIsolationAndRestart$' -count=1 -timeout=8m -v
```

The manual `tier3-container-smoke` workflow runs the same proof on a clean CI
runner. This is a five-container topology and one network isolation plus one
server kill/restart. It does not cover the full chaos matrix, clock skew, disk
stalls, client histories, or the 24-hour Tier 3 soak.
