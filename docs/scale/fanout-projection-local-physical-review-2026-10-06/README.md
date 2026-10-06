# Fanout and projection physical evidence scope correction

The older fanout and projection copied helpers also used three pinned clients'
leader-routed Stream.Info calls. Their retained integrity/results and logical queue/
64durable drain evidence remains valid, but these records do not independently
establish all three local physical stores. The 500-child native drain helper's
observeFanoutPhysicalDrain has the same API-only scope. Earlier all-three physical
wording for these fixtures was too strong. Original archives and verdicts are
retained; native rolling-upgrade /jsz and recent clock copied /jsz proof are distinct.

The copied fanout/projection templates now additionally call the public NATS Server
Jsz monitoring method on each actual in-process server, preserving full snapshots,
node IDs and observation intervals. They require distinct IDs, exactly one local
WF_RUN per server and zero messages/64consumers for a physical-drain success.
Context is checked before and after each synchronous local call; original20-second
budget is unchanged. Snapshots are sequential, not simultaneous. No worker,
acknowledgment, purge or provisioning is introduced by the copied helpers.

Both copied .go templates compile. This is preparation; fresh copied runtime
execution and repaired full500-child native local physical proof are pending.
The direct method is the same public monitoring implementation backing /jsz:
https://github.com/nats-io/nats-server/blob/v2.15.0/server/monitor.go .
Documented per-server monitoring: https://docs.nats.io/learn/monitoring/monitoring-endpoints .
