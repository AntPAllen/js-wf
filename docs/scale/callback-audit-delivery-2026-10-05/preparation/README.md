# Explicit callback retained scanner preparation

Private scanConsumeByteBoundedThrough uses the shared retained scanner invariant
reduction, captured bounds, gap/leader absence oracle, replay confirmation and
two-resume budget. It keeps a Consume cursor across4096-record windows and uses
the same8MiB pull buffer/2s expiry. An unbuffered callback channel adds no payload
queue; first reported transport error is preserved for bounded shared recovery.

Each cursor Stop releases a blocked callback before unsubscribing. Cleanup joins
Consume.Closed within the remaining caller deadline (or capped2s without a
deadline) and reports join failure. Defaults/public APIs retain the SDK iterator.

Unit race controls for record-window continuity, cancellation of blocked producer,
error preservation, bounded join and existing byte/compact behavior pass. Native
full report/corruption/state/compaction, payload/gap/cutoff/cancel and consumer
leader-loss controls are prepared; no native acceptance or400k pass claimed.

This remains experimental. Quiet100k directConsume cost evidence motivates the
measurement, but the adapter candidate may have different performance.
