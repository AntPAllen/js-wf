# Independent server clock estimate

`Estimate` converts server timestamps bracketed by monotonic caller durations
into an interval for UTC at the common caller anchor. Worker wall time is not
an input. Every supporting intersection needs more distinct identities than
`maxSkewed`, so at least one supporter must have clock error within the supplied
`healthyError`. Widening by the largest observation width prevents an arbitrary
skewed supporter from narrowing the result to an unsafe edge. Excessive RPC
latency, duplicate identities, malformed brackets and absent agreement fail
closed. Results are invariant under observation ordering.

The adapter must authenticate physical server identities, enforce the number
of skewed clocks assumed by the call, and supply real monotonic start/finish
brackets from one anchor. Multiple stream responses from one leader are one
clock source. A returned interval describes the anchor, not return time: callers
must advance it using monotonic elapsed time. Tagged workflow timers use an
upper bound at creation and a lower bound for due decisions.

Race tests pass in 1.056s. They exhaust every subset and ordering of five sources
with one clock shifted by ±60 seconds, healthy error endpoints and unavailable
sources. Other tests cover two arbitrary shifted/narrowing clocks, malicious
intersection narrowing, invalid/duplicate/slow observations and overflow of
the configured error window. Unit CI includes the package.

The opt-in worker CLI shares this clock between tagged SDK timers and elected
repair loops. Native delivery timestamps are scheduling hints; canonical due
decisions use the stored deadline and clock domain. Existing untagged deadlines
retain their legacy interpretation. The default configuration and R5 fault
fixture still use legacy clocks. The existing ahead-clock latency counterexample
remains open pending native admitted-cut recovery evidence.

## Bounded stream sampling

`NewStreamSource` reads a fresh single-replica stream-info response per probe.
It accepts only trusted server names mapped to physical identities and rejects
replicated or migrating streams. Replicated STREAM.INFO admission and leader
metadata construction are separate in the pinned server, so a leader field
alone is insufficient timestamp-producer provenance during a transition.
The single-replica/no-Raft response names its actual producer. The caller must
supply authenticated cluster access, protected reply subjects and trusted topology.

`NewSampler` brackets at most five parallel reads with one monotonic anchor,
limits the collection budget, deduplicates physical sources, and requires the
configured independent agreement. A source must honor cancellation. Unavailable
sources are omitted; invalid successful observations fail closed. `Reading.Bounds`
advances UTC bounds with monotonic elapsed time and rejects expired readings.
The configured healthy-error budget must cover clock/rate uncertainty throughout
the maximum reading age. Readings are private process-local values, not durable
clock records. Two probe names on one server cannot establish agreement.

[Race and native R3 evidence](../docs/scale/runtimeclock-sampling-2026-10-02/README.md)
verifies forwarded identities, alias collapse and replicated-stream rejection.
The existing failed ahead-clock progress gate remains open.

## Opt-in worker configuration

Give each independent physical server a unique placement tag in its NATS
configuration. Supply the operator-controlled server names and stable physical
identities in a topology file, for example:

```json
{
  "probes": [
    {"name":"WF_CLOCK_A","server":"nats-a","identity":"physical-a","tag":"clock-a"},
    {"name":"WF_CLOCK_B","server":"nats-b","identity":"physical-b","tag":"clock-b"},
    {"name":"WF_CLOCK_C","server":"nats-c","identity":"physical-c","tag":"clock-c"}
  ],
  "max_skewed": 1,
  "sample_budget": "250ms",
  "healthy_error": "20ms",
  "reading_age": "1s",
  "refresh": "100ms"
}
```

Add `-timer-clock-config clock-topology.json` to the normal `wf-worker` command.
At bootstrap, also add `-provision-timer-clock` to create or verify the R1 memory
probes while all declared nodes are available. Conflicting existing streams
are rejected without modification. Subsequent starts can omit provisioning;
fresh agreement can survive one unavailable source in this three-source example.
Repair loops must remain enabled (`-reconcile=true`, the default).

The operator must ensure each tag selects exactly its declared physical server,
at most `max_skewed` clocks violate the healthy UTC error bound, and the bound
covers clock/rate uncertainty throughout `reading_age`. Authenticated cluster
access and protected reply subjects are required. Correlated clock errors across
all peers are outside this assumption. Sampling and cache waits honor context
cancellation; expired or failed refreshes do not fall back to stale readings.

Upgrade every worker and repair reader to domain-aware code before enabling
tagged writers. Older binaries can ignore JSON clock-domain fields and make
unsafe due decisions. Provisioning does not perform that deployment migration.

[Topology, shared-clock and actual CLI race evidence](../docs/scale/timer-clock-topology-cli-2026-10-02/README.md)
covers healthy native/fallback timers and independent probe loss. It does not
accept skewed-leader recovery or the full release matrix.

[Native five-container topology evidence](../docs/scale/runtimeclock-docker-topology-2026-10-02/README.md)
verifies per-server placement tags, actual±60s outliers and fresh bounds through
one healthy probe's loss and restart. Post-restart metadata readiness can be
transiently unavailable; sampling continues from independent available probes.
This does not yet accept the mixed R5 workflow recovery gates.
