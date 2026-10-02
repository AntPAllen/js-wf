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
must advance it using monotonic elapsed time. A future timer implementation can
use an upper bound at creation and a lower bound for due decisions.

Race tests pass in 1.056s. They exhaust every subset and ordering of five sources
with one clock shifted by ±60 seconds, healthy error endpoints and unavailable
sources. Other tests cover two arbitrary shifted/narrowing clocks, malicious
intersection narrowing, invalid/duplicate/slow observations and overflow of
the configured error window. Unit CI includes the package.

This estimator is not wired to production timer scheduling. Authenticated clock
sampling, durable clock-domain provenance, deadline translation, repair,
backward compatibility and end-to-end simulation/native gates still need work.
It does not repair or accept the existing ahead-clock latency counterexample.
