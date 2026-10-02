# Consumer pending-deadline clock transition characterization

The dispatch model now accepts an optional consumer leader wall-clock offset.
Delivery, progress and delayed NAK deadlines use that clock; controller trace
times and elapsed transport budgets remain virtual elapsed time. Offsets default
to zero for all existing workloads. Changing the offset does not rebase pending
deadlines. This is an explicit model assumption, not a verified NATS election
contract or explanation of a retained queue failure.

A seeded workload runs the production partition dispatch loop across six cases:
initial ±60s consumer clock, with unacknowledged delivery, progress renewal or
100ms delayed NAK, followed by an unshifted leader. It requires exactly two
deliveries, successful final ACK, zero pending deliveries and zero physical
retention. Under the assumed deadline contract, ahead cases redeliver at virtual
controller73s (13s production AckWait) or61s (NAK, with the modeled1s empty-fetch
polling granularity). Behind cases redeliver immediately. These characterized
counterexamples are not acceptance of the under30s recovery target.

All1,000 seeds pass in0.049s; six first-covering traces are pinned and wired into
general replay/minimization, raising the corpus from174 to180. A combined race
check with the existing dispatch workload, production-worker timer clock
characterization and the complete180-trace corpus passes32.209s.

## Relationship to the retained R5 ahead drain failure

Run36938296826's last WF_RUN state records two messages and range endpoints725
and775. Dispatch logs show local lease-held/NAK at725 and local ACK at775, with
no later fetched delivery for either. Public monitoring from the current leaders
shows WF_P_26 and WF_P_43 each at one ACK-pending entry before and after the drain
window. The raw message membership and broker pending timestamps were not
captured, so a direct causal join to the model is unproven. Local ACK/NAK returns
do not prove broker commitment. The joins are retained in
`../tier3-mixed-server-clock-2026-10-01/ahead-corrected-drain-failure/dispatch-consumer-joins.json`.

The model separates one plausible pending-deadline behavior from production
workflow integrity. No production behavior or real-cluster drain target has
been changed on the basis of this hypothesis. The instrumented ahead rerun
36939480705 and behind ten-minute run36939477954 remain in progress.

## Native calibration (2026-10-02)

The initial consumer-clock-only assumption above does not hold for restored
initial ACK-wait state: pinned NATS persists the stored message timestamp.
Progress and delayed NAK do use consumer time. The legacy traces preserve their
hypothesis semantics; the corrected contract has 18 new pins and a separate
workload. [Native evidence and scope](../consumer-clock-native-contract-2026-10-02/)
retain both the failed prediction and the passing durable-consumer proof.
