# Mixed seed 12 latency failure at 7e414b2 — October 1, 2026

GitHub run 36810326702 passed mixed seeds 1–11 then failed seed 12 before the
final integrity audit: terminal p99 40.573740256 seconds, required under 30.
The source was 7e414b21e8d108eb527a72bd1b7e8a8370ca8fa0. The two slow signal
invocations mixed-02-0/mixed-06-0 completed 40.574/39.355 seconds after their
enabling signals. This is a failed release gate; eventual completion does not
clear latency or establish the final integrity audit.

The retained schedule applies persistent 85 ms disk latency to node 2, isolates
node 0, pauses node 1 at 18 ms and kills node 0 at 31 ms. Operations from the
replacement for each slow signal invocation include 48 pre-append lease renewals
with zero errors. Their measured KV update sums are 39.293/38.436 seconds; local
lease gate waits sum to only 60/76 microseconds. The sums are invocation-local
client measurements, not a claim about total server CPU time. Repeated updates,
not one monolithic wait, account for much of the observed latency.

`operation-summary.json` groups the preserved raw events by replacement worker,
invocation and operation. `ci-failed.log`, schedule, operation events, server logs,
disk events and pre-fault stream/Raft attribution are the downloaded CI evidence.
There is no established causal connection to the canceled-timer fix at this
revision, and no confirmed underlying server-side cause. The runtime's fencing
and pre-append renewals have not been weakened. Seed 65 remains independently
unresolved. A modeled reproduction or API-level causal contract remains needed.
