# Canonical clock fallback scan capacity

The retained behind run36965338232 has healthy native scheduling timestamps for
three canonical timer requests in batch11. After WF_RUN moves to the minus60s
leader, no qualifying shifted-origin request appears before third-cut admission
cancels. The independently joined start history gives invocation sequences315,
316 and318. Actual fallback pages after their start run from73 through161,
eight sequences per one-second scan, and none visits the target before fixture
cancellation. See the [actual correlation](../canonical-r5-native-2026-10-02/behind-ten-minute-admission-failure/scanner-target-correlation.json).
This proves the missing fallback visit. Native-server redelivery causality is
separate; no broker defect is inferred from the absence of delivery.

The new seeded workload runs production SuspendedScan and the fenced cursor
loop with persisted cursor73, one canonical timer due at2s plus1s grace, and
otherwise completed invocations. Its model omits native delivery to isolate
fallback capacity. Immutable concurrent reads avoid adding an artificial read
order to replay. Six cases combine populations336/1000/3000 and8 per second or
256 per100ms. The336 target is313, the first sequence in the same scan page that
contains all three actual targets. It is a page-capacity reproduction, not an
exact replay of all native traffic or Raft elections.

Slow repairs occur at30/113/363 seconds; configured repairs at3.1/3.1/3.5 seconds.
All1,000 seeds, first-ten exact replays and six generated pins pass in3.494s
normal package time. Existing admission negative/control tests pass under race
in1.032s. The new1000-seed capacity race run remains active at evidence creation.
Corpus now258 pins and113 tracked seeded workloads. Current-source full-suite
and100k acceptance remain open.

The clock-row fixture now provisions the existing production scanner at256
sequences per100ms and retains its policy JSON and log. The10s admission budget,
positive duration/source proof,750ms removal lead and strict workflow latency
checks are unchanged. This capacity is for the ten-minute population; it does
not certify400k retained invocations in a24h matrix or real RPC throughput.
The configured scan path retains its32-reader concurrency and4096 page cap.
A new native sustained result is required before claiming recovery.

## Terminal race evidence and changed-source campaigns

The full1,000-seed capacity race attempt is terminal/failed at its300.059-second
package deadline. Its stack shows seed895 running production Scan at cursor953
with budget8 and a runnable WaitGroup; no deadlock is proven. The1000 normal
pass remains distinct. All six pinned capacity traces pass exact replay under
race in1.294s, independently counted from Go JSON. Both original race logs are
retained; no whole1000-seed race pass is claimed.

Changed-source admitted behind ten-minute run36967016515 is queued atc477f00,
with common-clock and every-cut admission enabled. A new full113-workload1000
normal suite runs under authoritative unitjs-wf-tier1-scan-capacity-20261002.
Launch snapshots are retained and are not acceptance. Existing100k and
million-timer handles are unchanged.
