# Quiescent blob-sweep retained metadata validation

The production adapter now enumerates filtered retained subjects through
paginated `Stream.Info`, then reads each subject's last retained message. The complete filtered subject map must match the
stream's total subject count. Object chunk subjects are included in that census
and then excluded from object metadata decoding; unknown subjects fail closed.
It uses raw KV tombstones and object deletion metadata rather than watcher
initialization markers. Object names must match their encoded metadata subjects
and local bucket. Unknown KV operations/markers and malformed/mismatched object
metadata abort before the sweep enters its deletion loop. An unknown Object Store subject
control separately passed under race in 4.28 seconds.

The [NATS KV design](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-8.md)
describes nonempty `KV-Operation` headers as retirement markers and defines the
three supported `Nats-Marker-Reason` values. This adapter accepts the known
retirement values and fails closed on unknown metadata. It reads the fixed
runtime buckets, not arbitrary KV mirrors or Object Store links across buckets.
The client library's paginated subject API supplies enumeration; this small
contract suite does not test a greater-than-100,000-subject metadata page boundary.

The real predecessor adapter at `a1995cc` failed the new unknown-operation
control: it returned success after deleting one orphan. A Go overlay disabling
the new production decoder guard makes the Tier 1 workload fail at seed 15
with the same deletion result. The seeded workload uses the production metadata
decoders with shared modeled retained KV/object state and the production sweep,
checks zero deletions under each fault, repairs it, then checks correct reclamation.
It does not model watcher closure, Raft, or the earlier intermittent listing miss.

Four modes (JSON/name/bucket/KV operation) are pinned at seeds 1/2/3/15.
Exact replay and separate-process trace identity pass. The combined model/pinned
race run passed in 10.60 seconds. Real inline/snapshot/shared-reference sweeps,
four invalid metadata controls, and both model-to-cluster contracts passed under
race in 28.87 seconds. A 100,000-seed local run passed in 2.83 seconds on modified
source; its command was `SIM_SEEDS=100000 go test ./sim -run
'^TestSeededBlobMetadataReplay$' -count=1 -v`. Sources were based on `a1995cc`
with the adapter, shared decoders, modeled workload and real controls added.

This strengthens quiescent collection. Online collection remains unsupported,
and the original intermittent Object Store listing cause remains unconfirmed.
