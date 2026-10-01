# Continuation snapshot publication foundation

The modified tree based on `8ecb81b` adds a version-2 snapshot manifest with an
archive plus runtime pointer. Ordinary manifests remain version 1. Old journal
readers explicitly reject versions other than 1, so they cannot silently ignore
a runtime pointer and compact away its anchor. Current readers reject a v2
manifest without its runtime pointer or a v1 manifest containing one.

`WriteCheckpointSnapshot` verifies the actual completion's sequence/index/epoch,
its frame reference/hash, the last SDK request's checkpoint stage and input hash,
SDK request/completion pairing, SDK position independently of auxiliary journal
indices, frame generation and bounded content. It archives entries strictly
before the completion anchor, verifies that archive, then CAS publishes both
pointers. It does not purge; the caller must hold the invocation lease and use
unconditional pre-append renewal. Unknown writes retry safely, and explicit
purge confirms the manifest before touching its fixed prefix. Older checkpoints
cannot replace newer ones. Generic compaction of continuation journals fails
instead of discarding the only live anchor.

The collector validates and marks current frame bytes and their promise outcome
references. A retained current frame can therefore keep a child result blob
reachable independently of that child's original terminal journal. Collection
still requires quiescent writers. An overlay of actual predecessor production
`retention/blobs.go` from `8ecb81b` fails the real contract by deleting that child
blob. The corrected production contract passes. Injected frame corruption aborts
before deleting an otherwise eligible orphan. This does not establish a NATS bug.

Proofs:

- Three-node R3 file-store direct-storage contract under race: 4.498 seconds.
  Two checkpoint boundaries, cross-peer exact logical reconstruction, preserved
  completion anchors, generic compaction rejection, idempotent confirmation,
  older-pointer rejection, frame/promise retention, and corrupt-frame abort.
- Seeded production snapshot transport decisions, all nine archive/manifest/purge
  fault modes, exact replay and cross-process trace identity: race 7.587 seconds.
- 100,000 seeds for the new workload: 42.881 seconds. No coverage-summary counter
  was enabled in this run; the retained command/seed limit identifies the range.
- Complete final simulator suite including the new workload and pin: 91.741 s.
- Journal race controls: 1.018 s. Legacy journal/retention race suites: 1.043/1.843 s.
- Pinned corpus race run: 2.427 seconds (before manifest v2 trace regeneration);
  the final complete simulator run verifies the regenerated v2 pin.
- Vet and diff checks passed.

Commands: `go test -race ./integration -run '^TestCheckpointManifestRetainsAnchorAndPromiseBlobs$' -count=1 -v`,
`SIM_SEEDS=100000 go test ./sim -run '^TestSeededCheckpointSnapshotReplay$' -count=1 -v`,
`go test ./sim -count=1 -timeout=5m`, and `go test -race ./journal -count=1 -v`.
Logs and exact implementation source hashes are retained.

Scope remains incomplete: no production worker calls these APIs. The frame and
checkpoint pair are created directly by these fixtures; the seeded object fault
is the archival object write, not SDK frame creation. Frame publication cuts,
lease-fenced worker dispatch, prefix-free suffix reads, handoff repair, offline
continuation replay, retirement and a modeled GC traversal still need their own
proofs. The mixed seed 65 latency failure and release matrix remain independent.
