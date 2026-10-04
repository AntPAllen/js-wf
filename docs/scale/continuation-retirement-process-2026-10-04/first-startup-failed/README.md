# First retirement process control: startup failure preserved

Both new process cases failed during initial provisioning at their unchanged
30-second readiness deadline, before any retirement/workload/manifest cut. The
same executable's existing library-restart control passed22.03s. The process
fixture had used one unbounded provisioning attempt; the corrected fixture uses
four-second attempts inside the same30-second startup budget, matching existing
process tests. This is a harness preparation correction, not a runtime fix or a
confirmed explanation of the underlying missing reply.

The actual live race SDK and environment, selected local source pre/post hashes,
exact modified test inputs/overlay, process server binaries, original retained
process stores/logs and terminal stdout are preserved. All65 archive members and
three parts verify; compressed56518659bytes, SHA256
`06a70d9c3dd3bf0bc75b3dc6f8eca92e4ba0002d3a0f56c1018be122ecb6420c`.
See the manifest; concatenate parts in order and check the digest before restore.
Actual SDK SHA256:
`9b15f5ab65570cee5d402b1bcf47370cf5d26c5c240aaffefb1ff4532f87098f`.
The source inventory is a local superset, not a hermetic toolchain/modules proof.
The library control's temporary stores were not retained; retained process stores
were not reopened. No process-fault, release-matrix or24h qualification follows.
