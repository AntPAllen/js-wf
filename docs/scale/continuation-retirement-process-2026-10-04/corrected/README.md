# Retirement/reuse with actual server SIGKILL: focused race controls

The corrected fixture passes both fresh-manifest response-loss cases: confirmed
state leader SIGKILL/restart (34.10s) and all-three-server SIGKILL/restart (33.64s).
The process test totals67.74s; the existing library-restart control passes25.74s.
Every killed process is reaped with SIGKILL status before any replacement starts.
Independent log review binds original/replacement PIDs, exact node sets and the
observed state leader. All cases retain generation1→3, two reclaimed retired
objects, three effects, two terminals, exactly one manifest loss and preserved
shared/survivor/fresh blob references. Existing raw integrity and peer-result
assertions remain. Startup uses4s attempts within the original30s budget; worker
publication and scenario60s bounds remain unchanged.

Actual live race SDK/environment/all build-info and629 selected local pre/post
inputs verify;627 files match base `d59bd0b`, with both exact modified test inputs
retained. SDK SHA256:
`a7b43a7924c86592b87cc00789b05ca4d77b26046a23416b761ac25dd4924142`.
All855 original archive members and three parts read back; compressed56783520bytes,
SHA256 `e643b1bbc381de412edaa3fa1f8bf3ab8fafe9c36db1145570317e8bf5b90237`.
The archive retains actual SDK, process server binaries/stores/logs, input ledgers,
exact overlays, launcher, reviewer and commands. Concatenate numbered parts and
verify the manifest before restoration. Failed startup originals remain separate.

Source inventory is a local superset, not a hermetic modules/toolchain proof.
Process stores were not reopened; integrity/GC assertions retain named-test scope.
The library control's temporary stores were not retained. This qualifies these
focused cuts only, not worker SIGKILL, concurrent-writer GC, lease/TTL/limit or
latency-p99 combinations, final-source full matrices or24h.
