# Absent canonical terminal projection repair — 2026-10-08

A worker can crash after canonical terminal publication and before `WF_STATE` publication. Previously, a held-lease duplicate validated that terminal and ACKed while the projection stayed absent. The original implementation fails the new control at seed 6 with an absent projection after ACK; its overlay, failure trace and result are preserved.

The canonical terminal probe now attempts `Create` with the verified journal payload when its initial purge/projection lookup finds an absent key. It never overwrites an existing mirror or tombstone. A failed or ambiguous write retains NAK/retry behavior. A concurrent key creation is followed by the existing invocation and purge checks; a concurrent purge marker prevents ACK. The projection never authorizes terminal data or handler execution. Existing present mirror bytes remain untrusted and unchanged.

## Shared simulation and native development evidence

The new `graph_terminal_projection_repair` workload covers absent projection, dropped create, committed create with lost acknowledgement, concurrent untrusted mirror, and purge-marker creation during the attempted repair. Failed/ambiguous creates require NAK before reopened graph/client/worker adapters recover. Every interception must fire. All cases preserve the foreign lease, execute no handler/effect, leave the canonical journal unchanged and reclaim fixture graph objects.

Normal 1,000-seed exact replay passes for both the original eighteen-mode terminal family and the new five-mode family. Both families also pass 1,000 seeds under race detection. Five new fault traces are saved; only the original missing-state trace changes, and its seed decisions/non-transport fields remain unchanged. Its transport changes are confined to the repaired outcome's KV create/read/delete lifecycle. The other 730 prior pins remain byte-identical; current inventory is 147 families and 736 pins. Complete corpus normal/race results and executed coverage/lineage review are in `development/`.

Actual native R1/R3 controls verify the repaired payload (including the existing external result case), one effect, and unchanged healthy-owner lease. A native R3 race attempt failed the foreign-lease equality check; the original log lacked before/after values, so its cause remains unconfirmed. The fixture now anchors the baseline to the acknowledged owner's epoch/value, permits at most six reads for strictly older observations, reports detailed revisions/values, rejects newer/different observations and still requires exact lease equality plus the owner's successful revision-CAS renewal. Final normal/race results use this receipt-aware fixture. No production lease algorithm, TTL or recovery target changes.

The first original-family capture attempt failed because its output directory was absent. That failure is retained; the corrected run creates the directory before capturing. The expected original-code failure and native race failure remain separate from passing results.

## Scope still open

This repairs an absent projection on a canonical terminal duplicate; it does not replace a present forged/stale mirror, introduce an atomic cross-stream lifecycle/state publication fence, provide autonomous terminal catalog recovery, or migrate snapshots, continuation, import, history, projection consumers, CLI or deployment. Legacy behavior is unchanged. Fixture graph collection does not enable production online GC.

The accepted full normal 146-family suite and the live full 146-family race campaign at `2b71f1d` exclude this later change. Complete current qualification, extended fault combinations/permutations/seed campaigns and every original native/process/storage/power-loss/concurrency/capacity/matrix/actual24h/million physical-drain/default-adoption/release requirement remain open.

## Frozen complete normal qualification in progress

Source `5d84b63` is isolated at `/home/exedev/js-wf-terminal-projection-qualification`:

```sh
python3 scripts/check-tier1-race.py --root /home/exedev/js-wf-tier1-full147-normal1000-20261008 --seeds 1000 --no-race
```

This retains the binary and source inventories and requires every current family and pin. Terminal evidence and independent review are pending. The earlier complete race campaign remains at its original 146-family source; no duplicate full race run is started while it remains active.
