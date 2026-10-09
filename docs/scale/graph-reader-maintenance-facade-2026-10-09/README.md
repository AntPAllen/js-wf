# Reader maintenance through GraphStore

`GraphStore.ReaderMaintenance()` returns a scoped facade with only watermark capture and bounded reader-expiry batches. Construction verifies scan/watermark capability and canonical namespace scope without storage calls. `RunGraphReaderExpiryWithStore` wires the facade into the persisted native leased scheduler, with configuration preflight before KV preparation. The publication port and object collection are not exposed by the facade.

## Verification

The exact race command and actual process exit are recorded in `result.json`; raw output is retained in `race.log`. Journal facade controls pass in 1.017 seconds. Reconcile admission and file-backed R1/R3 restart/lost-checkpoint-ack controls pass in 15.212 seconds. They cover bounded resumption, expired pin removal, retained application data, live reader preservation, namespace isolation and unchanged objects. Invalid store-based configurations make zero instrumented JetStream calls; an opaque worker identity reaches the expected storage preparation boundary. The native restart test reconstructs both store and facade and resumes from the persisted cursor.

## Remaining scope

No worker CLI activation in this change. Reader expiry does not permit object collection; production collection remains off. No VM/process kill, route/storage fault, arbitrary interleaving or scale claim. Existing 155 families/835 traces are unchanged. Both live frozen race campaigns and queued frozen full155 normal exclude these later changes. Complete current normal/race/all-pin/extended qualification and every original broader runtime/rollout/fault/scale/soak/migration/release gate remain open.
