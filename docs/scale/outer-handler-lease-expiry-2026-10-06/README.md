# Native outer handler and continuation lease expiry

Accepted focused real R3 race controls at `fb1e515` cover both an ordinary handler and a named continuation stage. Native package execution passes in 35.32 seconds, including race shutdown overhead. Successor takeovers are 13.072832160 and 13.077403431 seconds, each below the unchanged 30-second target.

Only the old owner's heartbeat ticks are withheld. The actual replicated server lease expires with its production 12-second TTL; no KV deletion, TTL shortening, wrapped server clock or parent cancellation causes the takeover. A live successor receives redelivery through the unchanged 13-second AckWait consumer and completes in a higher epoch while the old handler remains held.

Resuming the old heartbeat requires `lease_heartbeat_lost` for the original owner/epoch and the real JetStream key revision mismatch. Worker shutdown succeeds while the ignored handler is still held. The queues physically drain through public local Jsz on all three distinct peers. A later SDK call is cancelled before its effect runs, and accepted journal records and tail remain identical. The production integrity checker passes on the retained invocation.

The independent review binds all 1,862 selected files to Git, before/after/current bytes, actual observed SDK executable/argv/profile and race build. Known SDK/producer PIDs and visible root descriptors/processes/containers/mounts/loops are closed, with inspection limits recorded. All six closed replica consumer/lease configurations independently retain AckWait13s/TTL12s and R3 leases. Complete original files are member/hash/mode/mtime verified and preserved in S3.

- [Accepted native log](native-race/native.log)
- [Independent review and raw replica configurations](native-race/review.json)
- [Complete archive metadata](native-race/archive-verification.json)
- [Full S3 readback](native-race/s3-readback.json)
- [Original failed observer guard](initial-error-guard/review.json)

The first native run at `13af4d2` reached both successor results but incorrectly rejected wrapped lease errors. Its complete rejected fixture remains preserved separately. Correction changes the assertion only; production runtime and deadlines are unchanged.

This closes the focused native lease-expiry combination for these two handler boundaries. Other network/process/clock/checkpoint cut combinations, full real-cluster matrices, million physical-drain and actual24h gates remain open. Stores are preserved and inspected as bytes/configuration, not independently reopened; no exhaustive process-lifetime or provider-durability claim is made.
