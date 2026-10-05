# Observed per-consumer cleanup delivery measurement

Native e43d1b3 passes7.89s. Eachsix reader validates100k exactcoordinates/payloads.
Every delete is followed by streamconsumer-count and consumer-name observations
inside the same original30s reader context. Allsix reach count0/namesempty; this
run observes no intermediate positive count. It doesnot establish the cause of
the earlier count1 failure or erase that failed verdict.

| Mode | Delivery | Cleanup | Allocation delta including linked server |
| --- | --- | --- | --- |
| next | 368.058ms | 2.904ms | 263,594,448B |
| adapter | 393.065ms | 2.476ms | 237,400,136B |
| consume | 209.716ms | 2.588ms | 174,020,064B |
| callback_adapter | 257.029ms | 1.958ms | 187,378,144B |
| buffered_callback_adapter | 389.725ms | 3.037ms | 234,768,976B |
| next_recheck | 349.206ms | 1.940ms | 252,433,072B |

Buffered390ms versus unbufferedcallback257ms doesnot support adopting the queue.
The run is ordered and VM-sharing/noisy; no general slowdown factor is claimed.
The queue implementation is moved to test-only diagnostics, and production
consume_scan.go is restored exactly to a39bb6c. Existing unit lifecycle/byte
controls pass under race after the move1.216s. Defaults stay unchanged.

Independent selectedGit/actualSDK/module/closure and allsix terminal cleanup
observations verified. Full1087-member /onepart /25,008,949byte archive read back.
This is a delivery/cleanup diagnostic, not buffered fullintegrity/fault/capacity/
24h qualification. Next optimization should remove per-record adapter overhead
or measure safe ephemeral-cursor replication cost, with full gates retained.
