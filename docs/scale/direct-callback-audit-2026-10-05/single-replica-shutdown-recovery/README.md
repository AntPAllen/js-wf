# DirectR1 bounded shutdown recovery native race proof

Executed d779882; native PASS55.04s, allthree independent1500-workflow/6000-entry
fixtures pass. Four-core/GOGC500/4GiB race SDK with linked v2.15.0 R3 servers;
only temporary INV/JRN audit consumers useR1. No separate external server byte
claim or OS SIGKILL qualification.

| Fault | Verified tail at128 | Audit elapsed | Result |
| --- | --- | --- | --- |
| Confirmed R1 owner library Shutdown | 4374 pending | 1.505353947s | Exact1500 INV/journals/terminal and6000 entries |
| Acknowledged captured consumer deletion | 4121 pending | 1.309517058s | Same exact report |
| Cancellation | 128 accepted visits | 96.505287ms | context.Canceled, no journal reduction |

Both loss cases accept exactly6000 journal visits; cancellation exactly128.
All original20s, verified cursor name/stream/MemoryStorage/AckNone/R1, pending
tail, source oracle and zero remaining audit-consumer assertions pass. Library
shutdown identifies ownerwf-test-1 from actual ConsumerInfo.Cluster.Leader.
No stream-leader substitution, added retry budget or metadata-overlap allowance.

The typed SDK ErrServerShutdown now enters existing original-context/two-resume/
leader-fallback transport recovery. Race differential and classifier controls
pass1.040s; visitor errors including that sentinel, arbitrary matching strings,
corruption and unqualified deletion status remain fatal. Original8000503 owner
failure remains failed and preserved; its other passing subcases are distinct.

Independent selected Git/source, actual SDK executable/race/VCS/module and closure
review complete. Full1791-member archive readback verified,27039699 bytes/two
parts, SHA256
`d32b74c576e198e93551d5784de8d905805c7d92e83d1d6f282b2eed1580066e`.
Capture excludes exhaustive external compiler/toolchain inputs.

This qualifies these focused R1 R3-library faults at the executed source. It does
not qualify same-owner abrupt process restart, R5 physical SIGKILL, arbitrary
metadata replay, full400k fault capacity, live/default adoption, final fullfault
matrices or actual24h. Next work must exercise real R5 process-owner loss and
larger retained populations before considering default adoption.
