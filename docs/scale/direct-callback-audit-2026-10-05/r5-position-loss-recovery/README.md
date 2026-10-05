# Actual R5 cursor-owner SIGKILL and position recovery accepted

Executed clean889a70c, race SDK, actualv2.15.0 external five-container servers,
four CPUs/GOGC500/4GiB. NativePASS61.26s, both fresh fixtures.

| Fault | Audit elapsed | Last zero-consumer observation | Exact report |
| --- | --- | --- | --- |
| Verified R1 cursor owner SIGKILL, left down |4.341273287s|4.344642071s|1500 INV/journals/terminal,6000 entries|
| Verified owner SIGKILL, same-store restart |2.409395266s|2.412255838s|1500 INV/journals/terminal,6000 entries|

Original20s includes fault/restart/audit/cleanup. Every journal sequence visited
exactly once in contiguous order. Left-down old-owner delete times out while
independent replacement delete succeeds under unchanged shared2s ceiling.

Restart actually exercises position-regression recovery: same assignment name,
creation time, complete config and owner; delivery/AckNone floor regresses1961→1726.
New distinct cursor starts1962; replay never reaches visitor. Additional derived
verification records these exact archive-reviewed API values. Ordinary unconfirmed
replay and semantic errors remain fatal; original two-resume/gap proof unchanged.

Independent683 selected Git Go/module inputs, actual SDK/race identity,
11 actual external server processes/bytes/module/mounts including replacement
owner PID and all process closure verified. Complete1668-member69,326,509byte
archive, three parts, SHA256
`fc2523766ccd9d7ea19a544ecde368f5c0cbb61cfb6808c49bbad82b61797bdf`, full readback.
Original failed parents remain closed/preserved. No large fault capacity, legacyR1,
live/default adoption, current-source full matrices or actual24h qualification.
