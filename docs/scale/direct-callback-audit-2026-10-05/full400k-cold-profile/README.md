# Cold full-capacity CPU diagnostic: baseline failed

Clean source `4086e44` executed a fresh verified full400k/4.8M copied fixture.
Normal 4 CPU / GOGC500 / 4GiB configuration and original 20-second audit budget
were retained. Native test failed in 35.08 seconds: healthy baseline expired at
20.000090 seconds after 4,282,850 journal visits. No owner SIGKILL was injected.
The zero journal/entry/terminal report fields record incomplete reduction, not
missing retained data. This is not a capacity or fault qualification pass.

Invocation scanning took approximately 1.223 seconds; journal scanning occupied
the remaining 18.776 seconds. Partial allocation was 2,831,061,512 bytes across
eight GC cycles; total GC pause was 21.295 milliseconds. Memory statistics include
profile shutdown overhead; the verdict/deadline was captured before profile flush.
Watch creation and Stop succeeded. Stop alone does not prove the initial snapshot
barrier completed and does not attribute the failure to state watching.

The profile contains 20.33 CPU seconds over approximately 20 wall seconds.
Cumulative costs include journal.UnmarshalEntry 5.02 seconds, Conn.parse 4.09
seconds, and runtime.selectgo 2.69 seconds. Cumulative costs overlap and must not
be added. These observations justify investigating decoding and per-record
handoff; they do not isolate a server defect, GC cause, or speed ratio. The earlier
18.35-second ordered/warmed pass remains limited to that executed profile.
The million-timer campaign shares this VM. Observer cadence was 500 milliseconds.

Independent review verified 684 selected source inputs, the actual SDK and five
server executables/modules/mounts, process closure, and 1,058 unchanged donor
files. Complete base-plus-delta reconstruction covers 1,711 logical files with
819 aliases. The 50,146,730-byte delta SHA256 is
`760724fd0b1162231747f5d678efe59747b509413cc75f2c4ede765a2f13f60e`.
Both pinned canonical base Git parts and these delta parts are needed. Original
fixtures remain closed; no unchanged rerun or public reader adoption follows.

Next change should measure a bounded alternative to per-record handoff or reduce
measured decoding costs while preserving all semantic, cancellation, recovery,
cleanup, cardinality, and deadline checks.
