# Experimental byte-bounded retained audit reader

The public/default reader still uses its original 512-record Fetch windows.
This candidate shares its frozen stream bounds, metadata/order checks, visitor,
leader absence oracle, maximum two resumptions, deadline and cursor cleanup.
Only delivery changes: documented `PullMaxBytes(8 MiB)` bounds the SDK client
payload buffer and `StopAfter(4096)` bounds records delivered by each iterator.
The combined `PullMaxMessagesWithBytesLimit` option does **not** promise a total
client byte bound and is deliberately not used.

The SDK additionally allocates a one-million-slot pointer channel in byte mode
(approximately 8 MB on this VM). The byte option is not a bound on all heap
allocations or downstream checker state. Messages larger than the byte budget
may stall delivery until the existing transport deadline/fallback; this control
uses 256 KiB records, below the standard server payload limit. Large-cohort
performance and interruption acceptance remain required before adoption.

## Recorded native attempts

Both attempts use real in-process NATS file replicas (R3), 120 published records,
256 KiB per record, deleted sequences 1/7/119 and cutoff 119. Exact point-read
digests include sequence, timestamp, subject, proof header and payload.

- Initial candidate **fails** after 93 records with `nats: no heartbeat received`.
  Test duration 11.26 s; no promotion. Original binary/log/source/store files stay.
- Corrected candidate preserves the missing-heartbeat error identity and also
  classifies it as a transport timeout for the unchanged bounded recovery path.
  Other semantic errors remain fatal. Native control **passes in 11.44 s**:
  116 records / 30,408,704 payload bytes exactly match point reads; later sequence
  120 excluded; cancellation produces one visit; zero audit consumers remain.
  This confirms refill-sized payload correctness, not a 100k/full fault gate or
  proof that the resumption branch was admitted in this successful native run.
- Focused race controls pass, including explicit heartbeat identity/recovery,
  record limit, blocked iterator cancellation, semantic errors and existing scan
  controls. The race executable itself was not captured.

Both native test executables were hashed through their live `/proc/PID/exe`.
Full build information, selected repository Go/module input hashes before/after,
tracked source diffs and new source overlays are retained. Each native run's
input hashes match. Base revision is `73b4766`; in-process servers share the test
executable. This is not an exhaustive compiler dependency inventory.

Original file stores are included in the archive, but have not been reopened
independently. Archive verification records every member and split-part hash.
Primary originals remain at `/tmp/js-wf-byte-native-20261005`.
