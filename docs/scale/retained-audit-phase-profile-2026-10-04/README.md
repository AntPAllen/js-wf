# Native audit phase profiles: race and normal builds

Both executions use source `1c9fb4d40bf46dbf6a44779d2f2c060a69504ae5`,
GOMAXPROCS=2, GOMEMLIMIT=512MiB, and the same full audit with 512-record batches,
stream-matched replicated consumers, and unchanged 20-second limit. Separate
fresh three-node file-backed embedded clusters contain identical handcrafted
12,000-invocation /144,000-journal-entry /12,000-terminal logical populations.
These are sequential diagnostics, not repeated timing distributions or the same
physical stores. All asynchronous publication acknowledgments are checked.

| Measured wall time | Race | Normal |
| --- | ---: | ---: |
| Full audit | 14.672637649 s | 1.669375955 s |
| Invocation scan, including visitor | 0.595234083 s | 0.158557623 s |
| Journal scan, including visitor | 8.878759272 s | 0.995376810 s |
| Journal visitor: decoding/grouping | 3.964196141 s | 0.179839630 s |
| Outside the two scans | 5.198644294 s | 0.515441522 s |

Both complete reports match and each stream's exact record counts/bytes are
checked. Race full-audit time is about 8.8 times normal in this fixture. TSAN
operations dominate the race profile's flat CPU samples. Normal CPU is spread
across transport/parser/runtime work. Neither observation proves a NATS defect
or five-container performance. Profiles include embedded servers and the client
in the same process; labels overlap and cannot be summed. Scan time includes
visitor time; outside-scan time includes setup, grouping/sorting and terminal
checks, not just validation CPU. CPU sampling starts after fixture publication.

Independent reviews verify all 2,889 selected inputs and 59 Git-local files,
retained runner, actual executable digests, every build-info field including Go
version, exact named-test success, phase JSON and readable CPU profiles. Live
/proc executable identities were not captured. Original stores are retained but
not independently reopened. Selected-input inventory excludes exhaustive
assembly/embed/generated/hermetic proof.

- [Race complete proof](race/): 3,274 members /155,064,767 bytes;
  compressed 43,165,702 bytes; SHA256
  `6946836193822499f714d1515b9999aa3ae6a177447634333aa7f5afb9b0c4a8`.
- [Normal complete proof](normal/): 3,274 members /146,280,106 bytes;
  compressed 39,750,415 bytes; SHA256
  `95fd5f76d0d6ddf194fdb2047f165b615625a8da711268e4d3bcfa3fbe6b74f0`.

Each contains actual SDK, native stores, input copies, events, CPU profile,
derived commands and output, and independent review/preservation scripts.
Every member and original input was hashed before and after archiving; parts
and concatenation hashes verify. Concatenate ordered parts, verify manifest
SHA256, then extract into a fresh directory.

No race requirement, production setting, invariant, audit budget or release gate
is changed. No soak restart or full-matrix/24-hour qualification follows. The
next diagnostic allows an explicit population up to 100,000 invocations to test
large-cohort scaling with the same complete audit and original deadline. The
12,000 default remains; the opt-in population is test-only and captured in the
retained producer environment.
