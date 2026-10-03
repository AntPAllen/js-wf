# Complete six-scanner prefix graph: hosted race

[Run 37145175684](https://github.com/AntPAllen/js-wf/actions/runs/37145175684)
completed successfully at exact source
`4f11ca2957f6d3426a1dee227f0aadb91f23207b`. Public artifact
`11282152460`, `tier1-race-evidence`, is 15,190,219 bytes.

Independent review validates terminal job/source/artifact metadata and accepts:

- 174 top-level passes and two documented trace-only skips;
- all 379 pinned regressions;
- all 121 scalable workloads, contiguous seeds 1–1,000 (121,000 bodies);
- 1,126 unchanged before/after source hashes, byte-identical to exact Git;
- retained race executable SHA256
  `1b7f9c053f03cd78d547ed1a059d3fc95b79ef9c161fb6058bc3fa78c664de5c`;
- actual build settings, compiled test inventory and source-extracted AST seed
  inventory;
- byte-identical report regeneration from all raw test2json events and event SHA.

Producer wall time: 2,160.43 s. Aggregate counters include fixed/repeated tests:
124,719 schedules, 1,806,737 choices and 27,606,244 transport events.
The download lost executable permission; chmod restored it for list/build-info
inspection without changing a single executable byte.

All originals, metadata and independent review are archived with complete
per-member SHA256 readback before atomic publication. This proves the complete
race suite after Start, signal, suspended, native/fallback timer and tombstone
partial-prefix fixes. It predates the subsequent retained deletion-marker
cleanup. That later graph has independent hosted campaigns. Normal 100k,
real-cluster matrices, population bounds and 24-hour release remain separate.
