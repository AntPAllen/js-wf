# Complete marker-cleanup graph: normal 1k

Exact source: `9ecc37c74979132a8a2bae5877eb44bc175cbf89`.

The producer built and retained the actual normal simulator executable, captured
all selected source hashes before/after execution, compiled test inventory,
exact-source AST seed inventory, complete pin inventory, commands/environment,
and raw test2json events. Complete package elapsed: 126.114 s.

Independent review against that exact Git tree accepted:

- 175 top-level passes and two documented trace-only skips;
- 391 pinned regressions;
- all 121 scalable workloads, contiguous seeds 1–1,000 (121,000 bodies);
- 1,140 identical source hashes before/after;
- actual executable SHA256
  `fe8252afbb7363bfed62820fe1b3a1e176121dd587fa84982bdaddf573c9be78`;
- compiled inventory, exact-source AST inventory, and byte-identical report
  regeneration from raw events, including their SHA256.

Aggregate counters include fixed/repeated cases: 124,847 schedules,
1,807,121 choices and 27,606,589 transport events. The new marker-drain test runs
128 fixed seeds and exact replays; it is not a scalable 100k workload.

All 18 archive members, including the executable and raw events, were reopened
and independently SHA256 checked before atomic publication. This is complete
normal 1k qualification. Hosted race/100k, real fault matrices, 24h soak and
population-independent cleanup remain separate gates.
