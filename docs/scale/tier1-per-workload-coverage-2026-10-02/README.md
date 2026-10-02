# Per-workload seeded completion evidence

Tier 1 campaigns now use a tracked range iterator for 112 source-inventoried
seeded tests. It yields exactly seeds 1 through SIM_SEEDS and increments only
when each loop body returns normally. Cleanup logs requested and completed
counts and fails a passing test that exits early. Fatal failures retain partial
counts. The iterator unit control verifies that continue counts a completed
body and break does not. Test-pass evidence is still independently required.

Four cooperative races previously hardcoded to 100 seeds now share the campaign
range: two-starter, start/scanner, lease-acquire and two-writer CAS. The separate
500-item terminal-owned test searches for eleven modes within 100 seeds; it is
a cardinality/mode control, not a campaign loop or a claimed 100k workload.

The Go AST inventory identifies every test calling the tracked iterator and
rejects ambiguous multiple loops. CI retains that source inventory. The suite
guard requires exactly one record per inventoried test, exact identity/schema,
first=1, last=requested=completed=campaign seeds, and the existing complete test,
pin, package and aggregate checks. Historical artifacts without this optional
inventory remain explicitly configuration-only evidence; future CI requires it.
These records prove completed seed-loop bodies, not exploration of every fault
combination or independence of scheduling choices.

Actual focused race tests pass: two clock workloads with 1,000 seeds each in
1.456s, and the four cooperative workloads with 1,000 seeds each in6.060s.
Original Go JSON events, inventory, hashes and independent six-record review
are retained. All ten Python guard methods pass, including missing, duplicate,
short, malformed, foreign and wrong-inventory negative controls. All simulator
test files compile in the focused command. A full112-workload completion and
100k campaign at this source remain unproven.
