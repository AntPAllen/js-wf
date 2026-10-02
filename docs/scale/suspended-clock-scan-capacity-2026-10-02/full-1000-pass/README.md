# Full1,000-seed suite at scan-capacity source

Sourcec477f00a16466729b941ea5bd3e121c6d795d66e completes in120.184s package time. The authoritative service
is inactive/success/exit zero. All157 top-level tests pass,
with only the two documented trace-only skips. All258 source-inventory pins
pass; all113 inventoried seeded workloads independently complete1..1000,
113,000 completed bodies. Aggregate115032 schedules,
1781009 choices and26755928 events.
The original and independently rechecked reports match; source-sensitive
simulator, guard, inventory and workflow files remain equal to that commit.
Original Go JSON, inventories, source, timing and reports are compressed and
byte-verified. This is a full normal1,000-seed pass, not the100k release gate or
a whole-suite race pass. The new capacity workload's five-minute race attempt
failed on deadline; its six exact pinned regressions passed under race.
