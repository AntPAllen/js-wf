# Larger audit window: no material speedup observed

Actual retained race SDK at `e638a932b5b6d17c327db1ca9bd6fb1256b1aed9` executes all three named native
cases successfully. Full audits of identical quiescent 12,000-invocation /
144,000-entry /12,000-terminal stores produce matching complete reports:

| Reader | Actual elapsed | Outcome |
| --- | ---: | --- |
| Point reader | 20.020236665 s | Original20s deadline |
| Previous512 window | 14.853547828 s | Complete |
| Candidate4096 window | 14.851015237 s | Complete |

The0.0025s difference is not a demonstrated performance improvement. Default
opt-in delivery windows are restored to512; the private parameterized helper
and explicit512/4096 native comparison remain available for future measurements.
No soak restart, audit-budget relaxation or general capacity claim follows.

Fresh compacted-prefix/cohort, snapshot/terminal corruption, orphan, duplicate,
epoch/index and unresolved-step controls pass on the executed source. All2889
selected inputs /59 Git-local files, runner, actual live SDK executable SHA and
all build-information fields including the Go-version header verify. The actual
SDK is `b5ce41f652297c6594b33f1bfedc0a2ae1004b5ba285a3878cb1aae383ee7b7b`. Stores are retained, not independently
reopened; fixtures are handcrafted retained records, not workflow executions.

[Complete source/native proof](complete-proof/) preserves3984 members /
156636238 bytes, including actual SDK, captured inputs, all1077
original broker files, raw events and exact producer/reviewer. Compressed proof:
43346429 bytes in two parts, SHA256 `85a6f953426e63467417d541560471849b78c170f56b939121f6a358dc6927f7`.
All original/member hashes and part/concatenated archive hashes verify by readback.
Concatenate numeric parts, verify their hashes and the archive digest, then
extract into a fresh directory. Named cases and comparative numbers qualify only
this executed source; final-source matrices, the failed original24h attempts and
million-timer physical drain remain open.
