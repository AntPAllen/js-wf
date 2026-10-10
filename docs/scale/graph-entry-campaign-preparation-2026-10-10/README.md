# Actual-entry campaign preparation — 2026-10-10

The canonical native worker limit fixture now optionally retains its file stores
when WF_GRAPH_LIMIT_STORE_ROOT is an absolute path. Each case gets a distinct
new directory; default runs still use automatic temporary cleanup. No existing
store is overwritten or imported. The retained budget-20 R1 indexed/durable
control passes normal 2.133 seconds, with two SDK checkpoints, terminal slot 19,
zero forbidden effects and both prefix stages called once. Its native files
survive cluster close; inventory and hashes preserve that evidence.

prepare.py creates a new isolated sparse Git worktree at an exact source commit,
inventories every module Go source and testdata input, and builds retained normal
and race worker binaries. It checks identical source fingerprints before/after
compilation and a clean checkout. Compiler status, binary hashes, toolchain and
build flags are recorded. Sparse checkout excludes historical evidence and
repository tooling; it keeps all module packages and their testdata.

Preparation never runs the full-entry fixture. Launch must wait for the separate
100,000-native-grant result and a reviewed explicit lifetime budget. The actual
campaign must use budget 100,000 (49,992 real SetState calls), indexed authority,
durable staging, retained stores, operation timing and failure cursor snapshots.
The worker's default production cap and request/batch bounds stay unchanged.
Normal and race results remain separate; neither a successful compile nor the
small control qualifies actual 100,000-entry execution.

Future execution must persist the actual child exit, retain its service with
RemainAfterExit, capture immutable source/binary and environment receipts, and
audit all ordered entries, two checkpoints, terminal slot 99,999, zero forbidden
effects, exact rejected request, prefix calls 1/1 and terminal projection equality.
R3, live mode, collection, OS/process/VM/storage faults and original broad gates
remain separate. Public admission and collection remain off.
