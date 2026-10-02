# Mixed production dispatch deduplication mutation

The shared fixture admits four pending shorts, three suspended timer parents,
two suspended signal parents and a suspended parent with six children and twelve
grandchildren. It joins the initial workers, SIGKILLs the observed journal leader,
verifies the exit signal and restarts the retained store. Before replacement
workers consume dispatches,64 concurrent production Enqueue calls through two
surviving nodes use one stable message ID for the held short invocation.

The intact baseline retains one actual WF_RUN record with that message ID.
Removing the production WithMsgID option retains64 distinct physical records
with no message-ID header. Reads are bounded by stream sequence snapshots and
filter both partition subject and invocation body, so unrelated timer wakeups
cannot count. A mutant may retain more than64 after ambiguous retry; that also
violates deduplication. Partial retention, unrelated errors and header mismatch
fail without the accepted semantic escape.

Both variants then complete all28 invocations with passing raw-state integrity,
immutable journal prefixes, higher replacement epochs and target result42.
This proves dispatch deduplication in the admitted mixed leader-kill fixture;
it does not claim that duplicate dispatches necessarily corrupt journals.

The final runner passes the baseline in37.642s and detects the mutant in37.164s
(including runner overhead). Compilation and unrelated-test-failure controls
are rejected. The intact race run also passes in36.77s (37.788s package),
with full admission, confirmed SIGKILL, one retained dispatch, all28 terminals
and no race warning; its original JSON events are losslessly compressed.
Independent receipt checking decodes actual target bodies, checks
unique physical sequences53 versus53..116, all headers, admission and named
pass/fail outcomes. The original logs/report are losslessly archived with member
SHA256 manifests. Fixture and production source hashes match the final worktree
based on `c6793cf` before commit.

The first bootstrap runner was already running when the expected marker changed;
its old in-memory classifier rejected the new semantic escape. Those originals
are preserved separately. The final run uses matching classifier and fixture
sources. Hosted acceptance is pending for the new mixed-enqueue CI job. This
advances the fourth mixed source-mutation category; purge, start repair and the
full mixed-chaos release gate remain open.
