# Shared SDK and CLI journal header admission

Both raw SDK replay and CLI import now use the same explicit flat record wire
schema in `internal/journalwire`. The SDK decodes the full array unambiguously
before checking ordering or entering user code. Unknown headers, duplicate
names, escaped duplicates and case aliases reject with `ErrCorruptJournal`.
Payload contents stay opaque, including duplicate user keys. This extends the
previous CLI-only change; terminal payload semantics remain a separate open
admission task.

Development full SDK race passes30.542s. Fifteen SDK header controls prove one
valid opaque payload runs once and fourteen malformed imports run no handler.
Existing15 CLI header and14 envelope race controls pass. A negative overlay
restores ordinary decoding: all14 rejecting SDK leaves fail with actual handler
entry, exit1, no build failure. [Saved mutant](development/ordinary-decode.go.txt)
and [negative log](development/ordinary-decode.log).

The [prepared driver](run.py) retains full replay qualification: full SDK race,
848 normal saved traces, prior native/export/CLI controls,209 offline CLI
outcomes and64 required mutant rejection leaves. The [reviewer](review.py)
requires68 SDK test roots,15 SDK header controls,15 CLI header controls, exact Git
inputs, six binary identities and actual loaded matching supervisor exit0.
This qualification is prepared, not accepted. The previous CLI-only source's
separate live qualification remains unchanged and cannot qualify this extension.
Full payload/import/fault/retention/public admission/collector/rollout and all
broader original gates remain open.
