# Ahead200: two terminal native-delete failures

Campaign37014965084 is still running at
`82f323a593acef65b75102e6bfc9318d1caa0c8a`; the remaining jobs were not canceled
or restarted. These two actual terminal failures prevent acceptance of its
complete200-seed range. No historical worker-kill TTL miss was rerun.

| Seed | Actual named test elapsed | First observed fleet error |
| --- | --- | --- |
| 13 | 509.44s | Suspended repair: message deletion unsuccessful, API503/10008, JetStream temporarily unavailable |
| 16 | 577.13s | Suspended repair: message deletion unsuccessful, API500/10057, no message found |

Both failures are before the final release verdict. The later context-canceled
await or clock-cut-admission messages follow fleet cancellation; they do not
establish a separate timer-liveness failure. Final terminal/drain acceptance is
missing, so neither seed is relabeled as a pass.

The pinned `nats.go v1.54.0` implementation in
`jetstream/stream.go:deleteMsg` uses
`fmt.Errorf("%w: %s", ErrMsgDeleteUnsuccessful, resp.Error.Error())` on these
replies. This preserves the broad delete-error sentinel and flattens the typed
API error. The production reconciliation loop retries typed10008, while native
hint retirement recognizes `jetstream.ErrMsgNotFound`; these flattened replies
reach neither intended branch. This establishes a client-adapter error boundary
gap, without establishing why the server returned unavailable or which actor
removed seed16's hint.

The pinned server's10057 is a general message-delete failure code whose
description is the underlying store error. It also reports "message delete not
permitted" and other permanent errors. A fix must preserve the typed API reply
and normalize only a proven not-found case; it must not broadly swallow10057 or
all delete failures. The public legacy delete API preserves API errors and is
one candidate transport boundary, provided domain/prefix, context and permanent
error semantics remain intact.

Next required work: a seeded production-path reproduction for unavailable and
concurrent-already-absent deletion, permanent-error controls, native client
contracts, a source fix and fresh admitted-row validation. Existing accepted
twenty-seed evidence remains scoped to its earlier source. No recovery, admission
or physical-drain gate is relaxed.

`originals.tar.gz` losslessly retains both failed seeds' complete original
artifact sets, their terminal job metadata and an explicitly nonterminal campaign
observation. Every archived member was read back and SHA256 matched to its original
bytes; see `sha256.json`. Original downloaded files remain in
`/tmp/js-wf-ahead200-failures-37014965084`.
