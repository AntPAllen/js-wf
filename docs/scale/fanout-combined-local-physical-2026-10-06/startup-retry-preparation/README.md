# Fanout caller-side bounded worker startup retries

The failed fullsix aa2015a campaign exhausted Worker.New's shared5-second startup
attempt at the signal-stream lookup after journal-only catch-up; the failed
40.84-second case still had its original5-minute context. Underlying metadata
latency/cause remains unconfirmed. Five other cases have actual local physical
witnesses, but the enclosing campaign remains failed and fully preserved.

Fanout constructor callers now retry only typed deadline/timeout/no-responder/
API10008 unavailable errors. Permanent configuration/not-found/cancellation errors
fail immediately. Worker.New retains its internal5-second bound; retries/backoff
share the unchanged original5-minute case context. All attempts/errors/intervals
are retained in each case. A success arriving after cancellation is rejected and
its unused worker closed. No new effects, lease writes or acknowledgments are
added by the harness. The production constructor and runtime are unchanged.

Three race controls exercise4typed transient retries,4permanent immediate failures,
original deadline/backoff/pre-cancellation and late-success rejection. Fullsix
first/interior/last creation/results requalification runs on a fresh root, with
full500children/actualparentSIGKILL/journalrestart/prefix/results/localphysicaldrain.
Original failed stores are not reopened; both longhandles remain unchanged.
