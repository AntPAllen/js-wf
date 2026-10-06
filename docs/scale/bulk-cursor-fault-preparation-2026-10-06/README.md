# Real-cohort bulk cursor-owner failure preparation

The opt-in87920 bulk cohort owner-restart control prepares only historical audit
cursors on fresh verified disposable stores, retaining public metadata/deletion
receipts before acquiring source cuts. It targets a currently delivering actual
R1 memory AckNone WF_JRN cursor, confirms physical owner SIGKILL and restart,
and requires a distinct resumed cursor and exact original point-oracle samples.
Complete20s integrity before/after and6m bulk stage remain unchanged. Any error
invalidates samples. Delivery metadata trigger is not the visitor commit point;
the original scanner continues checking contiguous once-only source visits.

Focused race controls for delivery, failed metadata cleanup and existing bulk
projection controls pass. Native execution is pending. No default matrix/live
campaign changes or original failure reclassification.

The earlier full400k read-capacity donor has empty StepRequested payloads: the
frozen point latency checker and bulk projection both reject those requests.
Future generated capacity fixtures now include valid activity request JSON;
earlier400k retained-read evidence remains scoped to its original audit task.
A valid full400k latency fixture and memory qualification remain required.

Storage procedures alongside this file verify entire canonical archives (or
base plus delta), bind actual closed SDK metadata/executable bytes, and check
visible descriptors before removing only redundant raw/worktree proof copies.
Original stores/source/exes/metadata/caches/live retained; no NATS startup.
