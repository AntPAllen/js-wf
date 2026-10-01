# Terminal duplicate wakeups under a held lease

Production source is hashed in source-sha256.txt; base commit is 83253a2.

## Mechanism and scope

The hosted 6a91ebf seed-5 operation log identifies retained run sequence 101 as
mixedsignal/mixed-06-0. Eight acquisitions return ErrHeld at approximately
five-second intervals from 05:54:23.711 through 05:54:58.756 UTC while other
wakeups acquire and release leases for that same terminal invocation. Selected
operations are retained here; the full original artifacts remain in the sibling
mixed-seed5-6a91ebf directory. This supports terminal duplicate contention as a
runtime liveness mechanism. It does not prove that an ACK was sent or lost, and
raw sequence-101 headers were not captured. Other latency failures remain open.

On ErrHeld the production worker now probes durable terminal state and current
invocation within a two-second context. A valid terminal outcome must match the
current invocation sequence. Tombstones, malformed or missing state, stale
generations, uncertain reads and unsuccessful parent notification keep the
existing five-second NAK. A confirmed terminal requires parent notification
before ACK, with the existing generation-scoped idempotency key. This path does
not renew/release the lease, change the journal/outcome, or execute the handler.
Scheduled same-generation timer probes read the logical journal to preserve the
canceled-timer no-op metric. Same-generation terminal-state durability relies on
the production terminal writer's existing publication ordering; this is not an
arbitrary externally corrupted state detector. Lease fencing and unconditional
pre-append renewals are unchanged.

## Evidence

- Final 100,000 seeded schedules: PASS 36.790 seconds, 18,443,969 events,
  maximum virtual time 5,000 ms. Eleven modes cover completed/failed outcomes,
  read loss, wrong generation, tombstone, malformed/missing state, child notify,
  dropped child notification, lost notification reply and lost ACK reply.
  Invalid probes must NAK before repair. Sixteen duplicate wakeups drain with
  zero additional virtual wait while the healthy external owner retains its
  exact lease revision/value and can renew. Handler/effect counts, immutable
  outcome/journal, child-result dedup and raw integrity are checked.
- Exact first-ten and separate-process seed-42 replay, new pinned regression,
  full corpus/workload race PASS 10.146 seconds. Old regression corpus remains
  unchanged. Full default simulator PASS 112.732 seconds before the final extra
  negative-mode/checker additions; final workload/corpus verification is above.
- Full worker package PASS 29.352 seconds; vet PASS.
- Real R3 race contract PASS 8.848 seconds: 32 plain terminal duplicates drain
  in 165.189 ms; 32 canceled-timer duplicates drain in 424.718 ms. A healthy
  twelve-second external lease remains unchanged, handler/effect runs once,
  journal/outcome stay unchanged, and canceled-timer count increases by 32.
- Baseline production worker overlay fails the seeded drain semantically after
  30,000 virtual ms; real plain contract fails its four-second drain while the
  external owner is still healthy. The generation-check omission fails seed 2
  semantically with an unsafe ACK and 15 retained messages. Retained patches
  reproduce the controls; compiler errors/timeouts are not mutation evidence.
- Local mixed seed 5 PASS 31.445 seconds, terminal p99 8.357 seconds, all 28
  outcomes, raw-state audit and original queue-drain gate. This is one local
  run with different physical timing, not a paired hosted reproduction or the
  200-seed full matrix.

## Diagnostic contract

The mixed fixture retains a bounded 4,096-event dispatch ring and dropped-event
count. After an unchanged queue-drain gate fails it spends at most 20 seconds
sampling raw retained messages (64 messages / 4,096 scanned sequences maximum)
and known consumers, with one-second call bounds. Original drain state/error,
per-call errors and truncation are retained. Reads are not atomic. Sampling
cannot convert failure to success. The real R3 race contract PASS 4.313 seconds
preserves payload/header identity through a deleted sequence hole, records the
unacked consumer, and reports a canceled diagnostic context without changing
the original gate error.

The newest old-source hosted seed 4 fails at terminal p99 31.049 seconds;
its separate retained proof directory records that unresolved latency failure.
This fix does not clear it, the full simulation gate, whole chaos matrix,
24-hour soak, online GC, or other release requirements.
