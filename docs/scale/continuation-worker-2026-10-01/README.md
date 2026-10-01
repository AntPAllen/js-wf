# Continuation worker integration — October 1, 2026

Worker registration, verified suffix dispatch, boundary manifest/purge and
handoff now call the production continuation protocol. The frame also preserves
unconsumed drained signals and their scan cursor: snapshot purge can delete their
original WF_SIG messages. Historical completion replay bounds signal/attempt
facts at its anchor instead of incorporating later deliveries.

## Scope

- Real R3 three-node file-store contract: 1,000 state updates, two boundaries,
  original input/locals and state checks, two buffered signals across purge,
  graceful worker replacement pinned to another node, archive-denied final
  resume, unknown-stage rejection before user code, immutable cross-peer result
  and full raw-state integrity audit. This does not simulate SIGKILL or a server
  fault. Recorded effects execute once; absolute external keys remain distinct.
- Integrated Tier 1 production worker, client, lease, journal, snapshot and SDK
  decisions over seeded transports. Seventeen modes cover clean execution and
  drops/hidden replies at request, frame, completion, archive, manifest, journal
  purge, signal purge and handoff. Every queued fault must be consumed. The
  fixture binds both journal and signal stores; earlier unbound runs that skipped
  successful handoff are not accepted as this proof. First ten seeds replay
  exactly; seed 42 is identical across two processes and pinned in the corpus.
- Mutation removes buffered signals from production SDK restoration. The
  compiled pinned worker trace fails because the workflow suspends without its
  checkpoint-held signal instead of completing. This is not a compile failure.
- Raw-state audit permits a continuation suspension only with a completed,
  matching checkpoint declaration and content-addressed frame reference. Wrong
  stage/hash, inline completion and failed-frame controls remain rejected.

## Remaining

Actual worker SIGKILL and all crash-cut recovery/repair contracts, integrated
unknown/corrupt-generation and retirement/reuse checks, panic/cancellation/timer/
promise and exact journal-limit boundary gates, offline multistage replay,
continuation GC/model proofs and full final-source release matrix/soak are open.
An unknown registered stage retries before effects; correcting the deployed
registry is necessary for it to advance. Online GC remains unsupported.

The final logs and source checksums in this directory record the exact verified
scope. This feature does not resolve the independent mixed seed 65 latency miss.

## Final evidence

- `real-race.log`: real three-node restart and audit passed in 10.824 s of
  package time (9.79 s test time); two frame reads and zero archive reads on the
  final worker, with both missing-registry and missing-stage delivery rejection.
- `model-100k.log`: 100,000 schedules and choices, 21,347,572 transport events,
  virtual maximum 1,000 ms, 102.297 s of Go test time. All seventeen modes observed
  and every queued fault exercised. This covers the modeled transport replies,
  not actual process crashes or NATS internals.
- `model-race.log`: integrated worker campaign and pinned corpus passed under
  race in 20.818 s; exact first-ten and cross-process seed-42 replay checked.
- `packages-race.log`: SDK, worker, codec, reconciliation and integrity package
  race suites passed; some unchanged packages used Go's cache.
- `full-sim.log`: complete final simulator suite passed in 83.005 s.
- `buffer-mutation.log`: compiled production restore mutation fails pinned seed
  42 with a nonterminal journal instead of completing the workflow.
- `vet.log`: vet exited zero with no diagnostics.
- `fixture-unbound-handoff.json`: pre-fix fixture debug trace; its unconsumed
  handoff fault was caught by the new guard. It is not a replay regression pin
  or accepted campaign evidence. The accepted pin uses the bound fixture.
