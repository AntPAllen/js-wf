# Verify legacy held-lease terminal results before ACK

The legacy terminal shortcut now checks Failed outcome/result consistency and
resolves external results with the bounded result port before notifying a parent
or authorizing ACK. ResultBytes verifies the reference/hash envelope and SHA-256.
Failures preserve the existing outcome and held owner and leave dispatch retryable.
Canonical held delivery already uses ReadGraphTerminal; its path is unchanged.

## Evidence

- Baseline 2e8c8ea with the final modeled fixture: actual exit 1, seven invalid or
  unavailable results ACKed, healthy external result not read, and empty Failed
  outcome admitted. Exact original terminal_delivery.go is retained.
- Nine new controls: healthy, unavailable, wrong_bytes, inline_conflict,
  missing_hash, hash_without_ref, failed_external, failed_inline and failed_empty.
  Actual Worker.RunPartitionWithTransport with another owner's held lease checks
  ACK/NAK, zero handlers, exact parent payload/subject or no parent signal,
  unchanged owner revision/value, unchanged terminal mirror, and empty journal.
  Unavailable and wrong-byte reads heal on two fresh worker deliveries.
- Final normal and race both exit 0; exact current parsed YAML CI guards admit
  all 63 prior metadata cases and nine new controls (72 total), plus the existing
  replay/parent-retention tests selected by that job.
- Native race exits 0 in 28.397 s. Expanded timer=false/true × external=false/true
  controls drain 32 duplicates each (128 ACKs; 64 use spilled 1 MiB results), with
  one original effect and unchanged owner, journal and state. Existing 5 MiB
  terminal spill/Await checksum-corruption control also passes. Four clusters for
  held controls and one for the large terminal control use real three-server NATS.
- All 1154 source/module/workflow inputs are unchanged after commands. Four real
  executables were observed and hash/build/parent-PID bound: normal wf, race
  wf/worker and native integration. The very short normal worker executable was
  missed; its actual command exit/log is retained, without claiming capture.
- The first fixture stopped only when all partitions drained. A parent wakeup
  remained pending, causing real-time waits and modeled lease expiry. Its failed
  run is retained under initial-stop-condition; final fixture uses the existing
  StopAfterNextAck boundary. Reviewer verifies that exact fixture-only change.

Run `python3 docs/scale/legacy-held-terminal-result-2026-10-11/review.py`.

## Scope

Unavailable/wrong-byte model responses operate over intact stored fixtures; these
are not NATS fault-conformance cases. Native controls prove healthy held result
reads and retained duplicate drain, not native negative-result fault histories or
all reader/purge/ID-reuse interleavings. The legacy state shortcut still depends
on existing generation/lifecycle rules and is not a full journal-causality proof.
Local CI assertions are checked; hosted CI is unclaimed. Full current qualification
and original release gates remain open. Public continuation admission remains off.
