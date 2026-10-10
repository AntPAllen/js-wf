# Shared replay journal ordering before plugin admission — 2026-10-10

The shared graph history API now checks the existing SDK logical record contract
before a CLI plugin is opened or a worker snapshot is exported: 100,000-entry cap,
contiguous logical indices, nonzero strictly increasing physical sequences,
nondecreasing fencing epochs, one initial Started, known kinds and no records
after a terminal. Supplied invocation identity binds Completed/Failed tails;
Failed requires a nonempty error. The SDK uses the same validator and removes
its duplicate header checks. Physical stream sequence gaps and legacy zero epochs
retain compatibility. Complete step/body/schema/authenticity gates remain separate.

[Development receipt](development-receipt.json): 15 SDK controls and 18 CLI
controls across unversioned/graph-v1 pass under race, alongside existing declared
format/input/provenance/legacy plugin tests. Disabling the shared validator must
fail all 31 rejecting leaves; [negative output](negative.log) has actual exit1.
The first malformed test selector returned exit0 with no tests; it is explicitly
invalid and preserved in [selector output](wrong-selector-no-tests.log).

The frozen [runner](run.py) retains all prior input/format/metadata/child/native
controls, adds envelope and ordering controls, runs full SDK race (66 top tests),
848 normal saved traces, six native CLI exports, eight worker exports, 33 required
mutant failures and 209 actual offline CLI invocations. [Reviewer](review.py)
requires retained terminal supervisor exit0 and exact source/binary/event/input
identities. This qualification is pending. Full latest157 seeded/extended,
original faults, retention, import, public admission, production collection and
rollout remain open. Existing long campaigns are independent and unmodified.
