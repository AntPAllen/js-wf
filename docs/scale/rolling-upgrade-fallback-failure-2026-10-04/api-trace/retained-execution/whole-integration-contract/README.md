# Whole integration SDK retention contract

Actual retained driver execution at `cd9468de8f71229a6fae80e7ddd7247c0ecb7d4b` runs `TestFiveUpgradeProvisioningTraceRetainsUnansweredRequest` once under race with1m timeout and integration package cwd. Named test passes0.24s; package1.253s. This is a real NATS request/reply contract with a deliberate unanswered second metadata lookup, not five-container fault qualification.

Independent review checks all3850 selected source inputs/captured byte copies, including563 exact Git-local inputs, unchanged pre/post source, execution commands/cwd/race, emitted events, empty stderr and retained actual SDK SHA256 `473485f8bb6e2f66ebb042c614f5a7c7a76629d5e2678063aac92b2763e4f6b9`.

The canonical proof has3867 members /41,971,813 bytes in two parts. Every member, unchanged original input and concatenated part readback verifies. `manifest.json` binds all member/part/archive hashes; concatenate in listed order, verify archive SHA256 and safely extract into a fresh directory. Actual SDK, build info, captured inputs, runner/commands, full events, independent reviewer and preservation script are retained.

No original NATS stores are retained by this control. Selected Go/Cgo/test/module bytes are a superset, not exhaustive assembly/embed/generated/hermetic provenance. The control does not retrospectively supply the older native seed's missing workload executable, establish historical timeout cause, or qualify full matrices/actual24h.
