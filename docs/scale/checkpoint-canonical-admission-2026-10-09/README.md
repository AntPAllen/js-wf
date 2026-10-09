# Unambiguous checkpoint envelope admission

Hash matching alone did not reject duplicate or case-aliased typed checkpoint fields. The revised negative control overlays the original 7a248da decoder and executes the final five mutation cases: repeated version, case-aliased version/epoch, duplicate state name, and escaped duplicate state name. Each mutation has a newly computed matching hash and otherwise valid generation/anchor; the original decoder returns restored state. `revised-before-fix.log` records all five failures and actual exit one.

`DecodeUnambiguous` now inspects the declared recovery schema before typed decode. Typed field names must match their JSON names exactly; duplicate decoded keys are rejected. Numeric map keys are normalized for duplicate detection, preventing metadata signal keys `01` and `1` from hiding one another. Frame/user state/locals stored as RawMessage stay opaque. Reordered fields, whitespace, nullable state and unambiguous escaped string keys remain valid with their exact content hash. Byte limits remain caller enforced (16 MiB for these frames/metadata); no resource-capacity or complete hostile-input qualification is claimed.

The frame decoder returns no partial frame on error. The worker uses the same guard for completion-owned checkpoint provenance metadata before installing restored child/signal state. No checkpoint version, serialized writer bytes, continuation activation or collection setting changes.

## Passing focused controls

`format-sdk-race.log` passes the selected frame and SDK groups with actual exit zero: checkpoint package 2.976 seconds, SDK 1.085 seconds. It covers ambiguity rejection with matching hashes, semantic/hash/generation/anchor failures, opaque locals and existing nullable-state behavior. `metadata-race.log` passes in 1.085 seconds, covering repeated/case-aliased/nested metadata fields, aliased numeric map keys, and valid reordered/whitespace metadata. Commands and observed exits are recorded in `results.json`; five directly relevant source hashes are in `review-inputs.json`, not a complete binary provenance manifest.

## Native integration remains partial

The actual native materialized-reference and SDK recovery controls were run under race with the original fixture and package deadlines. At GOMAXPROCS=1, native R1 materialized-reference recovery and both SDK R1/R3-domain flows passed, but the large R3 materialized-reference fixture exceeded its two-minute deadline during bounded stage execution; package exit one at 232.765 seconds.

The same group was rerun at GOMAXPROCS=4, matching the VM CPU count. R1 materialized-reference recovery passed again; R3 again exceeded its unchanged fixture deadline, this time reporting a committed append whose reader refresh remained uncertain at deadline. SDK R1 passed; the package's four-minute watchdog interrupted SDK R3 with no terminal verdict. Actual exit one at 240.096 seconds. The rerun overlaps existing race campaigns, and neither a sole CPU cause nor a NATS defect is established. No native R3 materialized-reference acceptance is claimed. Further runs require a concrete fixture-timing/recovery investigation; no deadline was relaxed here.

## Development contract corrections

The first proposal compared the entire envelope against reserialization. `development-canonical-contract.log` shows that this rejected the existing nullable-state/reordered-envelope SDK contract. It was replaced by schema-aware key validation that preserves those valid bytes. `development-escaped-key-fixture.log` retains an intermediate assertion rejecting a single escaped map key; the final negative case uses two equivalent decoded names and valid single escapes remain accepted. The initial `before-fix.log` belongs to that earlier canonical-byte proposal and is not the authoritative revised negative control.

## Remaining requirements

Native R3 materialized-reference qualification is still failed, and the package watchdog is retained as interrupted evidence rather than reclassified. Complete malformed-history/frame/provenance semantics, pending-child/limit/timer failure paths, resource/scale/clock/storage/route/VM faults, import/offline/deployment compatibility, public continuation admission, production retention collection and every original broader gate remain open. Inventory remains 156 seeded families/841 traces; frozen live/queued campaigns exclude this later decoder change, and full current/all-pin/extended acceptance remains open.
