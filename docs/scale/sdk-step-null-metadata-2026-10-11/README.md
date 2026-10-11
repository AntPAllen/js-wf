# SDK step null metadata admission — 2026-10-11

Production stepwire request/completion admission rejects present null runtime
metadata, including every request field, nested case metadata and null case
objects. Completion result remains opaque user JSON, including null and duplicate
user keys. Legacy omitted/empty kind and omitted optional metadata remain valid;
explicit zero/false/empty values remain readable. Existing complete unambiguous
wire decoding still rejects unknown/aliased/duplicate fields. Kind-specific
semantics remain with the existing callers.

The raw graph auditor has an independent implementation of this field check;
it does not call the production stepwire decoder. Rejected limit requests also
receive the existing step-envelope audit, including this new check.

## Baselines

At78ee4ee plus retained matching regression fixtures:

- Raw graph auditing accepted error:null in a step completion (actual exit1
  from the regression assertion).
- SDK replay accepted the same field and entered the handler once (actual
  exit1; handlers1/effects0/errornil). The fixed replay rejects before handler.

Exact fixtures, commands/source and logs are retained. Baseline executables were
not captured. This corruption admission gap is not a NATS fault diagnosis.

## Verification

- Raw race exit0 in42.876s: all41 raw graph top-level tests and four native
  checkpoint/census cases. New68 physical controls:10 positive/58 negative,
  JSON/protobuf request/case/completion metadata, null case objects, legacy
  defaults, explicit zeros and opaque user null/duplicate keys. Every physical
  fixture passes independent reference checking before semantic audit.
- Complete wf and stepwire race suites pass in95.957s/1.076s.28 SDK negatives
  assert zero handler/effect calls before rejection;60 wire null/zero/opaque
  controls include preserved legacy variants and opaque nested user data.
- Four native R1/R3 child/buffered-child compatibility flows pass in86.345s
  with four verified start/terminal witness pairs and eight checkpoint frames.
  Native stores are ephemeral; no restoration/power proof is claimed.
- All four final test executables were captured live with SHA/build/command
  metadata.1136 prelaunch Go/module inputs remain unchanged.
- CI was extended only after the three runtime commands closed. The captured
  before/after workflow bytes/hashes distinguish that edit from unchanged
  runtime inputs. A new normal/race sdk-step-metadata job and relevant stepwire/
  test trigger paths are added. Exact new raw and SDK guards pass against the
  final local race logs; YAML parses. Normal focused CI command and hosted
  execution are not claimed as executed locally.

Run from the repository root:

    python3 docs/scale/sdk-step-null-metadata-2026-10-11/review.py

Original I4 full program/causal history, omitted required kind-specific field
semantics, lease/client linearizability, current complete162/922 normal/race/
extended and original fault/scale/soak/power/rollout gates remain open. The full161
normal and four older race/scale campaigns are frozen and exclude this code.
