# Replay bundle envelope admission — 2026-10-10

The previous JSON decoder accepts duplicate keys and case aliases. A duplicate
`format` can replace graph-v1 with the empty compatibility contract. Import now
uses the existing unambiguous checkpoint envelope decoder before typed bundle
admission. Exact duplicates (including escaped names), case aliases of typed
fields, duplicate object names and pending signal header names are rejected.
Unknown fields and trailing JSON remain rejected. Workflow journal payloads
remain opaque; this change does not claim full journal ambiguity admission.

[Development receipt](receipt.json) records 14 race controls, all 76 retained
native exports loaded successfully and existing provenance/input/format/legacy
plugin controls. Retained inputs match the qualified producer copies. The
[old decoder negative control](old-decoder.log) accepts all eight ambiguity
mutations; its actual test exit 1 is required. [Current result](development.log)
is actual exit 0. This is development evidence, not a full frozen qualification
or authenticity/import/fault/rollout acceptance. Earlier frozen input-binding
and broad campaigns run separately and do not cover this change.
