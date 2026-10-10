# Unambiguous imported journal headers

Offline CLI admission now checks a flat wire DTO for epoch, logical index, kind,
payload field, worker ID and physical sequence. Duplicate keys, escaped duplicate
names and case aliases reject before any plugin load. Nested journal payload
bytes stay opaque. The explicit DTO avoids the ambiguity decoder's lack of
anonymous embedded-field flattening in `journal.Record`; a wire-shape control
checks all current Record fields survive import.

Development race verification passes 15 header controls and all14 existing
outer-envelope controls. Thirteen malformed headers demonstrate ordinary JSON
decoding accepts their last values, while the new admission rejects them. All76
retained native producer exports load under race in5.785s. A first development
compile failed because the test used nonexistent `journal.StepResult`; correcting
the fixture to actual `StepCompleted` allowed execution. No native or product
failure was inferred from that compile error.

The [driver](run.py) prepares full frozen replay qualification: full SDK race,
848 normal saved traces, existing CLI/export/native controls and209 offline CLI
outcomes. Disabling only the new journal-header DTO must fail all13 rejection
leaves, alongside37 earlier required mutant failures. The independent
[reviewer](review.py) requires exact Git inputs, binary identities and a loaded,
matching supervisor invocation with actual terminal exit0. Qualification is
prepared, not accepted. Inner payload admission, external authenticity, full
import/fault/retention/public admission/collector and broader original gates
remain open.
