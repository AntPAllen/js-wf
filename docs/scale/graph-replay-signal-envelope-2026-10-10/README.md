# Signal envelope admission before replay code

Shared record admission now decodes SignalConsumed with the strict typed
runtime envelope for every export format, including legacy/empty format.
Duplicate, escaped duplicate, aliased and unknown fields fail before handler
or plugin loading; nested child and canonical headers are typed. Missing/null
envelopes are rejected. The encoded user payload remains opaque. Replay uses
the same decoder when extracting signals. Existing graph-v1 canonical
consumption/owned provenance checks remain required.

Development:full SDK race passes37.576s; all853 normal saved traces pass8.351s.
Twenty SDK ambiguity controls and eight preplugin controls cover signal fields
and nested provenance; an encoded duplicate-key user payload stays valid.
Three missing/null controls pass. Disabling only strict signal decoding fails
18 SDK leaves through actual handler entry and7 CLI leaves through plugin
loading. Two nested-child SDK cases and one CLI child case remain rejected
by existing child provenance validation; they are not claimed as required
failures of this decoder mutant. [Negative review](development/negative-review.json).

The first CLI development fixture omitted its mandatory input hash and failed
with an input-hash mismatch before exercising this boundary. Its log is
preserved; the corrected fixture supplies the actual hash. Full CLI package
race verification is still running independently. Development checks are
not frozen qualification or completion of the full original plan.
