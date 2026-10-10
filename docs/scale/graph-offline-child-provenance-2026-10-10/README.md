# Offline graph child provenance validation

An offline diagnostic against the retained production race CLI/plugin from
frozenccc8f19 accepts four contradictory graph-child annotations: wrong type,
ID, invocation sequence and result reference. Every unchanged runtime request,
owned outcome and frame remains intact. All malformed runs report completed60,
actual exit0, just like the valid control, with a dead NATS URL and no effect.
[Executed diagnostic](diagnostic/report.json); exact input variants remain at
`/home/exedev/js-wf-child-offline-audit-diagnostic-20261010` with recorded hashes.
This is an offline audit defect, independent of NATS server behavior.

The CLI now validates graph child bindings before loading a handler plugin:
previous request name/type/ID, nonzero invocation, hashed parent-owned outcome,
matching outcome invocation/reference/hash and the external result bytes.
Canonical consumption after a child request requires the graph binding. Ordinary
signals consumed before the request and unannotated legacy signals retain their
existing path. Failed-child and inline outcomes and limit-rejected nested signal
records are handled. Missing objects preserve ErrReplayObjectMissing; malformed
annotations report ErrCorruptJournal. Twenty directed controls exercise these
boundaries and the actual CLI validation path before plugin loading.

Frozen qualification will compile retained race CLI/tests/plugin, replay every
real exported bundle from the native20-case source, require every original
negative control, and reject all four original diagnostic mutations. This fix
and replay audit qualification are separate from the still-live native campaign
atccc8f19, which lacks this validation. No full offline fault/import/rollout,
child-retention/admission, public/default or original release gate is claimed.

## Closed frozen fix qualification

Frozen57393ac97c18875eca1465a670e7ca5d0d3aa093 qualifies independently:
3,317 exact unchanged Git inputs; all20 directed annotation controls and existing
legacy continuation plugin test pass under race (package16.439s); actual five
review/compile/test commands exit0. All106 offline CLI runs satisfy their exact
expected status/error:48 healthy native bundle replays,28 missing-object controls,
20 missing-stage controls, valid diagnostic controls against both old/new CLI,
four malformed new rejections and four required old-reader acceptances. No
callback effect marker is created. The producer's native20-case source remains
ccc8f19 and its source/race/store/collection proof is separate.

[Executed independent review](review.json) verifies those source/binary/command,
complete case/output and input-file hashes. [Raw qualification](qualified/),
[closed run state and complete CLI receipts](state.json),
[actual supervisor exit](supervisor-exit.txt) and [reviewer](review.py) are retained.
The fixed race CLI, plugin and test binary remain in
`/home/exedev/js-wf-offline-child-provenance-20261010`.

The producer verifier originally summarized five inputs after reusing its names
variable for export filename kinds, despite checking all3,312 source inputs.
[Preserved original report, corrected checker/result and executed correction](../graph-continuation-child-offline-2026-10-10/review-count-correction.json)
record this reporting correction without a native rerun. This reviewer binds the
actual original checker hash and corrected producer receipt separately.

This closes the contradictory graph-child binding defect and the named retained-
bundle replay checks. Full offline provenance/fault/buffered-signal/import/rollout,
retention/admission, actual100,000-entry boundary, public/default continuation
admission, production collection and every broader original gate remain open.
