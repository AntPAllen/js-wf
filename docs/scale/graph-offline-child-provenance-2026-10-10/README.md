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
