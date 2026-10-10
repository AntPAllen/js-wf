# Terminal envelope admission before replay

Shared SDK/CLI/export record admission now checks present Completed and Failed
envelopes with the typed ambiguity decoder before user code or plugin load.
Identity, result/reference/hash, error and limit headers reject duplicate keys,
case aliases and unknown fields. Typed nested LimitEntry headers are checked;
encoded result bytes, LimitRequest and LimitEntry payload contents remain opaque.
This is not full admission of every journal payload or external authenticity.

Identity-free historical replay retains a **missing** terminal payload marker.
Present null/empty envelopes reject, and identity-bound replay always requires
its terminal envelope. The first full SDK development run exposed that legacy
marker case (`TestOfflineReplayWithSignalAndObjectResult`,28.892s failure); the
narrow compatibility correction restores it without skipping present envelopes.
Full SDK race subsequently passes29.412s. Sixteen SDK,13 typed envelope and32
CLI preplugin race controls pass. SDK controls exercise both identity-free and
bound replay; valid typed controls preserve opaque nested user keys.

Restoring ordinary terminal decoding must fail53 leaves:16 SDK handler
admission,32 CLI plugin admission and five nested-header cases. Actual
development negative run exits1 with all53 failures and no compile failure.
[Exact mutant](development/ordinary-terminal.go.txt) and [log](development/ordinary-terminal.log).

[Frozen driver](run.py) and [independent reviewer](review.py) prepare full70 SDK
race roots,848 normal traces, prior CLI/native/export controls,209 offline CLI
outcomes and117 required mutant failures. Acceptance requires exact Git inputs,
six binary identities and actual loaded matching supervisor exit0. Qualification
is prepared, not accepted. Previous shared-header qualification continues on its
own frozen source. Full import/payload/fault/retention/admission/collection/rollout
and all broader original gates remain open.
