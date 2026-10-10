# Terminal replay fixture correction and lifecycle cuts

The original terminal qualifier at `c57dffa` is failed, not accepted:
[preserved failure](../graph-replay-terminal-envelope-2026-10-10/failed-c57dffa/review.json).
Its70 SDK race roots pass;847 saved traces pass and `workflow-determinism.json`
fails because its fixture's terminal result is a numeric JSON field. Actual
runtime `wf.Outcome.Result` is encoded bytes. No native exports or offline CLI
matrix ran before this failure. Original source, events, state, executed scripts
and loaded matching supervisor exit1 are preserved.

The model now constructs terminal journal/state bytes with `wf.Outcome`, and
both successful and deliberately changed SDK replays bind their invocation.
The corrected1000-seed race family passes40.411s, including exact early replays
and cross-process trace identity. All848 normal saved traces pass8.397s. Pin
regeneration changes only15 terminal-payload hash observations across five
invocations. All decisions, event identities/order/count and other fields are
unchanged. [Old trace](fixture-correction/old-workflow-determinism.json) and
[exact difference review](fixture-correction/trace-change-review.json).

Four deterministic production snapshot lifecycle controls pass under race.
Retirement/replacement immediately before reader CAS must reject the old source
with `ErrStale` and an empty snapshot. Retirement/replacement immediately after
the pin commits preserves the complete old input7/result42/epoch3 capture.
Subsequent old-source admission rejects, while replacement input8/result99 has
its own generation and begins at logical index0/epoch0. The replacement fixture
removes the old native invocation pointer after retirement, matching purge
ordering; its first omitted-pointer-removal runs were correctly refused.

These controls use the production graph, client and export API over the
in-memory transport. They are four directed lifecycle cuts, not a new seeded
family, native failure matrix, physical collector proof or full lifecycle gate.
The snapshot cut qualifies one pinned generation; original source handles and
new handles deliberately have different admission rules.

[Retry driver](run.py) and [reviewer](review.py) prepare full frozen70 SDK race
roots,848 normal traces,12 worker export/lifecycle controls, six native exports,
prior CLI controls,209 offline CLI outcomes and117 required mutant failures.
Acceptance requires exact Git inputs, six binary identities and actual loaded
matching supervisor exit0. Retry qualification is prepared, not accepted.
All broader original simulation/fault/retention/import/admission/collector/rollout
gates remain open.
