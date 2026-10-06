# Residual identities confirmed: restored donor cursors

Fresh clean53b3871 full400k/4.8M R5 copy reports two consumers before any current
audit cursor is created. Assignment names and full actual Info agree:
`wf-audit-mweCABktZmjhL89Y7euEqi` (created2026-10-05T19:54:54Z) and
`wf-audit-mweCABktZmjhL89Y7euF5i` (created2026-10-05T19:55:14Z).
Both are R5 memory AckNone, start1, inactive30s, pending4.8M/position0.

Current exact read/reduction completes17.473097037s, with all4.8M entries visited
once. Both current deletes succeed. First residual named API shows exactly the
same two donor cursor names; current R1 journal cursor returns typed404/10014
consumer-not-found. INVcount0. Overall zero-count gate stillfails20.000800454s;
NativeFAIL36.34s, owner SIGKILL never injected. The residual count belongs to the
pre-existing restored assignments. Why they persist is not isolated here.

Independent SDK/686sourceinputs/fiveserverbytes/modules/mounts/closure/1058donor
files verified, plus complete1714-file828alias base-plus-delta reconstruction.
Delta50,170,116bytes SHA256
`d7de4aeb18c830006f95d15f2cb9ef02614de7070481c748ea307e568e401abb`.
Both pinned canonical donor Git parts and delta parts required. Failed fixtures
remain closed; no default/live/fault/24h pass.

Next fresh-copy preparation explicitly targets only these two verified old names,
validates their R5 memory/AckNone/config/created identity before deleting, records
before/deletion/after, checks populations unchanged, and requires zero consumers
before starting the unchanged20s healthy/fault gates. Original donor is untouched.
