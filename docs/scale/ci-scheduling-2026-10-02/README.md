# CI scheduling changes

The source431c934 main cluster job exhausted its aggregate30-minute Go timeout
while TestWorkerResultMissingReplyRecovery/terminal_put_ack_lost had run14s.
This is not proof of a test-local recovery defect. The complete original job log
is retained. The two deliberate reply-hold families now run in a separate
10-minute Go invocation/12-minute job. The original cluster package keeps its
30-minute timeout; latency assertions and test contents are unchanged. Source
inventory verifies that exactly these two families move to the new job.

Push/PR triggers exclude documentation-only changes but reinclude every current
executable fixture/configuration under docs. Manual dispatch and schedules
remain. All three edited/new workflow YAML documents parse successfully.

Four redundant queued documentation-only push runs were canceled after checking
their source changes:37031699961,37031186667,37031700134,37031186714. Manual
qualifications and already-running work were retained. The before metadata and
explicit reasons are archived; cancellation does not constitute test evidence.
Every archived member was SHA256-compared on readback. Fresh CI on the new code
must establish actual full-suite results.
