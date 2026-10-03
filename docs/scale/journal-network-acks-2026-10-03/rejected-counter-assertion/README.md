# Rejected first full-size network trial

At exact `2134de2b3946e04121368117a0e45998619eff6c`, the retained executable
fails in 5.167 package seconds after 550 completed receipts and the actual
midpoint leader restart. Case 550 rejects a 182-byte increase in forwarded
response counters. The original TCP transcript identifies those bytes as the
preceding `stream_msg_get_response` (404, no message found), not a publish
acknowledgment. Client consumption can precede the proxy's post-write accounting.

The fixture's zero-counter-delta requirement was invalid. The corrected fixture
and independent reviewer check that no actual publish acknowledgment reaches
the client, while retaining payload/CAS, committed/absent, retry, transcript
completeness and all cross-peer checks. A copied actual two-case transcript
with an injected publish acknowledgment and corresponding counter adjustment
is correctly rejected. This correction does not weaken acknowledgment loss.

This trial remains rejected; no thousand-case gate is cleared. The archive
retains the failed executable, original Go events, complete source snapshots and
copied sources, command/driver, all partial receipts and TCP transcript. All
members reopen and SHA-verify before atomic rename. Physical stores were removed
by test cleanup. A fresh full-size execution is required at corrected source.
