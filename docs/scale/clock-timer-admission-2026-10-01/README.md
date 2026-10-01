# Pending timer cut candidate selection

The clock-row fixture has a pure candidate selector for a durable positive
Sleep request observed at the latest suspended tail. It requires one matching
successful timer-clock lookup from the request's worker/index, the expected
shifted source, matching retained absolute deadline,matching waiting_on,and
controller time remaining beyond the supplied cut lead budget. Candidate choice
is deterministic by invocation subject, independent of receipt arrival order.

Eighteen controls cover both clock directions: valid selection and rejection
of an expired cut window,completed tail,wrong wait,wrong owner,unknown clock,
unshifted clock,ambiguous matching origins and an unobserved future receipt.
Normal checks pass0.005s and race checks1.019s. No production behavior changes.

This is preparation, not an admitted fault proof. Independent receipts can lag
behind a subsequent journal write. The fault injector must refresh the actual
retained tail and match its sequence/entry to the candidate before SIGKILL;
actual removal must precede the earliest conservative controller duration
boundary. Later final retained data must corroborate the selected prefix at the
cut. Admission must fail closed when those observations are missing or overlap.

The selector is not yet wired into the real R5 fault row or artifact guard.
It does not certify any native pending-timer cut, all effect/continuation cuts,
sustained/200-seed coverage or the full24-hour release matrix. Existing clock
role evidence still explicitly reports incomplete in-flight combination scope.
