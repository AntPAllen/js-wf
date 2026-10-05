# Continuous projection reader controls

The full PostgreSQL failure trace located a no-responder error at ordered WF_INV
Fetch(256). The pinned SDK recreates that consumer per Fetch; production rebuild
now retains one continuous Messages iterator. This changes the application path;
it does not establish the historical server-side cause.

Race controls: 1,000 seeded retained-hole/watermark cases, exact permanent error
identity, iterator cleanup, cancellation, and zero pending with an unobserved
delivery. Native R3 file-source control confirms actual consumer deletion and
recovery of every one of 600 retained inputs, across real overwritten sequence
holes (5.91s). The combined ready-controls run passed in 11.818s.

The initial native control failed before fault admission during stream creation;
its log is retained. Source admission was corrected to bounded readiness under
the same 30-second control deadline. These logs have named-test scope: the control
SDK executable and original temporary stores were not independently retained.
They do not qualify the full 50,000 projection fault, full matrices or 24 hours.
That changed full case must run with its original budget and complete proof.
