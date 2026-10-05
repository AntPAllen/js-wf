# Typed server-shutdown recovery correction

The pinned SDK documents `jetstream.ErrServerShutdown` as pull-request failure
because the server shut down. The initial verified R1 owner-loss fixture returned
that typed status after1947 accepted records, but the shared scanner classified
it as fatal. The transport predicate now recognizes that exact SDK sentinel,
including wrapped values, through its existing original-context/two-resume/leader
fallback path. No new retry budget or metadata overlap allowance.

Visitor errors (including the same typed sentinel), arbitrary matching error
strings, corruption and unqualified consumer-deletion statuses remain fatal.
Race unit/differential/typed-status controls pass1.040s. Native R1 owner loss,
consumer deletion and cancellation are prepared in three fresh fixtures with the
same1500/6000/fullreport/20s/identity/pending/cleanup gates. The initial failed
parent and its two passing subcases stay preserved; no unchanged native rerun.
No public R1 adoption, R5 loss/OS SIGKILL/largepopulation/24h claim yet.
