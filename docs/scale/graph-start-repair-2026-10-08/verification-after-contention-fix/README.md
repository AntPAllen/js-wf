# Third frozen qualification: failed

Frozen `c194e8c` passes all seven normal commands, ownership race and client race. Reconciler race fails in R1: all3 workflows reach Completed, but the handler effect runs4 times rather than3. R3 passes with3 effects. Four later race commands were not executed. Selected source bytes remain unchanged.

The retained bounded event ring records a reserved20 StepCompleted append losing its CAS at index2, redelivery, and a later successful completion at that index. The entry was not committed by the failed CAS; the computed effect result was discarded when execution returned to redelivery. The exact concurrent authority operation causing that CAS loss was not recorded. Subsequent deterministic reader acquire/release interleaving reproduces the duplicate effect on this original worker, while the fixed worker retries the same result with lease renewal and fresh logical cursor validation. No NATS cause is inferred.
