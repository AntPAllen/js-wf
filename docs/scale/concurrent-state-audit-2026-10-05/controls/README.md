# Concurrent retained-state audit controls

Experimental explicit APIs overlap a fresh state initial-set watch with journal
scanning after the invocation cohort is fixed. Original sequential APIs remain
the defaults. Complete initial barrier, terminal/snapshot equality, full journal
checks and caller20s/60s limits remain required. Journal errors cancel and join the
snapshot task and watch cleanup before returning.

Race native compaction, corruption and state-value comparisons pass65.995s,
including the concurrent candidate for eachfull/cohort positive/negative case.
Focused overlap/cancellation and initial-set tests pass1.017s. Integration
comparison compiles and skipswithout explicitcopied-store opt-in; it is not a
fullcopied-cohort pass. Full87,920 retainedcohort sequential/concurrent/recheck
comparison andfault/legacy/fullmatrix qualification remainpending.
