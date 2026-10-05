# Failed real-domain all-server retirement admission

Clean444bd43 actual retained race SDK exitsFAIL30.74s: completed server fault
batches=0. The strict scenario reached that assertion after fresh result recovery,
but the injected callback did not complete successfully. It returned an error
without preserving a partial admission record; no successful all-three cut or
healed-domain verdict is independently claimed. First actual candidate therefore
fails, rather than promoting workflow completion alone. Complete originals,
SDK/source/stores/logs retained and all archive members/parts read back.

The initial3fe9cad test patch had a misplaced import, rejected at compile time;
444bd43 corrected it and normal inactive-test compilation passed. No failed
compile is promoted to runtime evidence. Latera525067 adds partial cut stages,
server IDs and callback error capture even on early return, preserving all
original budgets. Diagnostic retry is pending; no runtime/server cause established.
