# Sustained behind-clock early timer failure

Run36939477954 at sourceb13f9c3 fails709.24s on the independent controller
timer audit after88 batches and19 clock-source leader cuts. It does not
clear sustained acceptance. All downloaded originals are retained here,
large files compressed with deterministic gzip; hashes refer to original bytes.

The failing250ms timer is matrixtimer/tier3-1-batch-38-6/timer-5, index16.
A successful clock lookup returns shifted23:17:57.662981282Z, journaling due
23:17:57.912981282Z. In unshifted controller time, the lookup starts
23:18:57.658417761Z and returns23:18:57.665564352Z. Both StepRequested16
and StepCompleted17 occur in delivery1 of run4307. Independent completion
receipt is23:18:57.675820797Z and Sleep returns23:18:57.675969332Z.
Both are about233ms before the earliest controller duration boundary.
This is confirmed early completion, not merely overlapping timing uncertainty.

The old delivery timestamp was eligible against the new shifted deadline,
so the SDK bypassed scheduling/suspension for the fresh sleep. Fresh timer
requests must not consume their current delivery's older wakeup. A regression
using this clock relationship covers Sleep,Await,SelectSignal and Select.
A Go overlay with original SDK files fails all four cases, retained in
sdk-original-negative-control.log. The production fix leaves persisted
replay and due-timer coalescing intact. Broader clock-source transition
counterexamples and the full release gates remain open.
