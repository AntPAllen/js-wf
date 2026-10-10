# Seeded canonical replay snapshot lifecycle model

Production graph storage, canonical client and worker export API run through
the in-memory transport. Five modes cover healthy export and retirement or
replacement immediately before/after reader pin CAS. Two generated input sizes
give10 observed mode/size cells. Each generated schedule is replayed exactly,
and every replay must match its complete trace. Failure traces are saved.

Captured exports must contain only the old input/result/epoch or reject with an
empty snapshot. Fresh old-source acquisition after retirement must be stale;
replacement export must contain only its new input/result/invocation, with
Started index0/epoch0. Replacement removes the old invocation pointer last.
The model injects linearized CAS boundary interleavings; it does not reproduce
broker internals, real concurrent process faults, physical collection or full
lifecycle/retention behavior.

Development1000 race seeds pass78.686s, all with exact replay. Five saved pins
pass under race1.349s. Current1000 normal bodies and the full853 normal corpus
pass13.182s, with all10 cells and exact body-count instrumentation.
Disabling only the graph view's generation/retirement admission gate fails all
four retirement/replacement pins with actual exit1, no compile failure.
[Negative source and log](development/), [normal body/corpus evidence](development/current-normal1000-corpus853.log).

Authoritative seeded inventory is now158 families; saved inventory853. The
[frozen driver](run.py) prepares1000 race and100000 normal bodies, the whole853
normal corpus and four required negative leaves. The [reviewer](review.py)
requires exact Git inputs, unchanged sources, both binary identities, actual
counts and all10 cells, and loaded matching supervisor exit0. Extended
qualification is prepared, not accepted. Full latest158 normal/race/extended,
native lifecycle/retention, original matrices and broader gates remain open.

## Extended model qualification accepted

Frozen `fd05fba` passes [independent review](review.json):3395 exact Git
inputs,1000 race bodies,100000 normal bodies, all five lifecycle modes and
ten mode/size cells, exact trace replay, full853 normal pins, four required
generation-gate mutant failures, both binary identities, and loaded matching
supervisor exit0. Normal100000 takes691.628s. Retained [evidence](qualified/)
preserves source inventories, command state, raw events, exact mutant and
terminal supervisor. This accepts this lifecycle family; full latest158
normal/race/extended and original native/retention/admission gates remain open.
