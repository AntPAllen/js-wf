# Bind Start and release its reader atomically — 2026-10-11

The full native gate stopped before Start binding because the separate preceding
reader-release observation exceeded its cleanup deadline. This change removes
that separate transaction from successful pending Start binding. It does not
establish the native server-side cause or change any timeout.

UpdateApplicationAndReleaseReader validates the original observed head and exact
reader snapshot, changes bounded application bytes and removes only that reader
in one CAS. All forests, ownership receipts and foreign reader snapshots survive.
Its uncertain acknowledgement uses the existing exact expected+1 publication
and reader-set witness; a replaced head or unavailable witness remains unknown.

BindStart now opens and validates the owned input, checks the original token and
current lifecycle/generation, then binds the source invocation and releases its
reader together. Success uses two metadata CAS calls (acquire, bind/release),
previously three (acquire, release, bind). Existing source-pointer verification
by the caller remains mandatory. Failed or already-bound paths retain bounded
cleanup; primary errors are preserved, and uncertain binding is never retried
inside BindStart. No payload validation, original-head fence or deadline is relaxed.

Twelve protocol cuts cover success, stale heads, missing/forged readers, oversized
metadata, cancellation, definite rejection and committed-lost/unknown/replaced
acknowledgements. Eight integration-seam cuts verify the removed cleanup-deadline
boundary, exact input/token validation, one binding attempt, foreign reader access,
explicit recovery and no pin leaks. Initial compile/classification errors are
retained separately.

The old Start fault runner armed the third CAS. The failed migration caught that
it no longer hit binding, and wrote no pins. The runner now arms the second CAS,
the combined binding transaction; fault kinds, modes and all assertions remain.
All946 cases then validate before any pin is written.208 pins change transport
sequences; every seed, scheduler decision, nontransport field and path remains.
No actor schedule, family, fault choice or invariant is removed or changed.
`review.py` independently compares the migrated bytes with bec3049 and verifies
that the runner's only change retargets those three binding cuts.

Eight actual retained SDK executables pass in normal/race modes:123 protocol,
28 client,2 journal and972 simulation named cases per mode, including all946
strict exact pin replays and both new control groups. Source binds3882 inputs.
Other existing simulation Go inputs are unchanged; CI requires both new groups.
This is component/corpus qualification, not the full current-source seeded
campaign or acceptance of a native full-scale gate.
