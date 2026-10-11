# Raw canonical graph reference audit

`integrity.CheckGraphReferences` accepts raw roots/fences and a bounded physical
object reader. It independently validates frontiers, physical identities,
encoded node hashes/positions, record populations, exact original ready ownership
scopes and node coordinates. Reused payloads must retain an original edge in the
same forest. Named forests and retained reader snapshots are audited, including
after live retirement. It uses shared framing types/constants, with its own
frontier, identity and traversal checks; no production graph walk, membership,
Root.Validate, journal reader or runtime outcome decision is called.

`CheckNativeGraphReferences` reads retained raw authority rows and physical
ObjectStore bytes in a selected isolated namespace. Ordinary and owner-indexed
layouts are supported. Indexed scopes require their durable owner markers.
Authority census/subject/revision and object metadata/byte limits are checked.
Authority and object high-water/population changes reject the observation.
The audit does not publish witnesses or acquire reader pins. Native controls
confirm unchanged authority sequence numbers on healthy observations.

## Required operating contract

Stop all namespace writers, runtime readers, collectors and retention/reuse
for the entire observation, and supply a bounded context and payload byte limit.
Administrative reads are observational evidence; high-water stability does not
prove quiescence, quorum agreement or disk persistence. Authorities/fences are
held in memory; objects are read one at a time with per-node/payload bounds.
The current authority scanner uses bounded parallel point reads across retained
sequence holes; full large-namespace cost qualification is pending.

The report counts visits across live and reader forests, so shared records and
objects may be counted repeatedly. Extra abandoned objects/scopes are permitted
and never reclaimed by the audit. Application descriptors remain opaque.
Runtime input/generation/epoch/index/step/terminal/projection invariants I1/I2/I3/I6,
full object census/physical stores and hostile administration remain separate.
This foundation does not enable admission, import or online collection.

## Verified evidence

- Eleven independent corruption controls reject missing/altered objects,
  closed grants, wrong generations/destinations/heads/coordinates, lost payload
  origins, duplicate readers/streams and wrong record counts. Cancellation and
  payload size limits are also exercised.
- Four native R1/R3 × ordinary/indexed configurations audit live plus retained
  snapshots and reader-owned snapshots after live retirement. Each rejects
  authority moving mid-read and a deliberately closed still-referenced grant.
- All six canonical retention SDK SIGKILL cases now run this raw audit after
  recovery, including both ID-reuse states. The complete focused race passes
  in 138.684 seconds with six explicit raw-audit receipts.
- `development-source.json` hashes the final Go inputs, observed during the
  integrated run; `development-review.json` records actual exits and coverage.
  `final-development-race.log` is the final auditor race result (4.675 seconds).
  Earlier development logs document earlier stages and do not qualify the final
  source. Evidence is scoped to the development worktree.
- CI selects all four native layouts and the corruption corpus, and requires
  six raw-audit receipts in the retention SIGKILL job. YAML, shell and coverage
  gates parse and accept the real passing logs. Hosted execution is pending.

The frozen f15a573 complete159 normal campaign and original 100,000-entry native
campaign retain their original invocations. This new integrity source has its
own focused evidence; complete current-source release qualification and the
original large retention concurrency gate remain open.
