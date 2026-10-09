# Seeded native authority SDK-boundary model — 2026-10-09

`NativeAuthorityTransport` supplies an in-memory durable store at the SDK boundary used by the actual `NativeAuthority` constructor, stream validation, canonical codecs, read witnesses and root/blob CAS operations. It retains opaque canonical bytes, a global physical sequence and conditional last-subject sequences. Read witnesses advance physical authority without changing logical revision. Selected boundary callbacks execute actual peer reads/writes; modeled dropped and committed-but-lost replies remain uncertain. This narrow model implements one durable store and does not establish NATS replication, routing, fsync or server conformance.

The shared `native_authority_witness` family covers root/blob × absent/present × healthy/witness/replacement/exhaustion/drop/lost: 24 combinations. Every chosen case executes and exactly replays the production adapter. Cases assert the unchanged prepared image, preservation of peer replacements, a sixteen-publication bound, no uncertain republication and fresh readback of the actual committed revision/image. Witness cases also choose one, three or seven intervening reads. Initial absent blob mutations obey the actual generation-one uploading/intent rules.

## Executed evidence

- Normal: all 1,000 contiguous completed bodies with exact replay, 0.883 s.
- Race: all 1,000 contiguous completed bodies with exact replay, 9.326 s.
- All 24 newly saved traces: normal 0.030 s, race 1.294 s.
- Pre-fix `6df8289` authority-file overlay: seed 1 reproduces absent-root witness contention in 0.007 s. Shared minimizer preserves/replays that failure in four reproductions, 0.004 s. This is a one-file old-production comparison, not a whole old checkout.
- [Executed source, seed, mode, trace and prior-corpus review](review.json): 1,803 inputs match committed `13a0fd6`; the canonical inventory contains 151 seeded families and 810 pins; all 786 preceding pins remain byte-identical to `4f1f2b2`.

The first fixture used an invalid initial closed blob generation; its failure/trace is retained under `development/invalid-initial-generation*`. The first pin capture used an incorrect relative output path; that harness error is also retained. Neither is counted as a runtime defect. The initial race command selected the family but missed the corpus; separate exact pin subtest filters establish the 24-pin verdict. The negative baseline/minimized traces remain evidence artifacts and are not positive regression pins.

CI includes the new family in its normal/race transport matrix. The existing full 149-family race and 336-combination Cartesian race campaigns retain their independently frozen earlier source scopes. Complete current 151-family/all-810-pin normal/race/extended qualification, arbitrary operation permutations and every original native/runtime/scale/actual-24-hour/physical-drain/migration/adoption/release requirement remain open. Public continuation admission and production collection remain disabled.
