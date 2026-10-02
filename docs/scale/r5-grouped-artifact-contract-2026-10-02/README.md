# Grouped artifact verification against accepted clock-row evidence

The new full R5 collector's per-seed helper verifies a copy of accepted
run 37030391587, ahead-clock seed 27, laid out as a grouped artifact:
`tier3-server_clock_ahead-seed-27-38/tier3-matrix-range/seed-27/`.
It regenerates the actual row report (644 invocations, 19 admitted cuts), event
explanations and fencing timeline byte for byte. All 123 original fixture
files remain unchanged, and the copied fixture is restored byte for byte.

Five controls are rejected:

- Uploaded row invocations changed to 645.
- Uploaded event-explanation fencing count changed to 999999.
- Uploaded fencing-review count changed to 999999.
- A second raw-events file under another `seed-27` directory.
- Requested `seed-28` events absent from the artifact.

The report controls run the real offline checkers before comparing uploaded
bytes. Missing/duplicate-event controls use the same locator as the full
campaign collector. The helper functions were extracted to allow this direct
check; the complete collector still requires every planned row and seed.
All 57 Tier3 guard tests pass after extraction.

This is collector validation using existing immutable runtime evidence. It
does not execute another cluster, qualify a twelve-seed shard, establish a
current fourteen-row campaign result, or clear a 24-hour soak gate. The
producer's scope and source revision remain those of the accepted seed-27 run.

The lossless archive contains executed checker bytes, script-source hashes,
fixture input hashes, regenerated reports and control verdicts, with SHA256
member readback before atomic publication. The 123 raw fixture files are
already preserved in the linked source archive named in `manifest.json`; their
byte-identical copy is excluded here. Mutated-upload recipes are recorded above;
the fixture copies were restored after each control.
