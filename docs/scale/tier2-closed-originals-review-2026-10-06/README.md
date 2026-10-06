# Reusable closed Tier2 original-store verification

`python3 scripts/verify-tier2-closed-originals.py --donor /absolute/native-root --canonical-proof docs/scale/.../native-ten-minute --output /tmp/precopy-verification.json`

Requires current main already pushed. Checks each canonical Git proof part and full concatenation, current archive SHA, the single embedded archive manifest against current manifest bytes, and original SDK/acceptance/process-record bytes against that manifest. Requires successful original producer/duration/observations, captured SDK/worker/server executables and closure, exact original store file set/sizes/hashes, and all visible task descriptors. Original stores are never opened by NATS.

Permission-limited tasks are recorded explicitly as unobservable_paths; this checks visible descriptors and observed owned process closure, not every host process or exhaustive lifetime coverage. Symlink/block-image donors and separately reviewed producer-failure cases require their existing dedicated procedures.

Three tests cover exact originals, five corrupted/missing/extra/symlink/empty-manifest variants, three forged/missing/duplicate embedded-manifest variants, and a real open original file descriptor. All pass. The complete accepted isolation fixture rechecks 2,281 original files and its full canonical native archive without a native or copied NATS rerun. First control attempt assumed all host tasks were readable; corrected implementation explicitly records permission limits.

`run-tier2-copied-audit.py --canonical-proof <repository-relative-native-proof-directory>` now runs this verification before creating fresh copies, retains its executed verifier/report, and rebinds original copy hashes to the reviewed manifest. The flag is opt-in for ordinary successful non-block donors. Original history/integrity/drain gates and native/full-matrix qualification scope remain unchanged. Prepared for the pending original ten-minute fanout copied audit.
