# Original committed-manifest assertion mismatch

Source92c5e16 native race case fails at41.47s/41.6151s actual SDK. The real fresh generation3 manifest is committed and read back before SIGKILL; all three peers are killed, held beyond12s productionTTL and restart in the correct domain within20.5607s. Final fresh-handler calls1 trigger the original shared prepublication-loss assertion requiring at least2. The result remains failed; final frame/epoch/integrity/GC assertions were not reached.

The fresh handler explicitly rejects entry when its own generation already has a durable runtime manifest. A committed manifest should restore the continuation rather than re-enter the initial handler; exactly1 fresh call is therefore the stronger committed-case requirement. Prepublication absence keeps its original re-entry requirement. The corrected fixture must require one real commit/drop, exact manifest/frame/generation match, one fresh initial call and all unchanged final integrity/GC/epoch assertions. No recovery deadline changes.

Independent preservation binds1895 exact source files, actual SDKrace/count1/3m and all six closed2.15.0 executable identities, plus2332 complete archive members. No successful runtime or full release qualification is inferred from this original failure.
