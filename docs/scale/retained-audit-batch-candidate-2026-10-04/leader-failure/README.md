# Retained native candidate fault run: leader loss fails

Executed `7daea560dfa68b4b270d6b43d920f24aabcef652`, actual race SDK. The
candidate is test-only; production audit behavior and20s/60s limits are unchanged.
The package fails, and this run is not promoted to full fault qualification.

- Leader loss: stops after512/3000 visited entries with
  `nats: no responders available for request`. This is an in-process real NATS
  node shutdown; it is not a worker kill or OS SIGKILL.
- Explicit cancellation: stops at128 visited entries in19.932983ms, returns
  `context canceled`, and leaves zero consumers.
- Consumer deletion: explicit scan failure is accepted as a safety control;
  partial results must never be returned as successful completion. This does
  not establish liveness after deletion.
- Three separate NATS2.11.17 processes:997 retained records after three deletions
  match the point-reader digest in1.825612704s; three next-message gap reads,
  zero consumers after cleanup. This narrow compatibility case passed before
  the later explicit consumer-replica and transport-fallback changes.

The current NATS source's default ephemeral-consumer policy selects one replica
when no count is requested. That is a relevant configuration omission in this
candidate, not proof of the sole cause of the observed no-responder response.
The later candidate explicitly requests the stream's replica count, verifies
returned native configuration, and falls back from named transport failures to
leader next-message reads at the next unvisited position. Semantic errors still
fail. Those changes need separate native qualification.

## Complete preserved evidence

All2884 captured inputs and54 Git-local files match executed-source/pre/post
captures. Actual SDK SHA and every build-info field verify. The complete proof
contains3123 members /139877845 uncompressed bytes, including actual SDK,
selected source/module/toolchain byte copies, raw events, native stores/logs,
the executed legacy server and the built modern server. The built modern server
in the legacy fixture was not executed; the current leader-loss/cancellation/
deletion cases run NATS inside the actual SDK. Stores are not independently reopened.

Archive59146636 bytes, three ordered parts. `manifest.json` contains every member
hash/size, part hashes and concatenated SHA; all were independently read back.
Concatenate the parts in numeric order, verify their hashes and the whole archive,
then extract into a fresh directory. Run:

```bash
python3 review.py --root /path/to/extracted/producer --repo /path/to/js-wf --output /tmp/candidate-failure-review.json
```

`review.py` independently checks captured inputs against Git and the actual SDK
and named outcomes. `preserve.py` verifies every original/member/part SHA. Neither
claims store reopening or independent reproduction of the NATS fault. The
original failed producer remains at `/tmp/js-wf-batch-candidate-faults-20261004`.
