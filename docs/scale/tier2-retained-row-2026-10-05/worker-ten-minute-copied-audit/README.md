# Independent copied ten-minute worker audit

Fresh copies of closed native worker seed1 stores from source `75bcf76`.
The original native test passed in 632.43 s, with 119 confirmed worker SIGKILLs,
31 active-worker faults and terminal p99 15.033519329 s. Its producer failed the
subsequent source check after build-cache cleanup removed one Go-generated test
main source. That failure remains preserved in the native proof; it is not
reclassified as an original producer pass.

The explicit `--reviewed-donor` path at helper source `79fc537` verifies the
separate native review, retained generated source bytes and unchanged durable
inputs before copying. Actual review SDK PID 522835, SHA256
`5750a42e4d9ab3427ebb82b49352c53a41902373f79df550c3be590ef75c7e8a`;
1,695 selected inputs bound to the original production source.

All three operation-history models pass. Full retained integrity reports 1,708
invocations, journals and terminals, and 18,918 entries, matching the native row.
All three client queues and all 64 durables have zero pending and acknowledgement
pending. Audit 394.964 ms; complete reads 401.033 ms within the original 20 s
budget, following existing-asset readiness. No workers, manual acknowledgements
or provisioning are used. All 2,281 original files remain unchanged. Three actual
NATS executable observations and the actual SDK bytes are retained; all observed
processes are closed. Observations are point samples, not complete lifetime tracing.

The archive retains closed copied stores, executable bytes, source inventories,
producer, observers and independent reviewer. All 4,001 members and both archive
parts were read back; concatenated SHA256 is recorded in archive-verification.json.
This proves copied integrity, history and drain for this donor. It does not prove
the full 13-row × 200-seed matrix or erase the original producer failure.
