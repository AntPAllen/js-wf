# Clock probe reply identity and original failure phase

The real clock-worker observer previously accepted any successful `GetMsg` reply's timestamp and sequence without proving it belonged to the just-published probe. A controlled transport can return an unrelated sequence, subject or payload and replace the last locally confirmed sample. Missing receipts/messages could also reach invalid dereferences.

The observer now requires a nonzero publication receipt from `MATRIX_CLOCK`, then a nonnil message with the exact acknowledged sequence, worker subject and byte-identical published payload, plus a nonzero broker timestamp. Only that reply can replace the prior sample. Publication/read failures retain their phase and wrapped cause. Existing two-second probe deadline, 500 ms cadence, four-second freshness and workload latency gates are unchanged.

Controlled transport tests cover a valid current sample, wrong sequence/subject/payload, missing timestamp/message/receipt, wrong receipt stream and zero receipt sequence. Rejections preserve the previous file. Separate publication/read deadline controls retain cause/phase and publish no observation. These are deterministic probe-port controls, not a seeded runtime campaign or proof of a broker defect.

Focused normal tests pass **0.005 s**, race **1.021 s**. Compiling exact original observer file from `ece2f2c` with the new wrong-sequence control fails **0.005 s** specifically because the old path accepts unrelated clock evidence. It is a semantic control, not an original server-timeout reproduction. Four actual fixture/module source hashes match before/after these executions; the actual test executables were not retained. The first normal pass/source variant is also retained; final controls use the current-sample variant.

Original failed campaign source `79915ca`, job 111324439684, has a separate finding: all five seed-6 children left clock samples before exiting, with sequences 6, 7, 3, 4 and 5. Their `worker clock proof: context deadline exceeded` message is emitted only by the periodic ticker path. Initial synchronous writes therefore succeeded; at least two children subsequently updated their samples. Exact five samples and five child logs match the already-committed failed raw manifest. Both old caller and writer sources are retained. Publication versus read failure and the server cause remain unconfirmed; startup consumer pressure remains a hypothesis. This guard change does not fix or qualify that failed campaign.

All **685** runtime/simulator/Tier1 producer inputs remain byte-identical to already race-qualified `283ba32`; only integration fixture files change. The running normal100k gate and older cluster campaigns keep their executed-source scope. No full gate was restarted, no real-cluster qualification is claimed from these controls, and the historical TTL30s worker-kill mismatch was not rerun.

`proof.tar.gz` preserves all 30 source, overlay, control/log, phase-evidence, provenance and exact final preservation-script members. Every member reopens and SHA256-verifies before atomic publication; `manifest.json` records the full digest and member hashes. `analysis.json` records exact scope and executable limits.

A subsequent [healthy real API contract](real-api/) verifies two exact probes
through each of three file-backed peers, normal2.169 s/race3.239 s. All six
stream messages and existing freshness/offset checks pass. This establishes
actual API compatibility, without qualifying the five-container fault row or
attributing the old periodic timeout. Its independent ledgers and preservation
limits are recorded separately.
