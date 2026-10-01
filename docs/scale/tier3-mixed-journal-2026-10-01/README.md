# Five-container mixed journal-leader smoke

Race PASS: 56.48s test / 57.502s package. Eight mixed batches produce 224 invocations, 2,465 journal entries and one confirmed actual journal-leader SIGKILL/restart. All six workload types pass terminal and enabling-progress p99 below 30s; largest terminal p99 is 9.170451406s. Raw histories pass the production Porcupine models. Final retained-state audit and WF_RUN physical stream plus all 64 consumer drain checks pass. Five R5 peers recover current replicas with production sync_interval=2m. Server logs before the fault and after recovery, dispatch events, suspended scans and latency records are retained.

Removing only published-port reuse makes the address-stability property fail after the first restart in 44.03s; this is a semantic negative control, not a broad timeout. The changed source and overlay mapping are retained. Vet and 27 Python result-verifier tests pass, including false-green rejection controls.

Scope is a 35-second fixture smoke of one row. Five worker objects share one persistent client connection; this is not five worker processes. It does not certify the ten-minute row, 24-hour full matrix, other fault rows, or attribution of every reconciler re-enqueue. Large raw records are gzip compressed. Original native stores remain under /tmp/js-wf-tier3-mixed-journal-race-20261001.
