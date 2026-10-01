# Ten-minute row failed final metadata/drain check

Compiled clean aec7486 binary ran as independent user service from17:36:53UTC. Test FAIL685.78s: 83 batches, 2,324 completed invocations, all19 journal-leader SIGKILL/restarts, per-type terminal/progress p99 below30s (largest terminal10.778016969s). Histories and retained-state audit passed before the final drain phase. The final WF_RUN metadata request did not answer inside its bounded drain context; this does not prove either a nonempty queue or a drained one. The required final drain was not established, so this is a failed single-row run and no release gate clears.

Raw histories, invocations latencies, fault records, dispatch, suspended scans and final server logs are retained. The old fixture did not capture independent HTTP placement/consumer state during the drain, so the missing-response cause is unconfirmed. The new fixture adds independent monitoring and individual drain-attempt evidence without treating monitoring as a substitute for the actual SDK drain check. Original stores remain /tmp/js-wf-tier3-journal-10m-20261001. Large raw records are gzip compressed.

The binary predates the completed-child notification fix, and this terminal-drain failure does not validate or contradict that separate fix.
