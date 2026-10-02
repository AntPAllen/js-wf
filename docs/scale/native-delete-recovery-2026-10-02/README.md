# Native timer deletion recovery

This fixes a confirmed client error-classification defect exposed by ahead200
seeds13/16. It does not establish why the server reported unavailable or which
actor had already deleted a timer hint. The original failed campaign remains
failed; no latency, clock admission or physical drain gate is relaxed.

The pinned modern SDK wraps DeleteMsg errors with a generic sentinel and formats
the API cause as text. The adapter uses the public legacy deletion API, preserves
the configured domain/API prefix, deadline and trace callbacks, then translates
its typed API error for existing production retry decisions. Exact500/10057
`no message found` and ordinary10037 absence are benign. Other10057 failures
(including deletion denied and storage I/O) remain typed permanent errors.
Error-free responses must explicitly confirm success. Fallback timer deletion
uses the same adapter; its existing normal recovery contracts pass.

## Accepted local evidence

The final contract runner passes under Go race instrumentation:

- Actual production model seeds1..1000, all eight modes and both scanners.
- Fourteen real R3 reply cases across custom API prefix/domain routes, actual
  retained hints and physical deletion, typed failures and trace callbacks.
- Existing terminal-journal retirement and three fallback contracts.
- All264 source pins execute successfully; seeds3/18 export byte-identical pins
  in separate processes.
- Retained source overlay recreating cause flattening produces actual named
  production-loop failures for seed3/already-absent and seed18/unavailable.
  The real R3 contract also fails on missing typed causes/benign absence.
  Build errors, missing executions, skips and timeouts cannot count as detection.

Source inventory is recorded before/after the final runner. Its base HEAD is
0525be88d507fbb1b3dfe1739afbc7a8414bf2d6 with the fix present in the worktree;
the per-file hashes identify the tested code. The subsequent commit contains
these exact production/model/test/runner/workflow bytes. This is focused local
acceptance, not hosted acceptance or current-graph full-suite/release acceptance.

`originals.tar.gz` losslessly retains all initial evidence and the final runner's
JSON events, stderr, mutant source, overlay, failure traces, intact exports and
source/report manifests. Every file was read back and SHA256-compared with its
original; `manifest.json` records the hashes. Original temporary files remain.
The initial `positive/pinned.log` has an incorrect regex and ran no tests; it
is preserved but rejected. Initial `negative/already-absent-failure.json` actually
contains an earlier seed2 permanent-error classification failure and is not an
absence reproduction. The explicit seed3/18 traces provide that evidence.

Run the permanent contract gate with:

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 python3 scripts/check-native-delete-contract.py --root /tmp/native-delete-new-run
```

The new workload is118 and the corpus264. Older116/117 campaign results retain
their original scope. Fresh full1k/100k, hosted contract and admitted R5 clock
rows remain required before broader release claims.
