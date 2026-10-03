# Full-matrix upgrade profile verification

The full R5 reviewer omitted `--require-start-scan-progress` when regenerating
rolling-upgrade reports, and had no way to require the selected Start-gap or
shutdown profile. Its old call rejects the unchanged accepted SIGKILL and
Lame Duck reports with `uploaded report differs from raw artifact verification`.

The full reviewer now accepts `--require-upgrade-start-gap` and
`--expected-upgrade-shutdown sigkill|ldm`. It forwards both Start-gap and scanner
progress flags only to rolling-upgrade raw review, requires the selected shutdown
mode and one proven gap per confirmed upgrade, and records the requested scope.
Missing, substituted and incomplete profile evidence is rejected.

Five full-matrix verifier tests pass, including profile negative controls and
argument propagation; three existing single-campaign tests also pass. The actual
previously accepted profiles `37132399483` and `37132401225` each regenerate
their row report, event explanation and fencing review byte-for-byte. Each has
1,820 invocations and five confirmed upgrades/gaps, with 4,013/4,023 recorded
Start scans respectively. The initial selective extraction omitted the Lame Duck
server configuration; restoring the original `.conf` completed that review.
No workload is rerun or newly qualified, and the live full campaign is not restarted.

The eight-member archive retains executed controls/reviewer bytes, results and
a 198-file input hash ledger. Every archived member reopens and SHA-verifies.
Original workload archives remain unchanged; their exact hashes are in
`actual-controls.json`. Their original scope does not certify the current full
matrix or any 24h soak.
