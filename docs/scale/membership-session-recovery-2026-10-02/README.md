# Joined member-session recovery

Controller.RunWithWorkers supervises the existing registered controller and
partition runner. A recoverable controller error cancels and joins both; only
then can identity/epoch checked lease cleanup and a fresh acknowledged
registration run. Worker-runner errors remain fatal. Invalid permanent errors
remain fatal; context cancellation joins and closes the active session.
Rejoin attempts have8s contexts and250ms backoff. Unknown cleanup can leave the
old registration held until server expiry; rejoin does not bypass Create CAS.
The runner callback must join every loop it started and respect cancellation.

CLI auto mode and the R5 automatic fixture use this production session method.
Fixture SessionEvents retain stopped/rejoin-wait/rejoined epochs. The artifact
guard rejects missing stop/join pairs, non-increasing epochs, identity/time
mismatches and unfinished sessions before final acceptance. Historical artifacts
without the new optional file retain their original review scope.

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 SIM_WRITE_MEMBERSHIP_SESSION_PINS=1 go test -race -p=1 ./sim -run '^TestSeededMembershipSessionRecovery$' -count=1 -v
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -race -p=1 ./sim -run '^TestPinnedRegressionCorpus/(membership-session-|automatic-membership)' -count=1 -v
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -race -p=1 ./assignment ./cmd/wf-worker -count=1
python3 -m unittest discover -s scripts -p 'test_tier3*.py'
```

1,000 seeded schedules pass5.04s / package6.064s, with ten exact replays and
both dropped-before-commit and lost-after-commit renewal outcomes. While old
workers block their join, the old registration remains unchanged. After join,
replacement epoch increases, all64 assignments are claimed, workers restart
and the old controller remains fenced. Two new pins and the previous automatic
membership pin replay exactly in package1.132s. Assignment/CLI race pass
40.482s /30.452s, including actual CLI auto startup/shutdown smoke. All38
Tier3 artifact test methods pass. Source/pin/log hashes are retained.

The seeded runner exercises production supervision/controller/lease decisions
with deterministic KV and controlled worker joins. It does not run a production
workflow handler or prove real fault latency. Full 100k/new234-pin acceptance,
native sustained rejoin, churn/coordinator/fault matrix and 24-hour release
remain open. Fenced lease renewal and takeover claims are not weakened.
