# Shared canonical clock in the R5 skew fixture

`WF_TIER3_COMMON_CLOCK=1` enables five independent physical R1 probes for
server-clock rows. Workers and the observed suspended repair loop share one
provider. Sampling limits, cadence, budget, admissions and latency gates retain
their existing values. The Docker node tags survive retained-store restarts.
The new workflow input also requires independent artifact corroboration of the
profile, five exact physical placements, metadata, node configurations, startup
interval and canonical domain on every retained timer request.

Verification:

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 ./integration -run '^TestFiveContainerMixedServerClock' -count=1 -v
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -race -p=1 ./integration -run '^TestMatrix(CanonicalTimerAdmission|PendingClockTimer)' -count=1 -v
python3 -m unittest discover -s scripts -p 'test_tier3*.py'
```

The first command compiles and skips the opt-in native fixtures. Admission
controls pass in1.031s; all40 Python methods pass, including rejection of false
physical topology, legacy timers, altered sampling assumptions and absent
profile. Logs and exact source hashes are retained. These checks prove wiring
and artifact rejection controls, not native recovery. Admitted canonical R5
ahead/behind smoke and sustained campaigns remain necessary. The historical
ahead60.27s failure and incomplete behind sustained run remain open.

The worker-kill31.1s/TTL30s mismatch remains excluded from further runs. The
productionTTL12s/heartbeat3s and strict30s recovery gate remain unchanged.
