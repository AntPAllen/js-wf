# Full canonical active purge/reuse native qualification — 2026-10-11

## Terminal closure

Actual SDK exit1 after196.502069164seconds. The first reported failure is old
Start purge-scale/old-00605 with a healthy parent context:
`start outcome unknown: bind: release start reader: observe reader release head: context deadline exceeded`.
The failing reader-release iteration stopped observing the head; this does not
establish whether preceding iterations attempted a CAS or a server-side cause.

The intended full10k/10k/1k population was confirmed at launch, but initial Start
publication did not complete. No old-population, active-population, purge/reuse,
Signal-release or final raw-audit acceptance is claimed. The store is closed:
2481files/225,488,736bytes. `review.py` independently verifies1171 Git inputs,
frozen snapshot bytes, actual normal executable/exit and every closed native file.
The user service is terminal (MainPID0, ExecMainStatus1); the SDK PID is absent.
The three older race campaigns and all original retained stores are unchanged.
Next investigation targets this reader-release observation in the Start binding
path. The full native gate remains open.


This follow-up targets the original full 10,000 old / 10,000 live / 1,000 reused
invocation populations. Count overrides are removed from the environment.
The existing 20-minute parent deadline, 22-minute test deadline, active-mode
handler population checks and raw final audit remain. PostgreSQL projection is
a separate outstanding proof. This is one normal R3 run, not a race acceptance.

The source snapshot is frozen from ac85566eb65a224ccb9c05680cc8443728d1609f:
1171 Go/module files compared byte-for-byte with Git before launch. The driver checks its bytes through
terminal closure and retains the actual executable, /proc executable hash,
actual exit and complete native-store census. No earlier store is reopened.

The dedicated user service uses CPUQuota=200%, CPUWeight=1000, MemoryMax=4G,
RuntimeMaxSec=2400 and KillMode=control-group. The three existing frozen race
campaigns retain their resource configuration. Service metadata and concrete
process identity are required to establish a live run; a preparation file or
observation timeout is not a terminal outcome. Acceptance requires the actual
SDK exit and audit population, retirement and zero-reader-pin checks.

## Verified launch

The actual SDK PID328790 was observed through /proc with executable SHA256
`d58829429031016f44e8b4c58f000f55023a4c5bc4984ef73bbcd0494a64f641`
(54,264,467bytes). Its environment excludes count override and legacy/PostgreSQL
flags; its log confirms canonical=true count=10000 reuse=1000 active=true
full_scale=true. Driver PID328494 belongs to service invocation
3eebcf2b02dc4e38869f0db755b342f8. CPUQuota200%, CPUWeight1000, MemoryMax4GiB,
GOMAXPROCS2 and GOMEMLIMIT2GiB are recorded. This launch is not a passing result.
Live metadata/logs remain mutable until terminal closure. Poll the same user
service and actual SDK; do not restart on an observation timeout.
