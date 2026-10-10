# Bounded seed2 partition dispatch diagnostic

Frozen `70eb819a1bca9e70a1f0a68a6d56ef85b25e2cd0`, original experimental
candidate SHA `a56911eef56974eb10eb8b749a46b618100e8662b524f44be37ba06f2de8ad68`,
35-second workload, seed2, nonrace, two Go CPUs and 2GiB Go memory limit. The
one-minute diagnostic runs on the shared VM while two older qualifications
continue. It changes duration and worker source from the failed original seed2;
it cannot clear that original failure or the full200 row.

The [driver](run.py) verifies the full 77,133,636-byte S3 candidate archive and
manifest before extracting only its executable. Actual SDK and all three peer
executable identities were independently captured while live. Native body
passes in 57.146s, with one confirmed majority partition, seven batches and 196
terminal invocations. Independent [review](review.py) verifies 3,370 exact Git
inputs, unchanged source inventories, executable hashes, original process
closure and loaded matching supervisor invocation/PID0/exit0. This is a
diagnostic outcome; production/default-server adoption is unchanged.

[Results](review.json) independently recompute all 924 raw latency samples and
require complete per-type terminal populations. Raw fanout terminal p99 is
3.638339736s over seven invocations; all six terminal cells are below30s. There
are 3,729 slot-wait and 3,729 complete-pull pairs with no unclosed observations.
Maximum pull is 1.001953670s; maximum slot wait is 14.570736ms. The old30s gap
was not reproduced. Current pull bounding works in this short partition run,
but duration/source differences prevent attribution of the original outlier.
These local observations do not prove broker NAK processing or its deadlines.

The exact original inputs, raw observations, build metadata, admission and
supervisor evidence are [gzip-preserved](qualified/) with original SHA256
digests in the review. Native stores and executable files remain external in
`/home/exedev/js-wf-partition-dispatch-gap-20261010`; no store was reopened.
The first launch's mistyped source argument was rejected before download,
compile or SDK execution: [failed launch](failed-launch.json), [original log](supervisor.log).
The corrected unit is `js-wf-partition-dispatch-gap-v2-20261010.service`, invocation
`bf326e9bcccc469a8f5d86da1d28ea20`, original supervisor PID100789.

The original majority raw p99 remains failed at30.188460653s. Original200,
default dependency, broad simulation/native matrices, actual24h, million
physical drain, canonical admission/retention/collection and rollout stay open.
