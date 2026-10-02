# Failed sustained behind-clock admission

[Run36947564272](https://github.com/AntPAllen/js-wf/actions/runs/36947564272),
source28b66c270839de5a648bd56b19a88ce017472779, fails129.53s. It is not a
sustained acceptance result or a completed latency verdict. The first cut is
admitted with the exact suspended tail and removal939.820ms before earliest due;
both roles replace the shifted source and heal. The second scheduled cut never
executes: its10s selection budget expires without a provable shifted-origin wait.
Only one of the intended19 cuts executed. The load is canceled during batch9;
there is no complete final workload verdict to promote.

Both roles are observed on the minus60s server before cut2 at01:33:00.175/.182Z.
The latest three first-wait2s timer origins are batch9 at01:32:59.949/.952/.953Z,
with unshifted server timestamps01:32:59.946/.950/.950Z. No new timer-clock call
is recorded after the role move before cancellation01:33:10.213Z. Thus the
fixture cannot prove the required new shifted-origin pending Sleep. Absolute
deadlines created just before the unshifted→behind transition are consistent
with the existing clock-domain counterexample; these artifacts do not prove
the broker's exact scheduling/redelivery path or a final60s latency.

Full originals, hosted job log and run API response are retained losslessly as
gzip. Every gzip was byte-verified; original-sha256.json records original sizes
and hashes. Keep this run failed. Do not stretch the selection budget or rerun
the campaign as a substitute for fixing common-domain clocks and repairs.
The verified one-cut smoke remains narrower evidence, and all sustained/full
clock-transition gates remain open.
