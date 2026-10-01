# Hosted mixed seed 4 latency failure

Source 83253a26554964d0887aee954e4bb56627868e0e,
[run 36822786434](https://github.com/AntPAllen/js-wf/actions/runs/36822786434).
Seeds 1–3 passed; seed 4 fails terminal p99 31.049193766 seconds against the
unchanged under-30-second gate. The slow invocation is mixedsignal/mixed-00-0.
Its journal continues advancing through signal consumption and effect entries
until Completed. Retained artifacts include the failed transcript, operation
history, disk trace and monitoring/server logs. This is a latency failure, not
the preceding seed-5 terminal queue-drain failure. Root cause is unconfirmed;
the terminal held-lease optimization does not prove this active execution
failure fixed. Independent standard test workflow passed on this source.
