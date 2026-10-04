# Observed launch: combined audit five-container race row

Executed source: `d7e075d8a1d2fce8b1b5c32e5a9f2d370213f83e`.
Actual live SDK: `8447a34b56a4ac64a08454f48f1a7882c74ee9483faa5242eedf68abd7bf3107`.

The actual ten-minute journal-leader row runs with race, explicit 2GiB memory,
GOMAXPROCS=2, combined streaming/state-watch checkpoint and final audit mode,
method tracing, original 20s/60s audit deadlines, 30s fault gates and 2m sync.
1259 inventoried source files match the isolated executed Git revision. Actual
live process executable, every Go build-info field and test environment match
the retained binary and producer metadata. Five fixture containers and the
persistent user service were observed live. See launch-review.json.

Unit: js-wf-journal-streaming-state-race-2g-10m-20261004.service.
Producer root: /tmp/js-wf-journal-streaming-state-race-2g-10m-20261004.
Supervisor PID110516; test2json wrapper110833; actual SDK110857.
This snapshot is an observed launch, not a terminal result or a complete original
archive. The producer retains the actual executable, isolated source and stores;
review all original raw histories, audits and final gate assertions after it
terminates. This row alone cannot qualify full matrices or the actual24h soak.
