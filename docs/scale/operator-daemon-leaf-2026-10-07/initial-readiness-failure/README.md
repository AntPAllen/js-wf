# Original packaged leaf readiness failure preserved

Clean c3d3591, actual race SDK787788, producer784591, stock leaf788706. Project startup SIGTERM passes1.09s with a complete pending API packet cancelled before upstream forwarding. Project running readiness exceeds the unchanged original60-second scenario; subsequent cases fail on that expired context. Producer records the original native failure and complete fixture archive, without qualification.

The complete running-project wire shows actual WFOPS stream/consumer initialization followed by repeated WF_VIEW pull publications and corresponding408 Request Timeout responses. The packaged child remains alive until parent cancellation; its output log is empty. This proves it reached its pull loop, but does not explain the failed parent NumWaiting observation or qualify the requested signal scenario.

The new test polled projection readiness every10ms, while the existing accepted daemon test polls every50ms after a prior similar unconfirmed timeout. The next source restores that established cadence, preserves NumWaiting>0 as readiness and the original60s/3m/count1 criteria, and uses a fresh root. No server-side cause is asserted. Original root `/tmp/js-wf-operator-daemon-leaf-20261007` and its archive remain closed and untouched pending verified S3 offload.
