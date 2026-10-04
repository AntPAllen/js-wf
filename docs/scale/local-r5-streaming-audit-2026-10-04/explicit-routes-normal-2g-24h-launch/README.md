# Explicit-route normal-build 24-hour launch

Executed source: `95b63c0e757095a66f3e15d991fafbb657e6034c`.
Started 2026-10-04 at 21:08:58 UTC under persistent user systemd service
`js-wf-journal-streaming-explicit-routes-normal-2g-24h-20261004.service`.

The journal/seed1 run uses five containers, normal build, explicit 2 GiB memory
limit, GOMAXPROCS=2, combined streaming journal/state checkpoint and final audits,
and explicit all-peer route seeds. Original 20 s attempt / 60 s total audit,
30 s liveness, 60 s heal and 2 min sync gates remain unchanged. The actual test
has a 24h20m timeout.

The launch review verifies 1266 inventoried source inputs against executed Git,
actual SDK digest/build information/environment, live supervisor and all five
container command lines with four route-only peers per node. SDK SHA-256:
`a5beec29e9a7909cd19904020b119e737bba0d1327ef7316e4fcbe2da3d47e39`.
See [launch-review.json](launch-review.json) and the retained launch inputs.

This is launch provenance only. No terminal result, 24-hour qualification,
race-profile qualification, full-matrix acceptance or original failure cause
is claimed. Complete originals and independent terminal review remain required.
