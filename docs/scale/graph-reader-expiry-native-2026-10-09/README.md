# Native reader expiry restart and uncertain checkpoint

The focused race integration passes with one and three JetStream replicas (actual tool exit 0, package 4.206 seconds; `result.json`). It exercises native file-backed graph authority/object storage, actual system leases and `WF_STATE` cursor CAS through the shared production reader scheduler.

The fixture creates a retained graph under an expired reader, retires the live graph, and adds an empty root with a still-live pin and another expired empty-root pin. Budget is one destination. The first worker saves its watermark, expires one destination, commits its updated checkpoint and receives an injected timeout at the SDK KV Update boundary. The test explicitly requires that hidden acknowledgment to fire. It stops that worker, releases its lease, reopens native adapters and starts a second worker from only persisted state. The second worker completes the original captured catalog pass and stores the zero checkpoint.

Independent root reads then require both expired pins gone and the live pin retained. Object enumeration requires the original object population unchanged: this reader-only loop cannot use pass completion as a global deletion barrier. This verifies controlled adapter/scheduler restart, not an OS kill, VM reboot, route partition, disk fault or full rollout. Native fault campaigns and operation-level seeded maintenance qualification remain open; default production collection and admission are unchanged.

An initial run passed in 4.405 seconds; it lacked an explicit assertion on the hidden acknowledgment. The accepted 4.206-second run includes that assertion. Both live supervised full151 and corrected actor race campaigns use earlier frozen sources and exclude this integration/scheduler.
