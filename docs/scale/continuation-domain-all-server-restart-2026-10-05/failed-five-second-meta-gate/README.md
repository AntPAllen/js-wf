# Five-second metadata gate rejected before leader recovery

Clean55b5b93 actual race SDKFAIL31.62s. All three original servers are stopped,
three new IDs restarted; five-second post-restart account-info observation fails
with zero healed responses, cut elapsed5.710203s. Actual library/server and NATS
client failure snapshots show all three running, all configured WFRETIRE, all
clientsCONNECTED, none reporting local metadata leadership. These are boundary
observations, not proof of a persistent server defect. Complete SDK/source/stores/
logs/admission archive and all split parts read back/hash-verify.

The newly added5s metadata gate was stricter than the original30s recovery target.
Replace it with one deadline30s after cut start, including shutdown/restart and
all domain observations;60s whole scenario stays. No unchanged5s rerun required.
The original failure remains preserved, not reclassified as a pass.
