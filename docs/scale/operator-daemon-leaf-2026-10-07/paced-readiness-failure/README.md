# Paced readiness failure preserved

Clean36aaf70 fresh native root `/tmp/js-wf-operator-daemon-leaf-paced-20261007`; original60s/3m/count1. Matching the existing50ms projection readiness cadence did not resolve the same running-project timeout. Project startup SIGTERM passes1.04s; other cases cannot qualify after the shared scenario expires. Original complete archive, logs and producer failure remain unchanged. No third blind run is started.

Next diagnostic source records the parent's actual consumer-info API responses and readiness errors. NATS2.15.0 source explicitly directs clustered consumer-info handling to the consumer leader; therefore the initial follower-information hypothesis is not established. The preserved child wire's repeated pull/timeouts alone cannot explain which consumer-info observation prevented readiness. No production defect or NATS cause is claimed.
