# Diagnostic race profile after full-suite watchdog failure

One existing saved batch/restart Signal trace passes in 14.321 seconds with race instrumentation and a retained CPU profile. This is a diagnostic run, not full qualification; no prelaunch complete source manifest was captured. Its command/environment, retained binary SHA256/build metadata and log/profile hashes are recorded in `profile.json`.

The flat profile is dominated by race instrumentation (`__tsan_read`, `racecall`, range access and writes). Go-side cumulative samples include JSON marshaling and protocol root normalization. One profile does not establish their contribution to the whole campaign or justify weakening authority validation. No production code or runtime deadline changed from this measurement.

The failed full-149 race evidence remains preserved separately, and the frozen current full-151 normal campaign is live. Full current race qualification still requires completion of every original test, seed body and saved regression.
