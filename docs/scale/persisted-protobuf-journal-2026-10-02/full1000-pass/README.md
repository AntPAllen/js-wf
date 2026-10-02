# Full114-workload /259-pin commit gate

Service is inactive/success/exit0. Original report and current guard recheck match
byte-for-byte:158 top-level passes, two trace-only skips,259 pins, and independently
counted seeds1..1000 for all114 workloads.116032 schedules /1783009 choices /
26779285 transport events. The source record is8ffb0ea; simulator/production
runtime dependency files equal that source (empty retained diff). The uncommitted
native integration file and clock-profile label omission were separately found
and committed later; neither changes this simulator source graph. This clears
the whole1000-seed simulator gate at the recorded source, not100k, full race,
protobuf rolling/chaos or a full-release completion claim.
