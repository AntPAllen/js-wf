# Million-timer candidate interrupted by VM reboot

Observed 2026-10-05 after the VM reboot: original supervisor and SDK PIDs are
absent, unit inactive with MainPID=0, no visible original-root file descriptors.
The unit's default Result=success is not a campaign result: no execution.json
or terminal report was written. Boot identity/uptime and unit/journal observations
are retained in interruption.json. The previous live observation is historical.

The last running report at07:43:56 records341,740 receipts. Offline recovery with
the actual retained SDK validates the indexed receipt ledger and recovers342,191
of the original million receipts, p99 lateness0.291773s/max18.041029s. Recovery
writes a separate receipt-recovery directory. SHA256 before/after proves the
original report and receipts unchanged. One planned all-three SIGKILL healed in
17.092s; the second planned restart and remaining deliveries did not complete.
7 redeliveries/7 ack errors/53 fetch errors remain recorded. Running final stream
and ack zeroes are placeholders, not drain evidence.

This is an interrupted diagnostic candidate campaign, not a24h/million pass or
server-candidate adoption. No original store is reopened or resumed. Original
SDK/server binaries, source, logs, stores, receipts and recovery observations
remain at /tmp/js-wf-native-million-scheduler-candidate-24h-20261004. The full
archive and every split part are read back/hash-verified (archive-verification.json).
Physical drain and release qualification remain open.
