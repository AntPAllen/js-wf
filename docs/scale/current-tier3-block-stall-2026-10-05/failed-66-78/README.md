# Original disk-stall66–78 failed at provisioning

Original run37164231641/job111324443526/source79915ca/artifact11331688365
stops at seed75: initial provisioning repeatedly reports no suitable peers for
placement, named testFAIL53.01s. No complete seed75 workload report. Uploaded
raw prefix66–74 is preserved without independent qualification;76–78 did not run.
No server-side cause claim. Full867-member archive and split parts read back and
hash-verify;853 original ZIP members have verified canonical unique regular paths
and API digest/size matches. OriginalSDK and physical stores were not uploaded.

Initial capture tried the success-only shard metadata binder, which correctly
rejected the failed job. Rejected source/log is retained. Capture then explicitly
verified failed job/run/source/artifact identity and ZIP digest before extraction;
no successful-shard gate was weakened or verdict promoted.
