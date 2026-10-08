# Closed completion retry artifacts

Archived three closed temporary directories containing Start contention replay captures, completion retry replay captures, and a failed worker trace. Failed evidence is preserved with its original verdict.

The archive was uploaded to the configured S3 endpoint and fully downloaded to verify its SHA-256 and every member before local removal. The exact object key and inventory are in `s3-readback.json` and `fixture-inventory.json`; the executed script records closure checks and removal.

Retained the dirty topology checkout, current Signal implementation, original plan attachment, registered checkouts, and active session files. At review, `/tmp` occupied about 19 MiB and the filesystem had 81 GiB available.
