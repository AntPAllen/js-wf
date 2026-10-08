# Closed temporary artifacts

Archived 39 roots: closed development logs and failed simulation traces, committed duplicate child/signal/reconciler pins, and five unused temporary Git object directories. Duplicate pins matched committed bytes before capture. No test verdicts changed.

S3 endpoint: `https://nameless-bird-8772.int.exe.xyz`
Bucket: `nameless-bird-8772`
Key: `js-wf/tmp-cleanup/2026-10-08/c2e10c86788e19520477032a77251f78444a4f8cdec3442f15840d261db23afc.tar.gz`

Archive: 5,728,632 bytes; SHA-256 `c2e10c86788e19520477032a77251f78444a4f8cdec3442f15840d261db23afc`. Verified every member and the full compressed body twice from S3. The manifest and receipt were committed and pushed before local deletion. Privileged process, environment, descriptor, container, mount and loop checks found no references to the selected roots. Original bytes, modes and mtimes were unchanged before removal.

Local originals reclaimed **15,925,248 allocated bytes**. `/tmp` decreased from 31,164 KiB to 15,612 KiB. Transfer archive and staging were also removed.

- [S3 receipt](s3-readback.json)
- [Preserved member inventory](fixture-inventory.json)
- [Removal ledger](removal.json)
- [Executed cleanup](executed-cleanup.py)

Retained: active session data and sockets, original attachment, operational scripts, new uncommitted purge pins, registered checkouts, and topology checkout with uncommitted benchmark changes.
