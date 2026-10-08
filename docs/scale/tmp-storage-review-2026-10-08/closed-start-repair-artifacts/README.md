# Closed Start repair artifacts

Archived closed native failure stores, simulation captures, and development logs from `/tmp`. This cleanup does not change any test verdict; failed qualification remains failed. Active sessions, the original plan attachment and the dirty topology checkout were retained.

The S3 object was downloaded and every archive member checked against its size and SHA-256 before local removal.

- Endpoint: https://nameless-bird-8772.int.exe.xyz
- Bucket: `nameless-bird-8772`
- Key: `js-wf/tmp-cleanup/2026-10-08/bcef0af2ad8ed7db843ab25a82711b62dac0a05fcd69cffd04f4a8c22829f837.tar.gz`

The inventory and capture receipt list the original paths and exact bytes. `removal.json` records a second S3 readback and fresh process-use checks immediately before removal.
