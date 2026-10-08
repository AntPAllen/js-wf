# Closed purge and signal temporary artifacts

Preserved two closed native purge boundary NATS stores and duplicate purge/signal-client simulation fixtures. All duplicate fixtures matched committed Git bytes. Active session data, the original plan, pending parent-worker verification, operational scripts and registered checkouts were retained, including the topology checkout with uncommitted changes.

S3 endpoint: `https://nameless-bird-8772.int.exe.xyz`  
Bucket: `nameless-bird-8772`  
Key: `js-wf/tmp-cleanup/2026-10-08/9c634fbe537fbc600b5eb53216beee13606dd5a07b1ed8d15d36be2d44b15d80.tar.gz`

The complete compressed archive and every member were verified by S3 readback before committing this receipt. Local removal requires a second verified readback, unchanged original inventory, no live process references and confirmation that the receipt is pushed to main.

- [S3 receipt](s3-readback.json)
- [Member inventory](fixture-inventory.json)
- [Executed cleanup](executed-cleanup.py)
