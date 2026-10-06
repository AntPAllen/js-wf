# S3 proof copy readback

Endpoint supplied by the user: `https://nameless-bird-8772.int.exe.xyz`.
Bucket: `nameless-bird-8772`. Account-wide listing is rejected because this
integration is pinned to that bucket. Bucket listing and object operations work.

`scripts/offload-proof-to-s3.py` uses documented curl SigV4 support and standard
AWS credential environment variables, with credentials passed through stdin
configuration. AWS CLI is not installed on this VM. Proof keys include the archive
SHA256; conditional PUT prevents replacing existing objects. Metadata keys include
both archive and metadata hashes. Complete streaming GET readbacks verify every
byte against committed archive metadata, followed by local donor rechecks.

The JSON32,498,174-byte proof and metadata were uploaded successfully (HTTP200),
then repeated uploads returned412 and complete readbacks still matched. Receipts
record exact keys, hashes, lengths and source revisions. No presigned public URLs
were generated. This is a verified remote byte copy, not a guarantee of provider
retention or durability. The helper performs no local deletion; canonical Git,
original/failed fixtures, live stores, sources, executables and caches are retained.

Example (credentials supplied separately):

```sh
python3 scripts/offload-proof-to-s3.py \
  --archive /path/to/closed/proof.tar.gz \
  --canonical-metadata docs/scale/example/archive-verification.json \
  --revision COMMIT \
  --endpoint https://nameless-bird-8772.int.exe.xyz \
  --bucket nameless-bird-8772 \
  --receipt /path/to/new/readback-receipt.json
```

To retrieve a recorded proof using an AWS CLI installation:

```sh
aws s3 cp s3://nameless-bird-8772/RECORDED_ARCHIVE_KEY /path/to/proof.tar.gz \
  --endpoint-url https://nameless-bird-8772.int.exe.xyz
```

Verify the downloaded SHA256 and size against the committed receipt before use.
