# Additional failures in the original ahead200 campaign

Run37014965084 remains at82f323a593acef65b75102e6bfc9318d1caa0c8a.
These are terminal jobs, independently fetched with original logs and artifacts;
the campaign is still active and cannot satisfy complete200 acceptance.

- Seed20/job110863532931 fails at479.90s: suspended reconciliation stops on
  flattened503/10008 unavailable, the same confirmed client defect as seed13.
  Subsequent context cancellation is secondary. The new source adapter addresses
  its error classification; a new sustained campaign is needed for acceptance.
- Seed27/job110863536623 fails at275.40s, after six accepted kills. Its seventh
  cut fails the strict timer admission check. The request has duration2s;
  earliest due15:07:54.199599633Z, kill starts15:07:52.320903710Z and the
  controller confirms absence15:07:54.594642897Z. Thus the observation is
  395.043264ms past the earliest due. Docker kill returns after2.262732737s,
  and the container is already absent when inspected. This is not the deletion
  defect and does not prove the server actually survived past the deadline;
  the recorded absence is an upper bound on exit. The admission gate correctly
  refuses to certify this cut. Its cause/fixture correction remains open.

No failed cut is retroactively accepted and no admission threshold is relaxed.
The native-delete model does not reproduce the Docker timing issue. Full original
artifact trees, job metadata and logs are retained losslessly in
`originals.tar.gz`; every member was SHA256-checked on readback against its
original and recorded in `manifest.json`. Originals remain in
`/tmp/js-wf-ahead200-additional-failures-37014965084`.
