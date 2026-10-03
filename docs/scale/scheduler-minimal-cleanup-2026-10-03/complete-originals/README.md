# Fresh cleanup reproduction with complete originals

Clean source `bac93561810bed2712dea7a42c487700209a9dcd` reproduces the baseline
missing-source cleanup defect in0.035s named package time; the exact copied
filestore dirty-count control passes in0.008s. Compilation wall time is separate.
Both stores are retained explicitly outside Go temporary-directory deletion.
The surviving anchor's subject/payload and physical last sequence2 are checked
before the expected baseline failure. Memory scheduling count changes1→0 in
both cases; reopen returns1 for baseline and0 for the control, with0 callbacks.

Independent review hashes both complete compiled source trees, verifies all598
upstream module files and the unchanged module cache, derives the sole
filestore.go control exactly, and checks harness/fixture bytes against recorded
Git source. Real Go JSON must contain the required baseline fail/control pass,
with no skip/build failure/global timeout. Both original stores are mandatory.

The relocated review passes and four actual evidence mutations are rejected:
missing retained store, inconsistent original command roots, changed compiled
inventory, and removed expected baseline failure. Mutations use disposable
copies; originals are unchanged. Utilities and complete copied sources/stores,
commands, inventories, logs and reports are retained in originals.tar.gz.
Every member SHA256 was verified by reopening the temporary archive before
atomic rename. Extract into a temporary directory, then run:

```sh
python3 /path/to/restored/reviewer.py /path/to/restored --repo /path/to/js-wf --require-retained-store --output /tmp/cleanup-review.json
python3 /path/to/restored/check-portable-controls.py /path/to/restored /path/to/js-wf
```

An offline machine without the original module-cache path still verifies the
complete archived compiled source trees; its review records that cache readback
was unavailable. This confirms the isolated secondary cleanup persistence bug,
not the original million-timer missed-retirement cause. Production dependency
and original million-store indexes remain unchanged. Hosted complete-originals
run37129135794 remains a separate pending qualification.
