# Hosted complete cleanup originals

[Run37129135794](https://github.com/AntPAllen/js-wf/actions/runs/37129135794)
uses clean source `bac93561810bed2712dea7a42c487700209a9dcd`.
All1225 original archive members verify against SHA256, including both original
stores and complete compiled-source trees. Offline relocated review verifies
all598 upstream files, exact harness/fixture Git bytes, the sole derived
filestore dirty-count control, recorded commands and actual Go JSON verdicts.
The baseline fails as expected in0.007s; the control passes in0.008s. Both have
0 callbacks and unchanged anchor/last2. Reopen schedule counts are1 versus0.

The independent result matches the hosted review except for the explicitly
recorded cache-readback availability: the runner's original module-cache path
is absent on this VM. Complete archived compiled sources remain independently
hash-verified. The uploaded archive is unaltered and was SHA256-readback checked
again before atomic rename. Run review-hosted.py with this directory and a
repository checkout as arguments to reproduce the review without modifying
original stores. Compilation time is separate from named-package timing.

This confirms the isolated secondary missing-source cleanup persistence bug.
Production dependency and original million-store indexes remain unchanged;
the original million missed-retirement cause and24h drain gate remain open.
