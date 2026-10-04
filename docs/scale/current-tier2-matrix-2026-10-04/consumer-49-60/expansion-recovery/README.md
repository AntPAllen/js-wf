# Verified duplicate expansion recovery — consumer49-60

At pushed `9416bf067e206fa01565458025c982a32cefd236` (exact full revision in recovery.json), the retained canonical archive and all206 member contents verify against the committed manifest. Every pushed Git part and their concatenated SHA verifies; all72 raw files match original reviewer and canonical hashes. No accessible process file descriptor references the expanded directory. Each file is unshared; actual model executable SHA verifies before/after removal.

Removed only the redundant raw expansion: **669978624 exclusive allocated bytes** recovered. Complete raw evidence remains in the retained canonical archive and pushed Git parts; actual model executable remains local/archived. Reconstruct raw paths from that canonical proof before further review. Failed originals are untouched; qualification scope is unchanged.

The cleanup and recovery record completed before a final redundant script-copy raised SameFileError. The executed script was already present in the destination. Retained canonical SHA and removed-path absence were rechecked; deletion was not rerun. See post-removal-script-copy-error.txt.
