# Sixth exact archive checkout recovery

At pushed `836863fcea9c5d8eac22da5a5bba3ce20eff06eb`, forty committed archive working files of at least 1 MiB under `docs/scale/` are omitted after exact Git blob, size, SHA256 and open-file checks. This recovers 135,835,648 allocated root bytes. All 791 previously tracked Go/Python/YAML/module inputs remain materialized and identical; HEAD and clean status stay unchanged.

The executed `verify-and-omit.py`, complete selected archive ledger, pre/post source hashes and sparse patterns are preserved here. All omitted bytes remain in local and pushed Git. Failed originals outside Git are untouched; no runtime trial, qualification or remote workflow changes. Restore an individual archive with `git show HEAD:<recorded-path>` and verify its recorded SHA. Disabling sparse checkout restores all historical omissions and needs space for their combined working copies, not just this extension.
