# Seventh exact archive checkout recovery

At pushed `070df4990b4144c7e68ab8188d2251f66cd710c2`, four committed diagnostic archive working copies are omitted after exact Git blob, SHA256, size and open-file checks. This recovers 90,779,648 allocated root bytes. All 792 previously tracked Go/Python/YAML/module inputs remain materialized and identical; HEAD and clean status remain unchanged.

Exact executed verifier, archive ledger, source pre/post hashes and sparse patterns are retained here. Complete archive bytes remain in local/pushed Git; failed originals outside Git are untouched. Restore a selected path with `git show HEAD:<recorded-path>` and verify its recorded SHA. Disabling sparse checkout needs room for all historical omissions. No runtime trial, qualification or remote workflow changes.
