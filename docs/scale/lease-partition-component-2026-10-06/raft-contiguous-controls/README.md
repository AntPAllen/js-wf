# Bounded trailing-entry refinement

Executed source dc332f2. The refined experimental overlay passes all12 direct state cases and all11 unchanged upstream catch-up controls under race, count1/original3m scope, 2CPU/2GiB. The four original canceled/replaced legacy/modern cases remain included. Eight additional cases cover completed-stream contiguous acceptance and rejection of ahead/behind indexes, wrong previous term, wrong leader, higher/older leader terms, and legacy callbacks.

Original upstream fails the obsolete-callback diagnostics but passes all11 controls. The historical strict guard passes its original four cases, fails the direct contiguous case, and fails the unchanged upstream completed-catchup trailing-entry control. These are retained negative controls. No race report or skip occurs in any of the six scopes. The refined variant fixes both trailing-entry failures while continuing to reject callbacks that could reset the WAL or start/replace catch-up state.

The bounded exception requires no current catch-up, an explicit matching leader term, the known leader identity, and exact previous log term/index. An obsolete subscription is always rejected while another catch-up is active. Legacy and replay behavior retain their earlier boundary. The old strict variant remains the default experimental builder option for historical reproduction; official production dependencies are unchanged.

The independent review binds actual SDK argv/hash/birth/cwd, race build and module inputs, all selected Git and compiled source, unchanged module copies, exact direct/control names and outcomes, and the complete archive. `strict-raft.go.txt` and `contiguous-raft.go.txt` are byte-identical copies of the executed overlays; the archive preserves their original .go paths.

This qualifies the direct12/unchanged11 scope only. It does not replace the failed original170 comparison, the real production-TTL/marker component or sustained SDK qualification of this new variant, full matrices, minimum-version compatibility, dependency delivery, or the original24h gate. Full archive/S3 readback receipts are retained alongside the complete inventory.
