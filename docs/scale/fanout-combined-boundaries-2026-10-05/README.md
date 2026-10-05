# Combined 500-child parent crash and journal restart matrix

The six-boundary race run at executed `e20cf89` failed after 1046.21 seconds.
Creation first/interior/last passed with 500 children, result249500, unchanged
parent prefixes and higher successor epochs. Results first/interior admitted
actual parent SIGKILL and library journal-leader restart but reached the original
five-minute deadline. Results last failed to reach its cut within the original
30-second helper allowance; its combined faults were not admitted.

The failed parent remains failed. No complete combined matrix, full release
matrix or 24-hour qualification is claimed. Recovery and non-admission causes
remain unconfirmed. Library servers are embedded in the actual race SDK; this
case does not kill a separate NATS server process.

The complete [failed proof](failed/) verifies the actual SDK, 665 selected Git
inputs and 3287 selected external inputs before/after, five observed child SDKs,
cut markers, journal restart identities/current replicas and three creation
final proofs. The native acceptance guard rejects the matrix. All16913 archive
members and three parts were read back; archive SHA256:
`d56a6a467ae974419962e7175692255e85c3a4f79443eef26ea059a99fcdfc7d`.
Go test2json conversion timestamps are not original execution timestamps.

The interior result metadata also disproves a count assumption in the guard:
its reconstructed prefix2223 ends at streamseq4194; live messages2210 after
snapshot purging, firstseq766/num_deleted1219. Before/after restart retain2210
messages and lastseq4194. A reconstructed prefix can include archived records,
so live count cannot bound prefix length. Correct future instrumentation using
prefix tail sequence and stream sequence, retaining exact prefix equality.
This does not explain or resolve the recovery deadlines.

Original stores are closed and retained. A separate diagnosis opens only fresh
byte-identical copies; its conclusions will be recorded independently.

## Copied-store diagnosis and fixture correction

Three separate actual SDKs opened fresh copies only. All original closed files
remain byte-identical (2277/2277/2095 files); each diagnosis verifies1655 selected
production/external inputs against original e20cf89 plus the exact fe719fe helper.
All3951 members per diagnostic archive/four total parts read back.

| Result boundary | Terminal children | Incomplete outside children | Parent waits on |
| --- | --- | --- | --- |
| first | 496/500 | 4, all partition16 | signal:child_788 |
| interior | 496/500 | 4, partitions16/34 | signal:child_898 |
| last | 493/500 | 7, partitions16/44 | signal:child_708 |

Parent partition is52. Each awaited signal maps to an incomplete outside child.
First/interior original captured prefixes exactly match the copied parent prefix.
This identifies a fixture admission error: aggregate WF_SIG messages include
children completed in the parent's partition. Counting489 messages does not prove
all489 outside children completed before stopping their only workers. The changed
combined fixture verifies each outside result before joining these workers.
No runtime production behavior or original gate budget changed.

Restart instrumentation now records live count, stream tail sequence and captured
prefix tail sequence, and asserts unchanged count/tail across quiescent restart.
The guard requires sequence coverage, exact final prefix/higher epoch, allsix
native passes and outside-child confirmation before result cuts. Nine controls
pass, including archived-prefix count shrinkage and rejecting missing sequence,
wrong coverage or missing/late/incomplete outside-child admission. Race compile
and opt-in skip passed1.083s; corrected full matrix qualification remains pending.

See [complete copied diagnoses](copied-diagnosis/). Copies have read-only
observations through node0, no workflow execution; they do not prove historical
server-fault causes or physical drain. The original matrix remains failed.

## Confirmed-outside fixture run: five passes, last cut not admitted

Executed161dfa9 race run failed729.17s. Allcreation boundaries and results
first/interior pass, with500children/result249500/exact prefixes/higher epochs.
The result cases confirm489 outside child results before stopping their workers.
Results/interior preserves prefix2227 through streamseq4211 while live messages
2178, exercising the corrected snapshot-aware sequence check. Results/last still
fails cut admission under original30s; no parent fault or journal restart there.
Whole matrix remains failed. Its complete [failed proof](confirmed-failed/)
verifies665Gitinputs/3287selectedexternal inputs/actualSDK/five childSDKs; all16656
members/threeparts read back, archive SHA:
`c0c01b346468a5f81388a5a7375d4cae68a7e98b925fbf354f251c77ccbaf821`.

A separate [last-case copy diagnosis](confirmed-copied-diagnosis/last/)
shows496terminal children; the four incomplete children are all in parent
partition52. Parent tail Suspended waiting_on signal:child_808 maps to one of
them; that durable has493pending/1ackpending. Outside shutdown defect is fixed;
last cut now requires investigation of partition processing/blocked operations.
2086 original files remain identical,1655 selected inputs/exact helper verified;
all3951 members/onepart read back. Diagnostic archive SHA:
`00a4daaa0f60f958f9f95ba81b9d4326ef374e05b3cc1b39361fd0f5f261858e`.
No timeout relaxation, original-store reopen, physical-drain or full-matrix claim.

Portable producer/manual CI prepared atc24e70f; workflow375497385 registered
active, not dispatched yet. Root retains actualSDK/full originals/selectedsource
before-after and exactproducerGit bytes; guard requires allsix combined passes.
