# Failed first legacy full-audit control

Executed source9c46e34; actual race SDK81dcae0eeb6ca45d100ad9ad849335a9027e0e6b48b344fe63d7ced742f55f70.
The new test fails because its expected orphan error wording is incorrect.
Original and candidate audits all return "journal wf.jrn.audit.legacy-orphan has
no invocation"; the test expected "journal without invocation". This is not a
qualifying legacy pass. Compaction/cohort/later malformed record/terminal
corruption/Delete/recreation/snapshot corruption and repair controls reached
matching reports and exact errors before the final failed assertion.

All2896 selected inputs/66 Git source files, runner, actual SDK/build info/live
memory settings verify. All three actual legacy processes report2.11.17 and
running executable digestdc3a94debfc18ee9762c783d941db36a8360955d9686c86bd660a9acc63701aa,
matching the retained copied binary. Processes are terminal. Complete originals,
member digests and archive parts read back; see manifest.json and independent-review.json.
Concatenate numbered parts to reconstruct proof.tar.gz. Stores were retained,
not reopened. The fixture also builds the modern pinned server but executes only
the copied legacy binary. Correct the test expectation and require a fresh pass.
