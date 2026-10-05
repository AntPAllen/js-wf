# Instrumented failed-drain fresh-copy observation

ActualSDK/changedb3268e7helper onfresh3413731 creationfirst copies passed within
unchanged20-second observation. Reads4.331s / nativewhole4.351s. Stage output:
parent2504record read27ms,500childtails300ms, consumercensus4.006s, fullrawqueue
census10ms. Predeadline19s stacktimer canceled; no capturedstack claimed.

All137 remaining messages are `parent.large-fanout` on `wf.run.52`; queuecount/
tail stable,64durables/onlyWF_P_52 nonzero136pending1ackpending. ParentCompleted/
result249500,500Completedchildren/exact2×indexresults, originalthree-recordprefix
preserved and successorhigher epoch independently verified. Original2090files
unchanged,1655selectedinputs/helperGit/actualSDK independently verified; complete
closedcopy/source/executable/process/stage/report archives readback verified.

The earlier live timeout sampled138messages before joins; this closedcopy has137.
This observation identifies residualterminal-parent wakeups after newfaileddrain.
It doesnotfix/explain the earlierbaredeadline, measureoriginalruntime operations,
orqualifydrain/fullmatrix/24h. No workers/manualacknowledgments/originalreopens.
Earlierfailedcopy remainsfailed. See [independent review](independent-review.json)
and [archive verification](archive-verification.json).
