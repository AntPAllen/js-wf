# Concurrent reader live-profile controls

Concurrent journalconsumer leaderloss andcancellation passrace47.536s. Recovery
visitsall6000entries/1500invocations with4039pending atcut in1.915392s; cancellation
returnscontext.Canceled after128visits in273.517ms andjoins snapshotcleanup.
ExistingNATS2.11.17 full/cohort compaction/corruption/state/tombstone comparisons,
nowincluding concurrentmode, passrace48.448s. Threeactuallegacybytes logged.

Explicit --concurrent-state-retained-audit enablesbothcheckpoint andfinalreaders;
originalsequentialdefaults and20s/60s/threeattempt/memory/sync/gate arguments stay.
Conflictingreader modes rejectedbeforetransport. NinePythonlaunchcontrols and
raceGo routingcontrols1.017s pass. Initialselectoronlyranlegacy; journalcontrols
wereexecutedwithcorrectedselector. Livefault/workloadprofile qualificationpending.
