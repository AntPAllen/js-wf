from pathlib import Path
import re
base = Path(__file__).resolve().parent
log = (base/'race.log').read_text()
for parent in ('false', 'true'):
    controls = ['valid','transport','input-ref-empty','input-ref','pointer','nil','sequence','subject']
    keys = ['Wf-Graph-Start-Token','Wf-Input-SHA256']
    parents = ['Wf-Parent-Type','Wf-Parent-ID','Wf-Parent-Inv-Seq','Wf-Parent-Signal']
    if parent == 'true':
        keys += parents
    else:
        for key in parents:
            controls += [key+'/unexpected-empty',key+'/unexpected-alias']
    for key in keys:
        controls += [key+'/'+mode for mode in ('duplicate','conflicting','empty-list','alias','missing')]
    for control in controls:
        assert f'--- PASS: TestGraphStartMatchesInvocationHeaders/parent={parent}/{control} ' in log
for name in ('TestGraphCanonicalStartPendingBindingAndRetainedInput','TestGraphCanonicalStartRejectsLegacyAndBounds','TestCanonicalStartRecoveryCannotReserveReplacementAfterRetirement','TestCanonicalStartAwaitPendingReplacementAndMissingReadyPointer','TestCanonicalStartAwaitSourceCommittedBeforeBinding','TestCanonicalStartAwaitMissingSourceCannotProvePurge','TestCanonicalBoundStartRepairRequiresExactSourceAndGeneration','TestCanonicalBoundStartRepairDoesNotDeduplicateRecovery'):
    assert f'--- PASS: {name} ' in log
native = (base/'native.log').read_text()
for replicas in (1,3):
    for mode in ('ordinary','reserved','source_committed','bound_no_enqueue'):
        assert f'--- PASS: TestNativeCanonicalStartRecoveryWorkerReplayAndPurge/R{replicas}/{mode} ' in native
for layout in ('R1','R3Domain'):
    assert f'--- PASS: TestNativeGraphContinuationChildPromise/{layout} ' in native
assert native.count('RAW_SDK_CHECKPOINT_AUDIT journals=1 entries=25 terminals=1 pending=0 retired_projection_only=1') == 2
assert native.count('RAW_SDK_CHECKPOINT_HISTORY checkpoints=2') == 2
assert len(re.findall(r'physical drain: zero chunks, \d+ durable attempt tombstones',native)) == 2
print('64 production controls, eight journal/client regressions, eight native start modes and two raw-audited child cases passed')
