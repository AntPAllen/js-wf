#!/usr/bin/env python3
"""Independently check the fresh two-message baseline/control originals."""
import hashlib
import json
from pathlib import Path
import sys

root = Path(sys.argv[1]).resolve()
repo = Path(__file__).resolve().parents[1]
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p): return json.loads(p.read_text())
def inventory(p):
    return {str(f.relative_to(p)):sha(f) for f in sorted(p.rglob('*')) if f.is_file()}
source = read(root/'source.json')
(root/'reviewer.py').write_bytes(Path(__file__).read_bytes())
(root/'reviewer-source.json').write_text(json.dumps(dict(reviewer_sha256=sha(Path(__file__))),indent=2)+'\n')
assert source['version']=='v2.15.0'
before = read(root/'module-before.json')
assert len(before)==598
assert before==read(root/'module-after.json')==inventory(Path(source['module']))
assert sha(root/'runner.py')==source['runner_sha256']==sha(repo/'scripts/check-nats-scheduler-cleanup.py')
assert sha(root/'fixture.go.txt')==source['fixture_sha256']==sha(repo/'scripts/fixtures/nats-scheduler-cleanup_minimal_test.go.txt')
test='TestWorkflowMissingSourceScheduleCleanupPersists'
cases=[]
for mode in ('baseline','dirty-control'):
    case=root/mode
    compiled=read(case/'compiled-inventory.json')
    assert compiled==inventory(case/'nats-source')
    assert compiled.pop('server/workflow_missing_source_cleanup_test.go')==source['fixture_sha256']
    changes={n for n,h in compiled.items() if before.get(n)!=h}
    assert set(compiled)==set(before)
    assert changes==({'server/filestore.go'} if mode=='dirty-control' else set())
    if mode=='dirty-control':
        baseline=(case/'baseline-filestore.go.txt').read_text()
        assert hashlib.sha256(baseline.encode()).hexdigest()==before['server/filestore.go']
        anchor='\tfs.scheduling.running = true\n\n\tscheduledMsgs := fs.scheduling.getScheduledMessages('
        assert baseline.count(anchor)==1
        expected=baseline.replace(anchor,'\tfs.scheduling.running = true\n\tpriorScheduleCount := len(fs.scheduling.schedules)\n\n\tscheduledMsgs := fs.scheduling.getScheduledMessages(')
        point=expected.index('\n\tif len(scheduledMsgs) > 0 {',expected.index('func (fs *fileStore) runMsgScheduling()'))
        expected=expected[:point]+'\n\tif len(fs.scheduling.schedules) != priorScheduleCount {\n\t\tfs.dirty++\n\t}\n'+expected[point:]
        assert (case/'dirty-count-control.go.txt').read_text()==expected
        assert compiled['server/filestore.go']==hashlib.sha256(expected.encode()).hexdigest()
    command=read(case/'command.json')
    assert command['command']==['go','test','-p=1','-json','./server','-run','^'+test+'$','-count=1','-timeout=3m']
    assert command['working_directory']==str(case/'nats-source')
    assert command['environment']==dict(GOWORK='off',GOMEMLIMIT='512MiB',GOMAXPROCS='2')
    events=[json.loads(l) for l in (case/'events.jsonl').read_text().splitlines()]
    assert all(e.get('Package')=='github.com/nats-io/nats-server/v2/server' for e in events)
    assert not any(e['Action'] in ('build-fail','skip') for e in events)
    verdict='fail' if mode=='baseline' else 'pass'
    assert sum(e['Action']=='run' and e.get('Test')==test for e in events)==1
    assert [e['Action'] for e in events if e.get('Test')==test and e['Action'] in ('pass','fail','skip')]==[verdict]
    assert [e['Action'] for e in events if not e.get('Test') and e['Action'] in ('pass','fail','skip')]==[verdict]
    output=''.join(e.get('Output','') for e in events)
    measurement='before=1 after=0 reopened='+('1' if mode=='baseline' else '0')+' callbacks=0 physical=1 last=2'
    assert 'CLEANUP_RESULT fresh_source_count=1 anchor_count=1 '+measurement in output
    assert 'panic:' not in output and 'timed out' not in output
    if mode=='baseline':
        assert 'missing-source cleanup returned after reopen: schedules=1 want=0' in output
        assert sum(e['Action']=='fail' and bool(e.get('Test')) for e in events)==1
    else: assert not any(e['Action']=='fail' for e in events)
    cases.append(dict(mode=mode,actual_test_verdict=verdict,measurement=measurement,changed_upstream_files=sorted(changes),package_seconds=next(e['Elapsed'] for e in events if not e.get('Test') and e['Action']==verdict)))
assert read(root/'result.json')['accepted']
review=dict(accepted=True,module_files_verified=598,module_cache_unchanged=True,runner_sha256=source['runner_sha256'],fixture_sha256=source['fixture_sha256'],source_revision=source['revision'],source_clean_at_execution=source['clean'],cases=cases,scope='Fresh two-message file-store cleanup reproduction and exact dirty-count control; no copied campaign data/index deletion/server/Raft; production dependency unchanged and original million retirement cause unconfirmed.')
(root/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review,indent=2))
