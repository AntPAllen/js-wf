#!/usr/bin/env python3
"""Independently review original timer-error race evidence against exact Git."""
import argparse, hashlib, json, subprocess
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--root',type=Path,required=True)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args()
root=a.root
terminal=json.loads((root/'terminal.json').read_text())
revision='223c665d41ba436e1cbd2b47f2f69ed9970c0e7c'
assert terminal['databaseId']==37113167752 and terminal['headSha']==revision
assert terminal['status']=='completed' and terminal['conclusion']=='success'
source=json.loads((root/'source-before.json').read_text())
assert source['revision']==revision and source['files']==json.loads((root/'source-after.json').read_text())
for name,digest in source['files'].items():
    raw=subprocess.check_output(['git','show',revision+':'+name])
    assert hashlib.sha256(raw).hexdigest()==digest,name
original=subprocess.check_output(['git','show',revision+':sim/dispatch_transport.go']).decode()
needle='if fault == "drop_before_commit_success" {\n\t\t\t\treturn nil\n\t\t\t}'
assert original.count(needle)==1
assert (root/'visible-nak-control.go.txt').read_text()==original.replace(needle,'if fault == "drop_before_commit_success" {\n\t\t\t\treturn ErrTransportLost\n\t\t\t}')
test='TestWorkerTimerErrorPendingClockCharacterization'
pin_count=0
for mode in ('positive','negative'):
    events=[json.loads(line) for line in (root/(mode+'-events.jsonl')).read_text().splitlines()]
    assert not any(e['Action'] in ('skip','build-fail') for e in events)
    output=''.join(e.get('Output','') for e in events)
    assert 'panic: test timed out' not in output
    def result(name): return [e['Action'] for e in events if e.get('Test')==name and e['Action'] in ('pass','fail')]
    package=[e['Action'] for e in events if not e.get('Test') and e['Action'] in ('pass','fail')]
    if mode=='positive':
        assert result(test)==['pass'] and package==['pass'] and result('TestPinnedRegressionCorpus')==['pass']
        pin_count=sum(e['Action']=='pass' and e.get('Test','').startswith('TestPinnedRegressionCorpus/') for e in events)
        assert pin_count==267
        for lost,ms in ((False,61000),(True,73000)):
            text=str(lost).lower()
            assert result(test+'/unapplied_nak_'+text)==['pass']
            assert f'virtual_ms={ms} exact_replay=true production_timer=true' in output
            trace=json.loads((root/'traces'/f'timer-error-unapplied-{text}.json').read_text())
            assert trace['seed']==42 and trace['workload']=='worker_timer_error_pending_clock'
            check=[e for e in trace['transport'] if e['operation']=='check_timer_error_pending_clock']
            assert len(check)==1 and check[0]['at_ms']==ms
            assert check[0]['outcome']=='locally_successful_unapplied_nak='+text
            naks=[e for e in trace['transport'] if e['operation']=='consumer_nak']
            assert len(naks)==1 and naks[0]['outcome']==('drop_before_commit_success' if lost else 'ok')
    else:
        assert result(test)==['fail'] and result(test+'/unapplied_nak_true')==['fail'] and package==['fail']
        assert 'local NAK acceptance was not successful: simulated transport lost' in output
report=dict(run_id=37113167752,revision=revision,accepted=True,source_files_verified=len(source['files']),
            actual_race_cases=2,exact_replay_cases=2,pinned_regressions=pin_count,compiled_local_acceptance_control=True,
            applied_nak_virtual_ms=61000,unapplied_nak_virtual_ms=73000,
            production_runtime_changed=False,qualifies_full_seed_gate=False,confirms_seed55_server_cause=False)
a.output.write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report))
