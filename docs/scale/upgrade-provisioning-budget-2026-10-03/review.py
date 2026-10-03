#!/usr/bin/env python3
"""Independently inspect hosted operation-budget evidence against exact Git bytes."""
import argparse,hashlib,json,subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
root=a.root;revision='256d7ae47342e75ece533565a94ded25d6f32b3d'
before=json.loads((root/'source-before.json').read_text());after=json.loads((root/'source-after.json').read_text())
assert before==after and before['revision']==revision
for name,digest in before['files'].items():
    data=subprocess.check_output(['git','show',revision+':'+name])
    assert hashlib.sha256(data).hexdigest()==digest,name
terminal=json.loads((root/'terminal.json').read_text())
assert terminal['headSha']==revision and terminal['status']=='completed' and terminal['conclusion']=='success'
source=subprocess.check_output(['git','show',revision+':integration/tier3_mixed_upgrade_test.go']).decode()
assert source.count('backend, err := lookup(ctx)')==1
assert (root/'compiled-control.go.txt').read_text()==source.replace('backend, err := lookup(ctx)','backend, err := matrixReadMetadata(ctx, lookup)')
name='TestFiveUpgradeProvisioningUsesWholeProofBudget';other='TestFiveUpgradeProvisioningPreservesCancellationAndBackendFailure'
elapsed={}
for label,action in [('positive','pass'),('control','fail')]:
    events=[json.loads(line) for line in (root/(label+'-events.jsonl')).read_text().splitlines()]
    actions=[e['Action'] for e in events if e.get('Test')==name and e['Action'] in ('run','pass','fail','skip')]
    assert actions==['run',action]
    assert [e['Action'] for e in events if 'Test' not in e and e['Action'] in ('pass','fail','skip')]==[action]
    elapsed[label]=next(e['Elapsed'] for e in events if e.get('Test')==name and e['Action']==action)
    if label=='positive':
        assert elapsed[label]>=2.2
        assert [e['Action'] for e in events if e.get('Test')==other and e['Action'] in ('run','pass','fail','skip')]==['run','pass']
    else:
        output=''.join(e.get('Output','') for e in events if e.get('Test')==name)
        assert 'whole-operation proof=' in output and 'err=upgrade fallback provisioning proof: context deadline exceeded' in output
    command=json.loads((root/(label+'-command.json')).read_text())
    assert '-race' in command and '-p=1' in command and '-count=1' in command
result=dict(run_id=37109195828,revision=revision,source_files_verified=len(before['files']),source_unchanged=True,
            positive_seconds=elapsed['positive'],compiled_control_seconds=elapsed['control'],accepted=True,
            scope='fixture whole-operation deadline preservation with injected two-stage operation',
            confirms_original_server_cause=False,qualifies_mixed_rolling_row=False)
a.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
