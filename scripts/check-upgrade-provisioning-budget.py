#!/usr/bin/env python3
"""Execute the whole-operation upgrade proof and its compiled 2s-budget control."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--root',type=Path,required=True)
a=p.parse_args();a.root.mkdir(parents=True,exist_ok=True)
def git(*args):return subprocess.check_output(['git',*args],text=True).strip()
def sources():
    names=[n for n in git('ls-files').splitlines() if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum')]
    return {n:hashlib.sha256(Path(n).read_bytes()).hexdigest() for n in names}
if git('status','--porcelain'):raise SystemExit('requires clean committed source')
before=sources()
proof=dict(revision=git('rev-parse','HEAD'),go_version=subprocess.check_output(['go','version'],text=True).strip(),files=before)
(a.root/'source-before.json').write_text(json.dumps(proof,indent=2)+'\n')
source=Path('integration/tier3_mixed_upgrade_test.go').resolve()
original=source.read_text()
needle='backend, err := lookup(ctx)'
if original.count(needle)!=1:raise SystemExit('compiled control target differs')
positive='TestFiveUpgradeProvisioningUsesWholeProofBudget'
other='TestFiveUpgradeProvisioningPreservesCancellationAndBackendFailure'
native='TestFiveUpgradeNativeUsesWholeProofBudget'
native_other='TestFiveUpgradeNativeRejectsUnavailableOrSuccessfulProvisioning'

def run(label,pattern,overlay=None,test_name=positive,marker="whole-operation proof=",expected_error="err=upgrade fallback provisioning proof: context deadline exceeded"):
    command=['go','test','-p=1','-race','-json','./integration','-run',pattern,'-count=1','-timeout=2m']
    if overlay:command.insert(2,'-overlay='+str(overlay))
    env=dict(os.environ,GOMEMLIMIT='512MiB',GOMAXPROCS='2')
    completed=subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,timeout=300)
    (a.root/(label+'-events.jsonl')).write_bytes(completed.stdout)
    (a.root/(label+'-stderr.log')).write_bytes(completed.stderr)
    (a.root/(label+'-command.json')).write_text(json.dumps(command,indent=2)+'\n')
    events=[json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    actions=lambda test:[e.get('Action') for e in events if e.get('Test')==test and e.get('Action') in ('run','pass','fail','skip')]
    package=[e.get('Action') for e in events if 'Test' not in e and e.get('Action') in ('pass','fail','skip')]
    if label=='positive':
        if completed.returncode!=0 or package!=['pass'] or actions(test_name)!=['run','pass'] or any(actions(n)!=['run','pass'] for n in (positive,other,native,native_other)):
            raise ValueError('positive named and package executions did not pass')
        elapsed=next(e['Elapsed'] for e in events if e.get('Test')==positive and e.get('Action')=='pass')
        if elapsed<2.2 or next(e['Elapsed'] for e in events if e.get('Test')==native and e.get('Action')=='pass')<2.2:raise ValueError('whole-operation multi-stage body did not execute')
    else:
        output=''.join(e.get('Output','') for e in events if e.get('Test')==test_name)
        if completed.returncode==0 or package!=['fail'] or actions(test_name)!=['run','fail'] or marker not in output or expected_error not in output:
            raise ValueError('control did not produce the precise executed operation-budget failure')
    return dict(exit_code=completed.returncode,named_test_actions=actions(test_name),package_actions=package)

try:
    result=run('positive','^TestFiveUpgrade(Provisioning(UsesWholeProofBudget|PreservesCancellationAndBackendFailure)|Native(UsesWholeProofBudget|RejectsUnavailableOrSuccessfulProvisioning))$')
    with tempfile.TemporaryDirectory(prefix='upgrade-budget-control-') as directory:
        temporary=Path(directory)
        altered=original.replace(needle,'backend, err := matrixReadMetadata(ctx, lookup)')
        patched=temporary/'upgrade-control.go';patched.write_text(altered)
        (a.root/'compiled-control.go.txt').write_text(altered)
        overlay=temporary/'overlay.json';overlay.write_text(json.dumps({'Replace':{str(source):str(patched)}}))
        control=run('control','^'+positive+'$',overlay)
        native_needle='nativeErr := lookup(ctx)'
        if original.count(native_needle)!=1:raise ValueError('native compiled control target differs')
        native_altered=original.replace(native_needle,'_, nativeErr := matrixReadMetadata(ctx, func(attempt context.Context) (struct{}, error) { return struct{}{}, lookup(attempt) })')
        patched.write_text(native_altered)
        (a.root/'compiled-native-control.go.txt').write_text(native_altered)
        native_control=run('native-control','^'+native+'$',overlay,test_name=native,marker='native-operation proof=',expected_error='err=upgrade native rejection=context deadline exceeded want=stream WF_RUN configuration mismatch:')
    (a.root/'result.json').write_text(json.dumps(dict(positive=result,compiled_two_second_control=control,compiled_native_two_second_control=native_control,
        confirms_original_server_cause=False,qualifies_mixed_rolling_row=False),indent=2)+'\n')
finally:
    after=sources()
    (a.root/'source-after.json').write_text(json.dumps(dict(proof,files=after),indent=2)+'\n')
    if before!=after or source.read_text()!=original:raise ValueError('source changed during qualification')
