#!/usr/bin/env python3
"""Independently verify both whole-operation bodies and precise compiled controls."""
import argparse,hashlib,json,subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();root=a.root
revision='24f2523994e11aa93ee4ad3ea95e2473d7ec75ba'
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text()) and before['revision']==revision
for name,digest in before['files'].items():assert hashlib.sha256(subprocess.check_output(['git','show',revision+':'+name])).hexdigest()==digest,name
terminal=json.loads((root/'terminal.json').read_text());assert terminal['headSha']==revision and terminal['status']=='completed' and terminal['conclusion']=='success'
source=subprocess.check_output(['git','show',revision+':integration/tier3_mixed_upgrade_test.go']).decode()
controls=[('control','compiled-control.go.txt','backend, err := lookup(ctx)','backend, err := matrixReadMetadata(ctx, lookup)','TestFiveUpgradeProvisioningUsesWholeProofBudget','whole-operation proof=','err=upgrade fallback provisioning proof: context deadline exceeded'),
 ('native-control','compiled-native-control.go.txt','nativeErr := lookup(ctx)','_, nativeErr := matrixReadMetadata(ctx, func(attempt context.Context) (struct{}, error) { return struct{}{}, lookup(attempt) })','TestFiveUpgradeNativeUsesWholeProofBudget','native-operation proof=','err=upgrade native rejection=context deadline exceeded want=stream WF_RUN configuration mismatch:')]
read=lambda label:[json.loads(l) for l in (root/(label+'-events.jsonl')).read_text().splitlines()]
def actions(events,name):return [e['Action'] for e in events if e.get('Test')==name and e['Action'] in ('run','pass','fail','skip')]
def package(events):return [e['Action'] for e in events if 'Test' not in e and e['Action'] in ('pass','fail','skip')]
positive=read('positive');assert package(positive)==['pass']
names=['TestFiveUpgradeProvisioningUsesWholeProofBudget','TestFiveUpgradeProvisioningPreservesCancellationAndBackendFailure','TestFiveUpgradeNativeUsesWholeProofBudget','TestFiveUpgradeNativeRejectsUnavailableOrSuccessfulProvisioning']
for name in names:assert actions(positive,name)==['run','pass']
elapsed={}
for label,path,needle,replacement,name,marker,error in controls:
 assert source.count(needle)==1 and (root/path).read_text()==source.replace(needle,replacement)
 elapsed[name]=next(e['Elapsed'] for e in positive if e.get('Test')==name and e['Action']=='pass');assert elapsed[name]>=2.2
 events=read(label);assert actions(events,name)==['run','fail'] and package(events)==['fail']
 output=''.join(e.get('Output','') for e in events if e.get('Test')==name);assert marker in output and error in output
 command=json.loads((root/(label+'-command.json')).read_text());assert '-race' in command and '-p=1' in command
result=dict(run_id=37111754630,revision=revision,accepted=True,source_files_verified=len(before['files']),source_unchanged=True,actual_positive_tests=4,multi_stage_seconds=elapsed,precise_compiled_controls=2,scope='fixture deadline preservation with injected fallback/native multi-stage operations',qualifies_real_rolling_row=False,confirms_original_server_cause=False)
a.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
