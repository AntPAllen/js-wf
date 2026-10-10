import copy, hashlib, json, os, subprocess
from pathlib import Path
root=Path('/home/exedev/js-wf-child-offline-audit-diagnostic-20261010')
artifacts=Path('/home/exedev/js-wf-child-offline-20261010/artifacts')
cli=next(artifacts.rglob('wf')); plugin=cli.parent/'handler.so'
assert plugin.is_file()
source=None
for path in artifacts.rglob('completed.json'):
    bundle=json.loads(path.read_text())
    if any(r['kind']=='SignalConsumed' and r['payload'].get('graph_child',{}).get('result_ref') for r in bundle['journal']):
        source=path; break
assert source
original=json.loads(source.read_text())
rows=[]
for name, field, value in [('control',None,None),('wrong-child-type','type','otherchild'),('wrong-child-id','id','otherchild'),('wrong-child-invocation','inv_seq',999),('wrong-child-result-ref','result_ref','missing-owned-child-result')]:
    bundle=copy.deepcopy(original)
    if field:
        event=next(r['payload'] for r in bundle['journal'] if r['kind']=='SignalConsumed' and r['payload'].get('graph_child'))
        event['graph_child'][field]=value
    path=root/(name+'.json')
    path.write_text(json.dumps(bundle,indent=2)+'\n')
    command=[str(cli),'-url','nats://127.0.0.1:1','-handler-plugin',str(plugin),'-handler-symbol','ChildWorkflow','-replay-bundle',str(path),'replay']
    run=subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=dict(os.environ,WF_REPLAY_EFFECT_MARKER=str(root/'effect')))
    (root/(name+'.log')).write_bytes(run.stdout)
    rows.append(dict(name=name,expected_exit='zero' if name=='control' else 'nonzero',actual_exit=run.returncode,command=command,input_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),output=run.stdout.decode()))
assert rows[0]['actual_exit']==0
assert not (root/'effect').exists()
report=dict(source_bundle=str(source),source_bundle_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),cli=str(cli),cli_sha256=hashlib.sha256(cli.read_bytes()).hexdigest(),plugin=str(plugin),plugin_sha256=hashlib.sha256(plugin.read_bytes()).hexdigest(),rows=rows,scope='Offline graph child annotations versus unchanged runtime request/owned outcome; no native cluster/API used.')
(root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps([dict(name=r['name'],expected_exit=r['expected_exit'],actual_exit=r['actual_exit']) for r in rows],indent=2))
