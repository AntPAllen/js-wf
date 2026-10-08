import json,copy,hashlib
from pathlib import Path
out=Path('/home/exedev/js-wf/docs/scale/native-authority-read-witness-2026-10-08/accepted/blob-component')
report=json.loads((out/'independent-review.json').read_text());observations=[];controls=[]
def check(identity,info):
 peers=[p for p in identity['peers'] if p['name']==info['server_name']]
 assert len(peers)==1 and peers[0]['id']==info['server_id'] and peers[0]['version']==info['version'] and peers[0]['embedding_commit']==info['git_commit']
for name,proof in report['proofs'].items():
 info=proof.get('server_info') or proof.get('wire',{}).get('server_info')
 if not info:continue
 identity=proof['identity'];check(identity,info)
 bad=copy.deepcopy(identity)
 for peer in bad['peers']:
  if peer['name']==info['server_name']:peer['id']='unobserved-server-id'
 try:check(bad,info)
 except AssertionError:controls.append(name+'/foreign-callee-id')
 else:raise AssertionError('foreign callee accepted')
 observations.append(dict(test=name,actual_server_id=info['server_id'],server_name=info['server_name'],native_fixture_identity_verified=True))
assert len(observations)==len(controls)==14
(out/'peer-binding-review.json').write_text(json.dumps(dict(source=report['source'],observations=observations,actual_positive_peer_identity_mutations_rejected=controls,scope='Link the actual INFO transcript callee ID/name/version/embedding revision to the retained native fixture peer identity. Offline review only; original native SDK not rerun.'),indent=2)+'\n')
(out/'executed-peer-binding-review.py').write_bytes(Path(__file__).read_bytes())
print(json.dumps(dict(native_wire_callees_bound=len(observations),foreign_id_mutations_rejected=len(controls))))
