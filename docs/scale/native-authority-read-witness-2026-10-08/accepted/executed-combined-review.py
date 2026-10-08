import sys,json,hashlib,shutil,re,copy
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-native-authority-witness-20261008');base=repo/'docs/scale/native-authority-read-witness-2026-10-08/accepted'
read=lambda p:json.loads(p.read_text())
source=read(root/'source-before.json')['revision']
assert read(root/'source-before.json')==read(root/'source-after.json')
reports={k:read(base/path) for k,path in [('snapshots','journal/independent-review.json'),('blob','blob-component/independent-review.json'),('reads','read-witness/review.json')]}
assert all(r['source']==source for r in reports.values())
peer_binding=read(base/'blob-component/peer-binding-review.json')
assert peer_binding['source']==source and len(peer_binding['observations'])==len(peer_binding['actual_positive_peer_identity_mutations_rejected'])==14
log=(root/'blob-actual.log').read_text()
for name in ['TestNativeAuthorityReadWitness','TestNativeAuthorityWitnessRacesCommittedReplacement','TestNativeAuthorityReadRequiresQuorum']:
 assert re.search(r'^--- PASS: '+name+r' \(',log,re.M)
assert len(reports['blob']['proofs'])==20 and len(reports['snapshots']['proofs'])==13 and len(reports['reads']['proofs'])==5
actual=read(root/'proxy-actual-sdk.json');binary=read(root/'proxy-binary.json');execution=read(root/'proxy-execution.json');command=read(root/'proxy-commands.json')['run'];proxylog=(root/'proxy-actual.log').read_text()
def proxy(a,b,e,c,l):
 assert e['exit_code']==0 and e['source']==source
 assert a['args']==c and a['exe_sha256']==b['sha256']==hashlib.sha256((root/'proxy-race.test').read_bytes()).hexdigest()
 assert a['admission']['stable_identity_observed_twice'] is True
 assert not Path('/proc',str(a['pid'])).exists()
 assert a['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_NATIVE_SNAPSHOT_ROOT=str(root/'snapshot-stores'),WF_BLOB_AUTHORITY_ROOT=str(root/'blob-stores'))
 assert '-race=true' in b['build_info'] and 'vcs.revision='+source in b['build_info'] and 'vcs.modified=false' in b['build_info']
 assert l.rstrip().endswith('PASS') and not any(x in l for x in ['--- FAIL:','--- SKIP:','DATA RACE'])
 for name in ['TestClientProxyFirstAPIBarrier','TestReadClientPacketRejectsMalformedPublications','TestClientProxyConsumerCreationSelector','TestPublicationExemptionOnlyExaminesHeaders']:
  assert re.search(r'^--- PASS: '+name+r' \(',l,re.M)
proxy(actual,binary,execution,command,proxylog)
controls=[]
for name,change in [('wrong_sdk',lambda a,b,e,c,l:a.update(exe_sha256='0'*64)),('missing_birth_stability',lambda a,b,e,c,l:a['admission'].update(stable_identity_observed_twice=False)),('foreign_source',lambda a,b,e,c,l:e.update(source='foreign')),('wrong_cpu',lambda a,b,e,c,l:a['environment'].update(GOMAXPROCS='3'))]:
 args=[copy.deepcopy(x) for x in [actual,binary,execution,command,proxylog]];change(*args)
 try:proxy(*args)
 except AssertionError:controls.append(name)
 else:raise AssertionError('bad proxy admitted')
out=base/'proxy';out.mkdir(exist_ok=True)
for name in ['proxy-actual-sdk.json','proxy-binary.json','proxy-commands.json','proxy-execution.json','proxy-actual.log']:shutil.copy2(root/name,out/name)
shutil.copy2(__file__,base/'executed-combined-review.py')
(base/'combined-review.json').write_text(json.dumps({'source':source,'source_inputs':reports['blob']['source_inputs_verified'],'native_snapshot_proofs':13,'existing_blob_proofs':20,'new_read_witness_proofs':5,'proof_substitutions_rejected':len(reports['snapshots']['actual_positive_substitutions_rejected'])+len(reports['blob']['actual_positive_proof_substitutions_rejected'])+len(reports['reads']['mutations_rejected'])+len(controls)+14,'native_blob_wire_entrypoints_bound':14,'additional_foreign_entrypoint_id_mutations_rejected':14,'proxy_substitution_controls':controls,'execution':read(root/'execution.json'),'scope':'Source/SDK/full archive and native component acceptance at frozen revision. Not full final-source Tier1/native release matrices, server OS/power loss, all runtime reference migration, concurrent runtime/scale, privileged permissions or production online GC.'},indent=2)+'\n')
print('COMBINED_REVIEW_ACCEPTED',source)
