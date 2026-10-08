import sys,json,hashlib,subprocess,shutil,copy,re,io,datetime
from pathlib import Path
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import *
root=Path('/tmp/js-wf-retained-append-graph-20261008');out=repo/'docs/scale/retained-append-graph-2026-10-08/accepted'
read=lambda p:json.loads(p.read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
before=read(root/'source-before.json');assert before==read(root/'source-after.json');rev=before['revision'];assert rev=='171d9f9f80466acc0e49cf26b5232fcfa145d395'
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();selected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(selected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],input=''.join(rev+':'+n+'\n' for n in selected).encode(),cwd=repo))
for n in selected:
 header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 p=root/'selected-source'/n
 if p.suffix=='.go':p=p.with_suffix('.go.txt')
 assert hashlib.sha256(data).hexdigest()==before['files'][n]==sha(p)
assert not stream.read()
unit=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show','js-wf-retained-append-graph-20261008.service','--property=ActiveState,SubState,MainPID,ExecMainPID,Result,ExecMainStatus,InvocationID,Restart,MemoryMax,RuntimeMaxUSec,CPUQuotaPerSecUSec,ExecMainStartTimestamp,ExecMainExitTimestamp'],text=True).splitlines())
assert unit['InvocationID']=='2eb7f672a42e4ba09a8152297aaa7231' and unit['MainPID']=='0' and unit['SubState']=='exited' and unit['Result']=='success' and unit['ExecMainStatus']=='0' and unit['Restart']=='no'
assert unit['RuntimeMaxUSec']=='12min' and unit['MemoryMax']=='3221225472' and unit['CPUQuotaPerSecUSec']=='1s'
assert unit['ExecMainPID']==str(read(root/'execution.json')['producer_pid']) and not Path('/proc',unit['ExecMainPID']).exists()
expected=['TestAppendGraph100000BoundedWritesAndCompleteTraversal','TestAppendGraphFailedUploadNeverAdoptsOrMutatesRoot','TestAppendGraphCorruptOrMissingNodesFailClosed','TestAppendGraphFrontierAndRecordBounds','TestAppendGraphCancellationAndVisitorStop','TestAppendGraphIndependentForksAndCopies']
metrics=dict(records=100000,staged_nodes=199994,max_append_writes=17,max_append_reads=16,max_root_bytes=3752,encoded_node_bytes=58514498,walked_nodes=199994,payload_edges=788)
def check(events,inventory):
 assert inventory.splitlines()==expected
 assert all(e['Package']=='js-wf/internal/retainedgraph' for e in events)
 assert not any(e['Action'] in ('fail','skip') for e in events)
 assert sorted(e['Test'] for e in events if e['Action']=='pass' and 'Test' in e and '/' not in e['Test'])==sorted(expected)
 terminal=[e for e in events if e['Action']=='pass' and 'Test' not in e];assert len(terminal)==1 and terminal[0]['Elapsed']<300
 output=''.join(e.get('Output','') for e in events);assert 'DATA RACE' not in output and output.rstrip().endswith('PASS')
 markers=re.findall(r'RETAINED_GRAPH_SCALE ([^\n]+)',output);assert len(markers)==1
 actual={k:int(v) for k,v in (f.split('=') for f in markers[0].split())};assert actual==metrics
 for test,subs in [('TestAppendGraphFailedUploadNeverAdoptsOrMutatesRoot',['lost_ack','bad_receipt','cancel_after_upload']),('TestAppendGraphCorruptOrMissingNodesFailClosed',['missing','corrupt_bytes','wrong_position','unknown_field','duplicate_field','trailing_json','oversized'])]:
  assert sorted(e['Test'] for e in events if e['Action']=='pass' and e.get('Test','').startswith(test+'/'))==sorted(test+'/'+s for s in subs)
 return dict(tests=expected,metrics=actual,elapsed=terminal[0]['Elapsed'])
results=[];controls=[]
for label in ['normal','race']:
 a=read(root/(label+'-actual-sdk.json'));b=read(root/(label+'-binary.json'));e=read(root/(label+'-execution.json'));cmd=read(root/(label+'-commands.json'))
 assert e['source']==rev and e['exit_code']==e['converter_exit_code']==0 and a['args']==cmd['run']==[str(root/(label+'.test')),'-test.run=^TestAppendGraph','-test.v=test2json','-test.count=1','-test.timeout=5m']
 assert a['admission']['stable_identity_observed_twice'] and a['exe_sha256']==b['sha256']==sha(root/(label+'.test')) and not Path('/proc',str(a['pid'])).exists()
 assert a['environment']==dict(GOMAXPROCS='1',GOMEMLIMIT='512MiB') and a['working_directory']==str(repo)
 assert 'vcs.revision='+rev in b['build_info'] and 'vcs.modified=false' in b['build_info'] and ('-race=true' in b['build_info'])==(label=='race')
 events=[json.loads(s) for s in (root/(label+'-events.jsonl')).read_text().splitlines()];inventory=(root/(label+'-inventory.txt')).read_text();result=check(events,inventory)
 raw=(root/(label+'-actual.log')).read_text().replace('\x16','');normalized=''.join(line for line in raw.splitlines(True) if not line.startswith('=== NAME  '));assert normalized==''.join(x.get('Output','') for x in events)
 def reject(name,ev,inv=inventory):
  try:check(ev,inv)
  except (AssertionError,KeyError,ValueError,TypeError):controls.append(label+'/'+name);return
  raise AssertionError('invalid graph proof accepted '+name)
 reject('missing_package_terminal',[x for x in events if 'Test' in x or x['Action']!='pass'])
 reject('duplicate_package_terminal',events+[x for x in events if 'Test' not in x and x['Action']=='pass'][0:1])
 reject('missing_scale_test',[x for x in events if not(x.get('Test')==expected[0] and x['Action']=='pass')])
 reject('missing_failure_control',[x for x in events if not(x.get('Test')==expected[1]+'/lost_ack' and x['Action']=='pass')])
 reject('short_inventory',events,inv='\n'.join(inventory.splitlines()[1:])+'\n')
 marker=next(i for i,x in enumerate(events) if 'RETAINED_GRAPH_SCALE ' in x.get('Output',''))
 reject('missing_scale_marker',[x for i,x in enumerate(events) if i!=marker]);reject('duplicate_scale_marker',events+[events[marker]])
 for field in ['records','staged_nodes','max_append_writes','max_root_bytes','walked_nodes','payload_edges']:
  changed=copy.deepcopy(events);changed[marker]['Output']=changed[marker]['Output'].replace(field+'='+str(metrics[field]),field+'=1');reject('changed_'+field,changed)
 results.append(dict(label=label,sdk=a,execution=e,verified=result))
staging=root.with_name(root.name+'-proof');meta=read(staging/'archive-verification.json');inv=read(staging/'fixture-inventory.json');assert fixture_archive.inventory(root)==inv['files']
with root.with_suffix('.tar.gz').open('rb') as f:declared,actual=fixture_archive.verify_hashed_stream(f,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert declared==inv;checks=closure(root);out.mkdir(parents=True)
for p in root.iterdir():
 if p.is_file():shutil.copy2(p,out/p.name)
for n in ['archive-verification.json','fixture-inventory.json']:shutil.copy2(staging/n,out/n)
shutil.copy2(__file__,out/'executed-review.py')
(out/'review.json').write_text(json.dumps(dict(source=rev,source_inputs=len(selected),original_unit=unit,actual_sdk_results=results,actual_positive_proof_mutations_rejected=controls,complete_archive=actual,closure=checks,scope='Immutable append-tree data structure at frozen171d9f9, normal/race100000 records and failure/fork/census controls. Selected Git inputs and actual SDK provenance; no hermetic compiler/external-cache, native ownership/publication/collector/migration/runtime throughput/onlineGC/full release qualification.'),indent=2)+'\n')
print('RETAINED_GRAPH_INDEPENDENTLY_ACCEPTED',len(selected),len(controls),flush=True)
