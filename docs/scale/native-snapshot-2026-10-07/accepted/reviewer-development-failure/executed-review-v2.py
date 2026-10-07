import sys,pathlib,json,hashlib,subprocess,shutil,base64,copy,datetime,re
sys.dont_write_bytecode=True
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-native-snapshot-20261007');out=repo/'docs/scale/native-snapshot-2026-10-07/accepted/journal'
sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
from worker_leaf_wire import protocol
read=lambda p:json.loads(p.read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
before=read(root/'source-before.json');assert before==read(root/'source-after.json');revision=before['revision']
for name,digest in before['files'].items():
 p=root/'selected-source'/name
 if p.suffix=='.go':p=p.with_suffix('.go.txt')
 assert sha(p)==digest==hashlib.sha256(subprocess.check_output(['git','cat-file','blob',revision+':'+name],cwd=repo)).hexdigest()
actual=read(root/'journal-actual-sdk.json');binary=read(root/'journal-binary.json');execution=read(root/'journal-execution.json');command=read(root/'journal-commands.json')['run']
assert execution['exit_code']==0 and execution['source']==revision
assert command==[str(root/'journal-race.test'),'-test.run=Snapshot|Checkpoint','-test.v','-test.count=1','-test.timeout=3m']
assert actual['args']==command and actual['exe_sha256']==binary['sha256']==sha(root/'journal-race.test')
assert actual['admission']['stable_identity_observed_twice'] and not pathlib.Path('/proc',str(actual['pid'])).exists()
assert actual['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_NATIVE_SNAPSHOT_ROOT=str(root/'snapshot-stores'),WF_BLOB_AUTHORITY_ROOT=str(root/'blob-stores'))
assert '-race=true' in binary['build_info'] and 'vcs.revision='+revision in binary['build_info'] and 'vcs.modified=false' in binary['build_info']
assert 'github.com/nats-io/nats-server/v2\tv2.15.0' in binary['build_info']
log=(root/'journal-actual.log').read_text();assert log.rstrip().endswith('PASS') and not any(x in log for x in ['DATA RACE','--- FAIL:','--- SKIP:'])
required=['TestNativeSnapshotRetainedGraph','TestNativeCheckpointSnapshotPromiseGraph','TestSnapshotSupersededRetriesWholeRead','TestNativeSnapshotPublicationFencedWhilePaused','TestNativeSnapshotWholeReadRetriesReplacement','TestNativeSnapshotImportLegacy','TestNativeSnapshotMissingDependencyDoesNotPublish','TestNativeSnapshotMissingCanonicalBytesFailClosed','TestNativeSnapshotArchivedCheckpointPromise','TestNativeCheckpointImportRejectsWrongLiveAnchor','TestRuntimeCheckpointBindsActualAnchorAndSDKCounter','TestRuntimeSnapshotPurgeBoundAndLimit','TestCheckpointReadAbsentOrdinaryAndMalformedMetadata','TestCheckpointReadInvalidIdentityAndCancellationBeforeTransport','TestSnapshotObjectDeadlinePreservesObservedCause']
for name in required:assert re.search(r'^--- PASS: '+name+r' \(',log,re.M),name
manifest=read(root.with_name(root.name+'-proof')/'fixture-inventory.json');meta=read(root.with_name(root.name+'-proof')/'archive-verification.json')
assert fixture_archive.inventory(root)==manifest['files']
with root.with_suffix('.tar.gz').open('rb') as f:declared,digest=fixture_archive.verify_hashed_stream(f,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert declared==manifest

def graph(r):
 assert r['Head']>0 and r['Token']
 d=json.loads(base64.b64decode(r['Data'],validate=True))
 assert set(d)=={'schema','key','manifest','objects'} and d['schema']=='js-wf-native-snapshot-v1' and d['key']=='snap.test.atomic'
 m=d['manifest'];assert m['version'] in (1,2) and m['last_seq']>0 and m['epoch']>0 and m['last_index']>=0
 assert m['object'].startswith('snapshot-'+hashlib.sha256(b'test.atomic').hexdigest()[:16]+'-') and d['objects'][m['object']]==m['sha256']
 assert set(d['objects'].values())==set(r['Blobs'])
 for name,h in d['objects'].items():
  assert name and re.fullmatch('[0-9a-f]{64}',h)
  ref=r['Blobs'][h];parts=ref['Object'].split('/')
  assert len(parts)==3 and parts[0]==h and str(ref['Generation'])==parts[1] and ref['Generation']>0 and re.fullmatch('[A-Za-z0-9-]+',parts[2])
 runtime=m.get('runtime')
 if m['version']==2:
  assert runtime and runtime['object']=='step-result-'+runtime['sha256'] and runtime['index']>m['last_index'] and runtime['sequence']>m['last_seq']
  assert d['objects'][runtime['object']]==runtime['sha256']
 return d

def wire(d,identity):
 held=d['held'];end=d['final_packet'];trace=d['wire']
 assert held['subject']=='wf.snapshot.authority.root.'+hashlib.sha256(b'snapshot.snap.test.atomic').hexdigest()
 packet=base64.b64decode(held['packet'],validate=True)
 assert held['disposition']=='held' and held['forwarded_bytes']==0
 assert end['subject']==held['subject'] and end['packet']==held['packet'] and end['disposition']=='forwarded' and end['forwarded_bytes']==len(packet)
 assert trace['truncated'] is False and len(trace['connections'])==1
 streams={direction:bytearray() for direction in ['client_to_server','server_to_client']}
 for frame in trace['frames']:
  assert frame['connection']==1
  streams[frame['direction']].extend(base64.b64decode(frame['data'],validate=True))
 assert bytes(streams['client_to_server']).count(packet)==1
 parsed=protocol(packet);assert len(parsed)==1
 op,parts,body=parsed[0];assert op==b'HPUB' and parts[1].decode()==held['subject']
 header=body[:int(parts[-2])];assert re.search(rb'Nats-Expected-Last-Subject-Sequence: [1-9][0-9]*\r\n',header)
 payload=json.loads(body[int(parts[-2]):]);assert payload['schema']=='js-wf-blob-authority-v1' and payload['identity']=='snapshot.snap.test.atomic' and payload['kind']=='root'
 assert payload['root']['Head']==payload['revision']==d['old_root']['Head']+1 and payload['root']['Token']!=d['old_root']['Token'];graph(payload['root'])
 incoming=protocol(streams['server_to_client'],True);infos=[v for op,_,v in incoming if op==b'INFO'];assert infos
 for info in infos:
  peers=[p for p in identity['peers'] if p['id']==info['server_id']]
  assert len(peers)==1 and peers[0]['name']==info['server_name'] and peers[0]['version']==info['version']=='2.15.0' and peers[0]['embedding_commit']==info['git_commit']==revision[:7] and info['jetstream']
 replies=[]
 for op,parts,body in incoming:
  if op not in [b'MSG',b'HMSG'] or parts[1]!=parsed[0][1][2]:continue
  if op==b'HMSG':body=body[int(parts[-2]):]
  replies.append(json.loads(body))
 assert len(replies)==1 and replies[0]['error']['err_code']==10071
 return dict(server_info=infos[0],packet_bytes=len(packet),original_wire_CAS_rejected=10071)

specs={
 'retained-graph':(['result_and_signal_retained','stale_publication_rejected','superseded_archive_reclaimed','retired_graph_reclaimed'],5,0),
 'checkpoint-promise-graph':(['frame_and_promise_imported','legacy_bytes_removed','all_imported_objects_reclaimed'],4,0),
 'paused-publication':(['old_graph_preserved','late_publication_rejected','all_native_objects_reclaimed'],None,0),
 'whole-read-replacement':(['manifest_reloaded_after_old_archive_collection'],4,1),
 'legacy-import':(['legacy_objects_deleted','existing_canonical_root_not_replaced'],4,2),
 'missing-dependency':(['candidate_not_uploaded'],4,1),
 'missing-canonical-object':(['legacy_copy_present','missing_current_object_failed_closed'],None,0),
 'archived-checkpoint-promise':(['archive_frame_promise_pinned'],5,3),
 'checkpoint-import-anchor':(['wrong_live_anchor_rejected','verified_import_fast_resume'],None,2),
}
def check(d,identity):
 scenario=d['scenario'];replicas=d['replicas'];flags,count,objects=specs[scenario]
 assert replicas in (1,3) and identity['replicas']==replicas and identity['parent_budget_seconds']==30
 assert len(identity['peers'])==len(set(x['id'] for x in identity['peers']))==replicas
 assert {p['name'] for p in identity['peers']}=={f'wf-test-{i}' for i in range(replicas)}
 assert all(p['version']=='2.15.0' and p['embedding_commit']==revision[:7] for p in identity['peers'])
 for flag in flags:assert d[flag] is True,flag
 if count is not None:assert d.get('all_records_replayed',d.get('records_replayed',d.get('full_replay_records')))==count
 boundary=d['native_boundary'];assert len(boundary['objects'])==objects
 for name in ['SNAP_AUTH','OBJ_SNAP_BLOB']:
  c=boundary['streams'][name]['config'];state=boundary['streams'][name]['state']
  assert c['name']==name and c['num_replicas']==replicas and c['storage']=='file' and c['retention']=='limits' and c['discard']=='old' and c['deny_delete'] and not c['allow_direct']
  assert c['max_age']==0 and c['max_msgs']<=0 and c['max_bytes']<=0 and c['max_msg_size']<=0
  assert state['consumer_count']==0 and state['messages']==sum(state['subjects'].values()) and state['num_subjects']==len(state['subjects'])
  if name=='SNAP_AUTH':assert c['max_msgs_per_subject']==1 and c['deny_purge'] and not c.get('allow_rollup_hdrs',False)
  else:assert c['max_msgs_per_subject']<=0 and not c.get('deny_purge',False) and c['allow_rollup_hdrs'] and c['metadata']['js-wf-blob-format']=='recoverable-v1'
 live=boundary['root'];subjects=boundary['streams']['OBJ_SNAP_BLOB']['state']['subjects']
 if live['Token']:
  graph(live)
  if scenario!='missing-canonical-object':
   byhash={x['Key']:x['Reference'] for x in boundary['objects']};assert byhash==live['Blobs']
   for h,ref in live['Blobs'].items():
    fence=boundary['fences'][h]['Fence'];assert fence['Phase']=='ready' and fence['Generation']==ref['Generation'] and fence['Object']==ref['Object']
    suffix=base64.urlsafe_b64encode(ref['Object'].encode()).decode()
    assert subjects['$O.SNAP_BLOB.M.'+suffix]==1 and subjects['$O.SNAP_BLOB.C.'+suffix]>0
 else:
  assert live['Head']>0 and not live['Blobs'] and not live['Data'] and not boundary['objects']
  assert all(x['Fence']['Phase']=='closed' and x['Fence']['Generation']>0 for x in boundary['fences'].values())
  assert all(s.startswith('$O.SNAP_BLOB.M.') and n==1 for s,n in subjects.items())
 if scenario in ['legacy-import','archived-checkpoint-promise','checkpoint-import-anchor']:assert d['root']==live
 if scenario=='whole-read-replacement':assert d['current_root']==live and graph(live)['manifest']['last_seq']>d['old_snapshot']['last_seq']
 result=dict(scenario=scenario,replicas=replicas,graph_objects=objects)
 if scenario=='retained-graph':
  a=graph(d['first_root']);b=graph(d['newer_root']);assert len(a['objects'])==len(b['objects'])==3 and a['manifest']==d['first_snapshot'] and b['manifest']==d['newer_snapshot'] and live['Head']>d['newer_root']['Head']
  for name,data in [('step-result-',b'retained step result'),('signal-',b'retained signal payload')]:
   h=hashlib.sha256(data).hexdigest();assert a['objects'][name+h]==b['objects'][name+h]==h
  assert d['newer_root']['Head']>d['first_root']['Head'] and b['manifest']['last_index']>a['manifest']['last_index']
 elif scenario=='checkpoint-promise-graph':
  a=graph(d['root']);assert a['manifest']==d['snapshot'] and live['Head']>d['root']['Head'] and len(a['objects'])==3 and a['manifest']['version']==2 and a['manifest']['runtime']['sha256']==d['frame_sha256'] and d['fast_resume_records']==1
  h=hashlib.sha256(b'promise outcome retained only in frame').hexdigest();assert a['objects']['terminal-result-'+h]==h
 elif scenario=='paused-publication':
  a=d['old_root'];b=d['fenced_root'];graph(a);graph(b)
  assert b['Head']>a['Head'] and all(a[k]==b[k] for k in ['Token','Blobs','Data'])
  result['wire']=wire(d,identity)
 elif scenario=='missing-dependency':assert d['before_root']==d['after_root']==live
 elif scenario=='missing-canonical-object':assert d['root']==live and len(live['Blobs'])==1 and not any(s.startswith('$O.SNAP_BLOB.C.') for s in subjects)
 elif scenario=='archived-checkpoint-promise':
  a=graph(d['root']);assert a['manifest']['version']==1 and len(a['objects'])==3
  h=hashlib.sha256(b'promise retained by archived checkpoint').hexdigest();assert a['objects']['terminal-result-'+h]==h
 elif scenario=='checkpoint-import-anchor':assert graph(d['root'])['manifest']['version']==2
 return result

paths=list((root/'snapshot-stores').rglob('snapshot-proof.json'));assert len(paths)==13
proofs={};controls=[]
for p in paths:
 d=read(p);identity=read(p.parent/'snapshot-fixture-identity.json');label=p.parent.name
 proofs[label]=dict(check(d,identity),identity=identity)
 changes=[('unsafe_direct',lambda x:x['native_boundary']['streams']['SNAP_AUTH']['config'].update(allow_direct=True)),('authority_purge_allowed',lambda x:x['native_boundary']['streams']['SNAP_AUTH']['config'].update(deny_purge=False)),('object_TTL',lambda x:x['native_boundary']['streams']['OBJ_SNAP_BLOB']['config'].update(max_age=1)),('consumer_remains',lambda x:x['native_boundary']['streams']['OBJ_SNAP_BLOB']['state'].update(consumer_count=1)),('subject_census_wrong',lambda x:x['native_boundary']['streams']['OBJ_SNAP_BLOB']['state'].update(messages=999))]
 for flag in specs[d['scenario']][0]:changes.append((flag+'_false',lambda x,flag=flag:x.update({flag:False})))
 if d['native_boundary']['root']['Token']:
  changes.append(('root_hash_graph_removed',lambda x:x['native_boundary']['root'].update(Blobs={})))
 if 'held' in d:
  changes.extend([('truncated_wire',lambda x:x['wire'].update(truncated=True)),('held_packet_forwarded',lambda x:x['held'].update(forwarded_bytes=1)),('root_fence_reset',lambda x:x['fenced_root'].update(Head=0))])
 for name,change in changes:
  bad=copy.deepcopy(d);change(bad)
  try:check(bad,identity)
  except (AssertionError,ValueError,KeyError):controls.append(label+'/'+name)
  else:raise AssertionError('actual-positive substitution accepted '+label+'/'+name)
 bad=copy.deepcopy(identity);bad['peers'][0]['embedding_commit']='foreign'
 try:check(d,bad)
 except AssertionError:controls.append(label+'/foreign-embedding-commit')
 else:raise AssertionError('foreign embedding commit accepted')
 if 'held' in d:
  bad=copy.deepcopy(identity);bad['peers'][0]['id']='foreign'
  try:check(d,bad)
  except AssertionError:controls.append(label+'/foreign-wire-callee')
  else:raise AssertionError('foreign wire callee accepted')
expected={(s,r) for s in specs for r in ([1,3] if s in ['retained-graph','checkpoint-promise-graph','paused-publication','legacy-import'] else [1])}
assert {(x['scenario'],x['replicas']) for x in proofs.values()}==expected
out.mkdir(parents=True,exist_ok=False)
for p in paths:
 dest=out/'native-proofs'/p.parent.name;dest.mkdir(parents=True)
 shutil.copy2(p,dest/p.name);shutil.copy2(p.parent/'snapshot-fixture-identity.json',dest/'snapshot-fixture-identity.json')
report=dict(source=revision,source_inputs_verified=len(before['files']),actual_sdk=actual,binary=binary,execution=execution,proofs=proofs,actual_positive_substitutions_rejected=controls,complete_archive_verified=digest,required_tests=required,reviewed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Selected source, actual race SDK/profile, thirteen native snapshot boundary graphs/configurations and two exact held CAS packet/reply/peer transcripts, full archived media and every file. Embedded module version/application revision bound; external Go module files not separately inventoried. Migration uses quiesced legacy readers/writers. Other runtime destinations, privileged lifecycle, arbitrary partition, large graph/message size, final-source full124/matrices/24h/million drain/release and production online GC remain open.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copy2(__file__,out/'executed-review.py')
for name in ['journal-actual-sdk.json','journal-binary.json','journal-commands.json','journal-execution.json','journal-actual.log']:shutil.copy2(root/name,out/name)
print(json.dumps(dict(accepted=True,native_scenarios=len(proofs),actual_positive_mutations=len(controls),elapsed=execution['elapsed_seconds'])),flush=True)
