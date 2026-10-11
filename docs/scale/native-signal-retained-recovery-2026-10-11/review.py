"""Review immutable originals, both copied probes and finite-dedup component controls."""
import base64,gzip,hashlib,io,json,re,subprocess
from pathlib import Path
out=Path(__file__).resolve().parent;repo=out.parents[2]
sha=lambda b:hashlib.sha256(b).hexdigest()
def census(p):return {str(x.relative_to(p)):dict(bytes=x.stat().st_size,sha256=sha(x.read_bytes())) for x in p.rglob('*') if x.is_file()}
marker=json.loads((out/'copy-verified.json').read_text())
expected=json.loads((out/'original-census.json').read_text())
assert census(Path(marker['original']))==expected
assert len(expected)==2158 and sum(v['bytes'] for v in expected.values())==14345960
rows=[]
for directory,wanted in ((out,1),(out/'expired-dedup',0)):
 inputs=json.loads((directory/'source-inputs.json').read_text());source=Path(inputs['source'])
 stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(inputs['revision']+':'+n+'\n' for n in inputs['files']).encode()))
 for n,h in inputs['files'].items():
  header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
  assert sha(data)==h==sha((source/n).read_bytes()),n
 assert sha((source/inputs['helper']['path']).read_bytes())==inputs['helper']['sha256']==sha((directory/'probe.go.txt').read_bytes())
 commands=json.loads((directory/'commands.json').read_text());assert len(commands)==2
 assert commands[0]['label']=='build' and commands[0]['actual_exit']==0
 run=commands[1];assert run['label']=='probe' and run['actual_exit']==wanted
 for cmd in commands:
  assert cmd['start_ns']<cmd['end_ns'] and not Path('/proc/'+str(cmd['pid'])).exists()
 binary=Path(run['argv'][0]);assert sha(binary.read_bytes())==run['binary_sha256'] and binary.stat().st_size==run['binary_bytes']
 copy_marker=json.loads((directory/'copy-verified.json').read_text());assert copy_marker['verified'] and copy_marker['original']==marker['original']
 assert census(Path(copy_marker['copy']))==json.loads((directory/'closed-copy-census.json').read_text())
 observation=json.loads((directory/'observations.json').read_text());steps={r['Phase']:r for r in observation['Observations']}
 assert len(steps)==len(observation['Observations'])
 for n,v in steps.items():
  if not n.startswith('metadata.') and n!='StreamCounts.after':assert not v['Error'],n
 inp=steps['ReadSignalInput.before']['Value']['input']
 assert inp['request']==dict(type='purge-scale',id='live-00004',invocation=11,name='release',key='release-4')
 assert steps['ReadSignalInput.before']['Value']['body']=='4' and inp['input_sha256']==sha(b'4') and inp['input_size']==1
 assert steps['ReadSignalBinding.before']['Value']['found'] is False
 source_msg=steps['SignalSource.before']['Value'];assert source_msg['Sequence']==6
 assert source_msg['Header']['Wf-Graph-Signal-Token']==[inp['token']]
 assert source_msg['Header']['Wf-Inv-Seq']==['11']
 pointer=json.loads(base64.b64decode(source_msg['Data']));assert pointer['token']==inp['token'] and pointer['index']==inp['index']
 assert steps['RecoverSignal.originalToken']['Value']==6
 after=steps['ReadSignalBinding.after']['Value'];assert after['found'] and after['body']=='4'
 assert after['binding']['input']==inp and after['binding']['sequence']==6 and after['binding']['index']==0
 assert steps['ReadSignalInput.after']['Value']['input']==inp
 assert steps['SignalSource.after']['Value']==source_msg
 if wanted:
  assert observation['Error']=='StreamCounts.after: recovery added signal source'
  assert steps['StreamCounts.after']['Error']=='recovery added signal source'
 else:
  assert not observation['Error'] and not steps['StreamCounts.after']['Error']
  before=steps['StreamCounts.before']['Value'];final=steps['StreamCounts.after']['Value']
  assert before['source_age_ns']>before['dedup_window_ns']==120000000000
  for kind in ('signal','run'):
   assert final[kind]['messages']==before[kind]['messages']+1 and final[kind]['last_seq']==before[kind]['last_seq']+1
  duplicate=steps['ExpiredDuplicate.exactToken']['Value']
  assert duplicate['Sequence']==before['signal']['last_seq']+1 and duplicate['Data']==source_msg['Data'] and duplicate['Header']==source_msg['Header']
  assert steps['BindNextSignal.expiredDuplicate']['Value'] is True
  assert steps['ReadSignalBinding.afterDuplicate']['Value']==after
  assert steps['BindNextSignal.exhausted']['Value'] is False
 rows.append(dict(actual_exit=wanted,source_inputs=len(inputs['files']),binary_sha256=run['binary_sha256'],seconds=(run['end_ns']-run['start_ns'])/1e9))
model_inputs=json.loads((out/'model-source-inputs.json').read_text())
for n,h in model_inputs.items():assert sha((repo/n).read_bytes())==h,n
for mode in ('normal','race'):
 receipt=json.loads((out/('model-'+mode+'-command.json')).read_text());assert receipt['actual_exit']==0 and receipt['source_unchanged'] and len(receipt['executions'])==1
 execution=next(iter(receipt['executions'].values()));assert execution['actual_exit']==0 and not Path('/proc/'+str(execution['pid'])).exists()
 binary=Path(execution['retained_binary']);assert sha(binary.read_bytes())==execution['sha256'] and binary.stat().st_size==execution['bytes']
 assert receipt['start_ns']<=execution['start_ns']<execution['end_ns']<=receipt['end_ns']
 log=(out/('model-'+mode+'.log')).read_text();assert not re.search(r'^\s*--- (FAIL|SKIP):',log,re.M)
 for cut in ('binding_commit','binding_commit_unknown'):
  for window in ('within','boundary'):assert f'--- PASS: TestCanonicalSignalRecoveryAfterDedupExpiry/{cut}/{window} ' in log
 for cut in ('reserve_prepare','reserve_commit','reserve_commit_lost','reserve_commit_unknown','binding_prepare','binding_commit','binding_commit_lost','binding_commit_unknown'):
  assert f'--- PASS: TestCanonicalSignalMutationErrorPhases/{cut} ' in log
rows_count=0;block=None
for line in (repo/'.github/workflows/graph-publication.yml').read_text().splitlines():
 m=re.search(r"python3 - <<'([^']+)'",line)
 if m:block=[];delimiter=m.group(1);continue
 if block is not None:
  if line.strip()==delimiter:compile('\n'.join(block),'CI Python block','exec');rows_count+=1;block=None
  else:block.append(line[10:] if line.startswith('          ') else line)
assert block is None
result=dict(accepted=True,original_files_unchanged=len(expected),probes=rows,original_binding_sequence=6,original_token_preserved=True,expired_window_duplicate_preserves_first_binding=True,model_cuts_per_mode=4,model_source_inputs=len(model_inputs),ci_python_blocks_compiled=rows_count,native_timeout_cause_established=False,full_scale_accepted=False)
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
