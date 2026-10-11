import hashlib,json,re,subprocess,textwrap
from pathlib import Path
out=Path(__file__).resolve().parent
inputs=json.loads((out/'source-inputs.json').read_text())
for name,sha in inputs.items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
executions={};counts={};native_bytes={}
for name in ('normal10-final','race10-final','legacy10-final','raw-normal-final','raw-race-final'):
 r=json.loads((out/(name+'-command.json')).read_text())
 assert r['actual_exit']==0 and r['source_inputs_unchanged'] and r['end_ns']>r['start_ns']
 expected_binary='integrity.test' if name.startswith('raw-') else 'integration.test'
 own=[e for e in r['executions'].values() if r['start_ns']<=e['start_ns']<r['end_ns'] and Path(e['argv'][0]).name==expected_binary]
 assert len(own)==1
 e=own[0];assert e['actual_exit']==0
 b=Path(e['retained_binary']).read_bytes();assert len(b)==e['bytes'] and hashlib.sha256(b).hexdigest()==e['sha256']
 assert ('-race=true' in subprocess.check_output(['go','version','-m',e['retained_binary']],text=True))==('race' in name)
 executions[e['pid']]=e
 log=(out/(name+'.log')).read_text()
 assert not re.search(r'^\s*--- (FAIL|SKIP):',log,re.M),name
 counts[name]=len(re.findall(r'^\s*--- PASS:',log,re.M))
 if name.startswith('raw-'):
  assert '--- PASS: TestRawGraphRetiredPurgeTombstones ' in log
  assert len(re.findall(r'^\s*--- PASS: TestRawGraphRetiredPurgeTombstones/',log,re.M))==48
 elif name!='legacy10-final':
  assert 'profile canonical=true count=10 reuse=1 active=false full_scale=false' in log
  assert 'purged 10 while 10 remained live; reused 1 ids' in log
  report=json.loads(re.search(r'canonical raw purge/reuse audit=(\{[^\n]+\})',log)[1])
  assert all(report[k]==11 for k in ('Invocations','Journals','Terminal'))
  assert report['Retired']==report['RetiredTombstones']==9
  assert report['References']['ReaderPins']==0 and report['Entries']==66
  census=json.loads((out/(name+'-census.json')).read_text());root=Path(r['native_root'])
  actual={str(p.relative_to(root)):dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in root.rglob('*') if p.is_file()}
  assert actual==census;native_bytes[name]=sum(v['bytes'] for v in census.values())
 else:
  assert 'profile canonical=false count=10 reuse=1 active=false full_scale=false' in log
  assert 'purged 10 while 10 remained live; reused 1 ids' in log
for name in ('normal10','normal10-fixed','legacy10-audited'):
 assert json.loads((out/(name+'-command.json')).read_text())['actual_exit']==1
assert json.loads((out/'initial-polling-stop.json').read_text())['signal']=='SIGTERM'
assert 'invalid retired projection' in (out/'normal10-fixed.log').read_text()
assert 'reused_during_purge=0' in (out/'legacy10-audited.log').read_text()
assert len(re.findall(r'^\s*--- FAIL: TestRawGraphProjectionCensus/.*/retired-survivor ',(out/'raw-race.log').read_text(),re.M))==4
workflow=Path('.github/workflows/canonical-purge-reuse.yml').read_text()
block=re.search(r"^[ ]*python3 - <<'PY'\n(.*?)^[ ]*PY$",workflow,re.M|re.S)[1]
code=textwrap.dedent(block);compile(code,'<canonical-ci>','exec')
for name in ('normal10-final','race10-final'):
 check=code.replace("pathlib.Path('canonical-purge-reuse.log')",'pathlib.Path('+repr(str(out/(name+'.log')))+')')
 import os
 assert subprocess.run(['python3','-c',check],env=dict(os.environ,PROFILE='diagnostic',ACTIVE='0'),check=True).returncode==0
result=dict(accepted_diagnostic=True,full_10000_qualified=False,active_handlers_qualified=False,go_yaml_module_inputs=len(inputs),captured_final_executions=len(executions),named_passes=counts,native_bytes=native_bytes,tombstone_controls=48,initial_failures_preserved=True,ci_reducers_verified=True,production_runtime_changed=False,independent_auditor_changed=True,native_server_cause_confirmed=False)
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
