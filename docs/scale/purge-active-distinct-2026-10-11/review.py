import hashlib,json,re,subprocess
from pathlib import Path
out=Path(__file__).resolve().parent
record=json.loads((out/'source-inputs.json').read_text());checkout=Path(record['checkout']);sha=lambda b:hashlib.sha256(b).hexdigest()
for n,h in record['files'].items():assert sha((checkout/n).read_bytes())==h,n
assert (out/'active-helper.go.txt').read_bytes()==(checkout/'integration/purge_reuse_active_test.go').read_bytes()
assert subprocess.check_output(['git','diff',record['base_revision'],'--','integration/purge_reuse_scale_test.go','.github/workflows/canonical-purge-reuse.yml'],cwd=checkout)==(out/'fixture.patch').read_bytes()
rows=[]
for mode in ('normal','race'):
 receipt=json.loads((out/(mode+'-command.json')).read_text());assert receipt['actual_exit']==0 and receipt['source_unchanged'] and len(receipt['executions'])==1
 execution=next(iter(receipt['executions'].values()));assert execution['actual_exit']==0
 assert receipt['start_ns']<=execution['start_ns']<execution['end_ns']<=receipt['end_ns']
 assert not Path('/proc/'+str(execution['pid'])).exists()
 binary=Path(execution['retained_binary']);assert binary.stat().st_size==execution['bytes'] and sha(binary.read_bytes())==execution['sha256']
 assert ('-race=true' in subprocess.check_output(['go','version','-m',str(binary)],text=True))==(mode=='race')
 log=(out/(mode+'.log')).read_text();assert not re.search(r'^\s*--- (FAIL|SKIP):',log,re.M)
 for test in ('TestPurgeScaleActiveHandlersRequireDistinctCohort','TestPurgeScaleActiveHandlersConcurrentCohort'):assert f'--- PASS: {test} ' in log
 assert re.search(r'^PASS$',log,re.M)
 rows.append(dict(mode=mode,actual_exit=0,binary_sha256=execution['sha256']))
blocks=[];block=None
for line in (checkout/'.github/workflows/canonical-purge-reuse.yml').read_text().splitlines():
 m=re.search(r"python3 - <<'([^']+)'",line)
 if m:block=[];delimiter=m.group(1);continue
 if block is not None:
  if line.strip()==delimiter:compile('\n'.join(block),'CI Python block','exec');blocks.append(block);block=None
  else:block.append(line[10:] if line.startswith('          ') else line)
assert block is None and len(blocks)==2
result=dict(accepted=True,source_inputs=len(record['files']),executions=rows,ci_python_blocks_compiled=2,scope='distinct-cohort counter and fixture compilation only; no native active population or full scale acceptance')
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
