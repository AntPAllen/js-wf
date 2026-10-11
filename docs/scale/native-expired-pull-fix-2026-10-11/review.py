"""Verify terminal diagnostic scope, frozen inputs, actual SDK and closed store."""
import hashlib,io,json,re,subprocess
from pathlib import Path
out=Path(__file__).resolve().parent;repo=out.parents[2]
sha=lambda data:hashlib.sha256(data).hexdigest()
source=json.loads((out/'source.json').read_text());inputs=json.loads((out/'source-inputs.json').read_text())
for name,digest in inputs.items():assert sha((Path(source['source'])/name).read_bytes())==digest,name
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(source['revision']+':'+n+'\n' for n in inputs).encode()))
for name,digest in inputs.items():
 header=stream.readline().split();assert header[1]==b'blob'
 data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert sha(data)==digest,name
commands=json.loads((out/'commands.json').read_text())
assert len(commands)==2,'terminal SDK receipt missing; acceptance refused'
build,test=commands
assert build['actual_exit']==0 and build['source_unchanged'] and test['source_unchanged']
assert test['actual_exit'] in (0,1)
assert not Path('/proc',str(test['pid'])).exists(),'SDK still live'
binary=Path(test['argv'][0])
assert binary.stat().st_size==test['binary_bytes'] and sha(binary.read_bytes())==test['binary_sha256']
assert '-race=true' in subprocess.check_output(['go','version','-m',str(binary)],text=True)
assert test['environment']['WF_PURGE_REUSE_COUNT']=='10' and test['environment']['WF_PURGE_REUSE_ACTIVE']=='1'
assert test['environment']['WF_GRAPH_PURGE_REUSE_SCALE']=='1'
log=(out/'test.log').read_text();name='TestCanonicalGraphTenThousandConcurrentPurgesAndThousandReusedIDs'
assert 'count=10 reuse=1 active=true full_scale=false' in log
assert not re.search(r'^\s*--- SKIP:',log,re.M)
report=None
if test['actual_exit']==0:
 assert f'--- PASS: {name} ' in log and not re.search(r'^\s*--- FAIL:',log,re.M)
 assert 'kept 10 other handlers executing before purge' in log
 assert 'purged 10 while 10 remained live; reused 1 ids' in log
 assert 'reused and released all invocations' in log
 report=json.loads(re.search(r'canonical raw purge/reuse audit=(\{[^\n]+\})',log)[1])
 assert all(report[k]==11 for k in ['Invocations','Journals','Terminal'])
 assert report['Retired']==report['RetiredTombstones']==9 and report['References']['ReaderPins']==0
else:assert f'--- FAIL: {name} ' in log
native=Path(test['environment']['WF_GRAPH_PURGE_REUSE_NATIVE_ROOT'])
files={str(p.relative_to(native)):{'bytes':p.stat().st_size,'sha256':sha(p.read_bytes())} for p in sorted(native.rglob('*')) if p.is_file()}
assert files==json.loads((out/'native-census.json').read_text())
result={'closure_verified':True,'actual_exit':test['actual_exit'],'source':source['revision'],'source_inputs':len(inputs),'actual_sdk_sha256':test['binary_sha256'],'sdk_seconds':(test['end_ns']-test['start_ns'])/1e9,'native_files':len(files),'native_bytes':sum(v['bytes'] for v in files.values()),'raw_report':report,'active_population_complete':'kept 10 other handlers executing before purge' in log,'purge_reuse_overlap':'purged 10 while 10 remained live; reused 1 ids' in log,'full_scale_accepted':False,'original_failure_cause_proven':False}
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
