import hashlib,json,re,sys
from pathlib import Path
root=Path(sys.argv[1]);state=json.loads((root/'state.json').read_text())
assert state['finished'] and state['actual_exit']==0,'full campaign is not closed and successful'
assert not Path('/proc/'+str(state['driver_pid'])).exists(),'driver is still present'
assert hashlib.sha256(Path(state['binary']).read_bytes()).hexdigest()==state['binary_sha256']
for name,sha in state['source_inputs'].items():assert hashlib.sha256((Path(state['snapshot'])/name).read_bytes()).hexdigest()==sha,name
log=(root/'full.log').read_text()
assert not re.search(r'^\s*--- (FAIL|SKIP):',log,re.M)
assert 'profile canonical=true count=10000 reuse=1000 active=true full_scale=true' in log
assert 'kept 10000 other handlers executing before purge' in log
assert 'purged 10000 while 10000 remained live; reused 1000 ids' in log
assert '--- PASS: TestCanonicalGraphTenThousandConcurrentPurgesAndThousandReusedIDs ' in log
report=json.loads(re.search(r'canonical raw purge/reuse audit=(\{[^\n]+\})',log)[1])
assert all(report[k]==11000 for k in ('Invocations','Journals','Terminal'))
assert report['Retired']==report['RetiredTombstones']==9000
assert report['Entries']==66000 and report['References']['ReaderPins']==0
native=Path(state['native_root']);expected=json.loads((root/'native-census.json').read_text())
actual={str(p.relative_to(native)):dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in native.rglob('*') if p.is_file()}
assert actual==expected
result=dict(accepted_full_functional_purge=True,revision=state['revision'],report=report,native_files=len(actual),native_bytes=state['native_bytes'],fault_matrix_qualified=False,power_loss_qualified=False,physical_collection_qualified=False)
(root/"full-review.json").write_text(json.dumps(result,indent=2)+"\n")
print(json.dumps(result))
