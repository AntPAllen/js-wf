from pathlib import Path
import hashlib,json,re,gzip
here=Path(__file__).resolve().parent
root=here.parents[2]
for name,digest in json.loads((here/'sources.json').read_text()).items():
    assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,name
    assert hashlib.sha256(gzip.decompress((here/(name.replace('/','_')+'.txt.gz')).read_bytes())).hexdigest()==digest,name

def read(name):
    p=here/name
    return p.read_text() if p.exists() else gzip.decompress((here/(name+'.gz')).read_bytes()).decode()

def passes(log,test,count):
    assert len(re.findall(r'^    --- PASS: '+test+'/',log,re.M))==count,test
journal=read('restored-journal.log')
assert re.search(r'^ok\s+js-wf/journal\s+',journal,re.M)
passes(journal,'TestGraphCompactionStoredCheckpointCASAndRecovery',36)
passes(journal,'TestGraphCompactionCheckpointBindingRenewalAndResumption',40)
passes(journal,'TestGraphCompactionCheckpointRejectsNoncanonicalEnvelope',11)
passes(journal,'TestNativeGraphCheckpointArchiveReopenAndCollection',2)
assert journal.count('NATIVE_STORED_COMPACTION revision=1 all_peer_restart=true fresh_kv_adapter=true deletion_confirmed=true')==2
storage=read('restored-storage.log')
assert re.search(r'^ok\s+js-wf/journal\s+',storage,re.M)
passes(storage,'TestGraphCompactionStoredCheckpointCASAndRecovery',36)
for name,count in [('refresh-update',4),('refresh-delete',2),('skip-expiry',2),('retry-unknown',8)]:
    log=read(name+'.log')
    assert len(re.findall(r'^    --- FAIL: TestGraphCompactionStoredCheckpointCASAndRecovery/',log,re.M))==count,name
pins=read('pinned.log')
assert re.search(r'^ok\s+js-wf/sim\s+',pins,re.M)
passes(pins,'TestPinnedRegressionCorpus',853)
failed=read('development-native-metadata-timeout.log')
assert '--- FAIL: TestNativeGraphCheckpointArchiveReopenAndCollection/R3-domain' in failed
assert 'context deadline exceeded' in failed
print(json.dumps(dict(storage_controls=36,binding_controls=40,malformed_controls=11,native_domains=['R1','R3'],bypass_failures=[4,2,2,8],common_pins=853,scope='journal descriptor storage; worker adoption remains unfinished'),indent=2))
