from pathlib import Path
import hashlib,json,re,gzip
here=Path(__file__).resolve().parent
root=here.parents[2]
for name,digest in json.loads((here/'sources.json').read_text()).items():
    assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,name
    assert hashlib.sha256(gzip.decompress((here/(name.replace('/','_')+'.txt.gz')).read_bytes())).hexdigest()==digest,name
log=(here/'restored-worker.log').read_text()
assert re.search(r'^ok\s+js-wf/worker\s+',log,re.M)
for test,count in [('TestGraphContinuationStoredMaintenanceAcrossDeliveries',40),('TestGraphDeliveryCompactionReleaseUncertaintyIsSticky',3),('TestGraphContinuationMaintenanceLeaseAndCancellation',32),('TestGraphContinuationRepairsArchiveBeforeStage',7),('TestNativeGraphStoredMaintenanceFreshWorkerRecovery',2)]:
    assert len(re.findall(r'^    --- PASS: '+test+'/',log,re.M))==count,test
for replicas in [1,3]:
    assert f'NATIVE_STORED_WORKER replicas={replicas} fresh_workers=3 stage_batches=[1 5 0] verify_batches=[0 10 0]' in log
assert log.count('STORED_WORKER mode=cut-stage deliveries=3 stage_batches=[1 5 0 0 0]')==2
for mode in ['cut-verify','cut-nodes']:
    assert len(re.findall('STORED_WORKER mode='+mode+r'.*verify_batches=\[[16] 10 0 0 0\]',log))==2
assert log.count('COMPACTION_RELEASE mode=drop attempts=1 hidden_reads=0 retained_readers=1')==1
assert log.count('COMPACTION_RELEASE mode=lost attempts=1 hidden_reads=0 retained_readers=0')==1
mutations=json.loads((here/'mutations.json').read_text())
counts=[2,2,2,8,2,2,2]
assert len(mutations)==len(counts)
for item,count in zip(mutations,counts):
    assert item['exit_code']!=0 and item['failures']==count,item
    failure=(here/(item['name']+'.log')).read_text()
    assert len(re.findall(r'^    --- FAIL:',failure,re.M))==count,item
pins=(here/'pinned.log').read_text()
assert re.search(r'^ok\s+js-wf/sim\s+',pins,re.M)
assert len(re.findall(r'^    --- PASS: TestPinnedRegressionCorpus/',pins,re.M))==853
print(json.dumps(dict(stored_delivery_controls=40,release_controls=3,existing_maintenance_controls=32,archive_repair_controls=7,native_domains=['R1','R3'],bypass_failures=counts,common_pins=853,whole_handoff_seconds=15,actual100000_accepted=False),indent=2))
