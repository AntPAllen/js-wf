from pathlib import Path
import hashlib,json,re,gzip
here=Path(__file__).resolve().parent
root=here.parents[2]
for name,digest in json.loads((here/'sources.json').read_text()).items():
    assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,name
    assert hashlib.sha256(gzip.decompress((here/(name.replace('/','_')+'.txt.gz')).read_bytes())).hexdigest()==digest,name

def passes(log,test,count):
    assert len(re.findall(r'^    --- PASS: '+test+'/',log,re.M))==count,test
log=(here/'restored-worker.log').read_text()
assert re.search(r'^ok\s+js-wf/worker\s+',log,re.M)
for name,count in [('TestContinuationPublicationContextOwnershipAndBounds',4),('TestGraphContinuationStoredMaintenanceAcrossDeliveries',42),('TestGraphDeliveryCompactionReleaseUncertaintyIsSticky',3),('TestGraphContinuationMaintenanceLeaseAndCancellation',32),('TestGraphContinuationRepairsArchiveBeforeStage',7),('TestNativeGraphStoredMaintenanceFreshWorkerRecovery',2)]:passes(log,name,count)
for mode in ['slow-handoff','slow-recovery']:
    durations=re.findall(r'^    --- PASS: TestGraphContinuationStoredMaintenanceAcrossDeliveries/json/'+mode+r' \(([0-9.]+)s\)',log,re.M)
    assert len(durations)==1 and float(durations[0])>=18,mode
mutations=json.loads((here/'mutations.json').read_text());assert len(mutations)==3
for item,count in zip(mutations,[2,1,2]):
    assert item['exit_code']!=0 and item['failures']==count,item
    assert len(re.findall(r'^    --- FAIL:',(here/(item['name']+'.log')).read_text(),re.M))==count,item
bounds=(here/'restored-request-bounds.log').read_text()
assert re.search(r'^ok\s+js-wf/worker\s+',bounds,re.M)
passes(bounds,'TestGraphContinuationStoredMaintenanceAcrossDeliveries',42)
passes(bounds,'TestContinuationPublicationContextOwnershipAndBounds',4)
limit=(here/'native-limit64.log').read_text()
assert re.search(r'^ok\s+js-wf/worker\s+',limit,re.M)
passes(limit,'TestNativeGraphContinuationGlobalLimitAndTerminalSlot',2)
assert limit.count('GRAPH_LIMIT_DURABLE storage=file whole_handoff=delivery_context request_bounds_unchanged=true')==2
pins=(here/'pinned.log').read_text()
assert re.search(r'^ok\s+js-wf/sim\s+',pins,re.M)
passes(pins,'TestPinnedRegressionCorpus',853)
print(json.dumps(dict(context_policy_controls=4,stored_delivery_controls=42,native_domains=['R1','R3'],native_slot_budget=64,common_pins=853,bypass_failures=[2,1,2],actual100000_accepted=False,renewal_namespace_scaling_qualified=False),indent=2))
