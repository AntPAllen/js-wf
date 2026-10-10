from pathlib import Path
import hashlib,json,re
here=Path(__file__).resolve().parent
root=here.parents[2]
for name,digest in json.loads((here/'sources.json').read_text()).items():
    assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest, name
worker=(here/'restored-worker.log').read_text()
assert re.search(r'^ok\s+js-wf/worker\s+',worker,re.M)
assert len(re.findall(r'^    --- PASS: TestGraphContinuationMaintenanceLeaseAndCancellation/',worker,re.M))==32
assert len(re.findall(r'^    --- PASS: TestGraphContinuationRepairsArchiveBeforeStage/',worker,re.M))==7
assert len(re.findall(r'WORKER_MAINTENANCE mode=renew-.*renew_batches=[1-9]',worker))==6
assert len(re.findall(r'WORKER_MAINTENANCE mode=(?:owner-loss|cancel)-renew .*retained_from=0',worker))==4
mutant=(here/'scheduling-bypass.log').read_text()
assert len(re.findall(r'^    --- FAIL: TestGraphContinuationMaintenanceLeaseAndCancellation/',mutant,re.M))==10
pins=(here/'pinned-restored.log').read_text()
assert re.search(r'^ok\s+js-wf/sim\s+',pins,re.M)
assert len(re.findall(r'^    --- PASS: TestPinnedRegressionCorpus/',pins,re.M))==853
print(json.dumps(dict(worker_controls=32,archive_repair_controls=7,scheduling_bypass_failures=10,common_pins=853,accepted_scope='worker renewal scheduling only'),indent=2))
