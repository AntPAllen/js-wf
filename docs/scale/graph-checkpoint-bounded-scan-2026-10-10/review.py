"""Verify retained directed development evidence; broader runs are scoped separately."""
import gzip
import hashlib
import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent
repo = base.parents[2]
inputs = json.loads((base / 'source-inputs.json').read_text())
assert {name:hashlib.sha256((repo/name).read_bytes()).hexdigest() for name in inputs} == inputs
def read(name):
    if name.endswith('.gz'):
        with gzip.open(base/name,'rt') as file: return file.read()
    return (base/name).read_text()
def terminals(raw, action, cases):
    return set(re.findall(r'--- '+action+r': (\S+)',raw)) & cases
header = 'TestGraphCheckpointRejectsAmbiguousEarlierCompletion/'
bad = {header+encoding+'/'+fault for encoding in ('json','protobuf-v1') for fault in ('duplicate','alias','escaped','unknown','missing','null')}
valid = {header+encoding+'/'+name for encoding in ('json','protobuf-v1') for name in ('valid','opaque-result')}
batch = 'TestGraphCheckpointScanBatchesAndAuthority/'
batches = {batch+encoding+'/'+mode for encoding in ('json','protobuf-v1') for mode in ('paused','renewing','tail-change','retire-reuse','closed','expired')}
copies = {name for name in batches if not name.endswith(('/closed','/expired'))}
positive = read('final-directed-race.log')
assert terminals(positive,'PASS',bad|valid|batches) == bad|valid|batches
assert '--- FAIL:' not in positive
assert 'ok  \tjs-wf/journal\t7.007s' in positive
original = read('original-prefix-failure.log.gz')
assert terminals(original,'FAIL',bad) == bad
assert terminals(original,'PASS',valid) == valid
assert terminals(read('batch-budget-bypass.log.gz'),'FAIL',batches) == batches
assert terminals(read('result-ownership-bypass.log.gz'),'FAIL',batches) == copies
assert positive.count('mode=paused records=36 batches=8 entry_read_attempts=38 renewals=0 captured_tail=36') == 2
assert positive.count('mode=renewing records=36 batches=10 entry_read_attempts=37 renewals=18 captured_tail=36') == 2
broader = read('checkpoint-frame-index-archive-race.log.gz')
assert 'ok  \tjs-wf/journal\t123.433s' in broader and '--- FAIL:' not in broader
pins = read('all-853-pins.log.gz')
assert len(re.findall(r'--- PASS: TestPinnedRegressionCorpus/',pins)) == 853
assert 'ok  \tjs-wf/sim\t20.378s' in pins
native = read('native20-recovery-race.log')
assert 'budget=20 entries=20 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=19' in native
assert 'original_receipts_physically_reclaimed=19 before_stage=true' in native
assert 'ok  \tjs-wf/worker\t41.020s' in native
result = dict(scope='Directed unfrozen development checks only; broader runs preceded the final ownership copy. Actual100000 and original whole-plan gates remain open.',
              source_sha256=inputs, earlier_header_rejections=12, valid_header_controls=4,
              batch_authority_controls=12, required_negative_failures=dict(original_header_acceptance=12,batch_budget_bypass=12,result_ownership_bypass=8),
              scanned_records=36, ordinary_entry_read_attempts=37, deadline_entry_read_attempts=38,
              renewal=dict(ttl_seconds=4,virtual_seconds=37,renewals=18),
              earlier_broader_saved_pins=853, process_local_progress=True, durable_handover=False,
              unchanged_worker_deadline_seconds=15, actual100000_accepted=False)
(base/'development-review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({key:value for key,value in result.items() if key!='source_sha256'},indent=2))
