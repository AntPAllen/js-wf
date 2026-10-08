import sys,json,hashlib,datetime,shutil
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-candidate-seed31-invocation-media-20261008')
v=json.loads(Path('/tmp/seed31-invocation-records.json').read_text());assert len(v)==3
records=v[0]['records'];assert all(x['records']==records for x in v)
assert all(x['contiguous_through']==850 and x['all_checksums_valid'] for x in v)
assert len({r['subject'] for r in records})==850
history=Path('/tmp/js-wf-candidate-seed31-diagnostics-20261007/selected/seed-031/matrix-partition-31-history.jsonl')
rows=[json.loads(l) for l in history.read_text().splitlines()]
starts=[r for r in rows if r['op']=='start' and r.get('result',{}).get('status')=='started' and r['args']['id'].startswith('seed-31-batch-29-')]
assert len(starts)==10 and sorted(r['result']['inv_seq'] for r in starts)==list(range(813,823))
for s in starts:
 rec=records[s['result']['inv_seq']-1]
 assert rec['subject']=='wf.inv.'+s['args']['type']+'.'+s['args']['id']
 assert hashlib.sha256(rec['data'].encode()).hexdigest()==s['args']['input_hash']
 assert s['args']['input_hash'] in rec['header']
cohort=[r for r in rows if r['op']=='getResult' and r['args']['id'].startswith('seed-31-batch-29-')]
assert len(cohort)==10 and all(r.get('result',{}).get('status')=='completed' for r in cohort)
meta=json.loads(next(root.glob('selected/seed-031/originals/*/node-0/jetstream/$G/streams/WF_INV/meta.inf')).read_text())
assert meta['max_age']==0 and meta['max_msgs_per_subject']==1
report={'scope':'Read-only parsing of original failed seed31 persisted media; no server reopened or test rerun. Physical record presence after shutdown does not independently prove visibility at the audit instant or rule out transient server rollback. All original native verdicts remain failed.','physical_record_count_each_peer':850,'contiguous_from':1,'contiguous_through':850,'checksums_valid_all_three':True,'all_three_records_identical':True,'no_erasure_or_tombstone_records':True,'block_sha256':v[0]['block_sha256'],'captured_weak_cutoff':812,'completed_cohort_expected':840,'acknowledged_batch29_starts':starts,'completed_batch29_results':cohort,'persisted_omitted_records':records[812:840],'original_stream_config':meta,'parser_format_source':'github.com/nats-io/nats-server/v2 v2.15.0 server/filestore.go recoverMsgs/msgFromBufEx/hashKeyForBlock','original_archive_receipt':'docs/scale/tmp-storage-review-2026-10-07/twenty-fourth-candidate-closed/s3-readback.json','restore_proof_sha256':hashlib.sha256((root/'restore-proof.json').read_bytes()).hexdigest()}
(root/'media-review.json').write_text(json.dumps(report,indent=2)+'\n')
shutil.copy2(__file__,root/'executed-review.py');shutil.copy2('/tmp/parse_seed31_invocation.go',root/'readonly-parser.go.txt')
print(json.dumps({'count_each':850,'weak_cut':812,'acknowledged_minimum':822,'completed_expected':840}))
