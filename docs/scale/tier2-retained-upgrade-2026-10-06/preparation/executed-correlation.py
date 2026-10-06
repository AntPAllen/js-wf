from pathlib import Path
import subprocess,json,hashlib,tarfile,io
repo=Path('/home/exedev/js-wf');base=Path('docs/scale/tier2-retained-row-2026-10-05/upgrade-smoke-failed');root=Path('/tmp/js-wf-tier2-retained-upgrade-smoke-20261005')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
m=json.loads(subprocess.check_output(['git','show',head+':'+str(base/'archive-verification.json')],cwd=repo));parts=[]
for part in m['parts']:
 data=subprocess.check_output(['git','cat-file','blob',head+':'+str(base/part['file'])],cwd=repo)
 assert len(data)==part['bytes'] and hashlib.sha256(data).hexdigest()==part['sha256'];parts.append(data)
archive=b''.join(parts);assert len(archive)==m['archive_bytes'] and hashlib.sha256(archive).hexdigest()==m['archive_sha256']
inputs={}
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as t:
 for name in ('matrix-upgrade-1-dispatch.jsonl','matrix-upgrade-1-operations.jsonl','matrix-upgrade-1-queue.json','matrix-upgrade-1-faults.json'):
  data=t.extractfile(name).read();assert data==(root/name).read_bytes();inputs[name]=dict(sha256=hashlib.sha256(data).hexdigest(),bytes=len(data))
rows=lambda name:[json.loads(s) for s in (root/name).read_text().splitlines()]
d=[e for e in rows('matrix-upgrade-1-dispatch.jsonl') if e.get('RunSequence')==494]
ops=[e for e in rows('matrix-upgrade-1-operations.jsonl') if e.get('RunSequence')==494 and e.get('Operation')=='dispatch_ack_confirmed']
assert len(ops)==1 and ops[0]['Delivery']==4 and not ops[0]['Error']
acks=[e for e in d if e['Stage']=='ack'];assert len(acks)==1 and acks[0]['Delivery']==4 and not acks[0]['Error']
q=json.loads((root/'matrix-upgrade-1-queue.json').read_text());assert len(q['messages'])==1 and q['messages'][0]['Sequence']==494
assert len(q['consumers'])==64 and all(c['num_pending']==c['num_ack_pending']==0 for c in q['consumers'])
f=json.loads((root/'matrix-upgrade-1-faults.json').read_text())['faults'][0]
out=repo/'docs/scale/tier2-retained-upgrade-2026-10-06/preparation'
(out/'acked-residual-correlation.json').write_text(json.dumps(dict(head=head,canonical_archive_sha256=m['archive_sha256'],all_canonical_part_and_concat_hashes_verified=True,current_logs_match_canonical_archive=True,inputs=inputs,dispatch=d,confirmed_ack=ops[0],fault=f,residual=q['messages'][0],consumer_count=64,all_consumers_pending_and_ack_pending_zero=True,scope='Historical confirmed ACK and subsequent retained raw message correlate to exact delivery; physical deletion cause remains unconfirmed. No original store reopened.'),indent=2)+'\n')
(out/'executed-correlation.py').write_bytes(Path(__file__).read_bytes());print('CANONICAL_ACK_RESIDUAL_CORRELATION_VERIFIED')
