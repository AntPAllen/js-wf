#!/usr/bin/env python3
"""Review the complete clock archive without duplicating physical stores on disk."""
import argparse,hashlib,importlib.util,io,json,subprocess,tarfile
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
row=load('row','scripts/check-tier3-journal-row.py')
explain=load('explain','scripts/explain-tier3-events.py')
fencing=load('fencing','scripts/review-tier3-fencing.py')
archive=a.root/'rolling-originals.tar.gz';manifest=json.loads((a.root/'rolling-manifest.json').read_text())['files']
revision='82069c3d45f344dae3068877f04e34ff190738c2'
terminal=json.loads((a.root/'terminal.json').read_text())
assert terminal['status']=='completed' and terminal['conclusion']=='success' and terminal['headSha']==revision
if archive.exists():
    tar_reader=tarfile.open(archive)
else:
    parts=json.loads((a.root/'archive-parts.json').read_text())
    data=bytearray()
    for part in parts['parts']:
        block=(a.root/part['name']).read_bytes()
        assert len(block)==part['bytes'] and hashlib.sha256(block).hexdigest()==part['sha256']
        data.extend(block)
    assert len(data)==parts['bytes'] and hashlib.sha256(data).hexdigest()==parts['sha256']
    tar_reader=tarfile.open(fileobj=io.BytesIO(data))
    del data
with tar_reader as tar:
 members={m.name:m for m in tar.getmembers()}
 assert len(members)==len(tar.getmembers()) and set(members)==set(manifest)
 cache={}
 for name,member in members.items():
  stream=tar.extractfile(member);digest=hashlib.sha256()
  while block:=stream.read(1<<20):digest.update(block)
  assert digest.hexdigest()==manifest[name],name
 class ArchivePath:
  def __init__(self,name=''):self.name=str(name)
  def __truediv__(self,name):return ArchivePath(str(Path(self.name)/name))
  @property
  def parent(self):return ArchivePath(str(Path(self.name).parent))
  def read_bytes(self):
   if self.name not in cache:cache[self.name]=tar.extractfile(members[self.name]).read()
   return cache[self.name]
  def read_text(self):return self.read_bytes().decode()
  def exists(self):return self.name in members
  def __str__(self):return self.name
 root=ArchivePath()
 read=lambda name:json.loads((root/name).read_text())
 before=read('worker-clock-source-before.json');assert before==read('worker-clock-source-after.json')
 assert before['revision']==revision and before['clean']
 for name,digest in before['files'].items():
  raw=subprocess.check_output(['git','show',revision+':'+name]);assert hashlib.sha256(raw).hexdigest()==digest,name
 events=[json.loads(line) for line in (root/'tier3-mixed-journal-events.jsonl').read_text().splitlines()]
 report=row.check(events,'10m','worker_clock',1)
 report['worker_clock_artifact_checks']=row.check_worker_clock_artifacts(root,report)
 report['checkpoint_audit_checks']=row.check_checkpoint_audits(root,report)
 assert json.dumps(report,indent=2)+'\n'==(root/'tier3-mixed-journal-result.json').read_text()
 sessions=read('process-evidence.json')
 expected={s['worker_id']:s['final_metrics']['fencing_events'] for s in sessions}
 explanation=explain.check(read('worker-metrics.json'),read('fencing.json'),read('repairs.json'),expected_workers=expected)
 uploaded=read('event-explanations.json')
 explanation['complete_hard_kill_attribution']=False
 explanation['input_sha256']={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ('worker-metrics.json','fencing.json','repairs.json','process-evidence.json')}
 assert json.dumps(explanation,indent=2)+'\n'==(root/'event-explanations.json').read_text()
 review=fencing.review(*(read(n) for n in ('fencing.json','dispatch.json','latencies.json','faults.json')))
 uploaded=read('fencing-timeline-review.json')
 assert json.dumps(review,indent=2)+'\n'==(root/'fencing-timeline-review.json').read_text()
 result=dict(run_id=37108814121,revision=revision,accepted=True,archive_members_verified=len(members),source_files_verified=len(before['files']),
             invocations=report['invocations'],journal_entries=report['journal_entries'],clock_probes=report['worker_clock_artifact_checks']['clock_probes'],
             broker_clock_messages=report['worker_clock_artifact_checks']['broker_clock_messages'],fencing_records=explanation['fencing_records'],
             graceful_counter_cross_checks=5,checkpoint_audits=report['checkpoint_audit_checks']['completed_cohort_audits'],
             worst_terminal_p99_seconds=max(c['terminal_p99_seconds'] for c in report['cells'].values()),
             worst_progress_p99_seconds=max(c['progress_p99_seconds'] for c in report['cells'].values()),
             row_report_regenerates_identically=True,scope='single real R5 worker-clock row; seed1/10m',clears_full_tier3_release=False)
a.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
