#!/usr/bin/env python3
"""Review the complete clock archive without duplicating physical stores on disk."""
import argparse,fnmatch,hashlib,importlib.util,io,json,subprocess,tarfile
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--shutdown',choices=('sigkill','ldm'),required=True);p.add_argument('--run-id',type=int,required=True);a=p.parse_args()
assert {37132399483:'sigkill',37132401225:'ldm'}[a.run_id]==a.shutdown
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
row=load('row','scripts/check-tier3-journal-row.py')
explain=load('explain','scripts/explain-tier3-events.py')
fencing=load('fencing','scripts/review-tier3-fencing.py')
archive=a.root/'rolling-originals.tar.gz';manifest=json.loads((a.root/'rolling-manifest.json').read_text())['files']
revision='c5125cd810d407e26a778ef102f2cb093ff233c6'
terminal=json.loads((a.root/'terminal.json').read_text())
assert terminal['status']=='completed' and terminal['conclusion']=='success' and terminal['headSha']==revision
assert len(terminal['jobs'])==2 and {j['name'] for j in terminal['jobs']}=={'seeds','journal (1)'} and all(j['status']=='completed' and j['conclusion']=='success' for j in terminal['jobs'])
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
  def __lt__(self,other):return self.name<other.name
  def glob(self,pattern):return [ArchivePath(n) for n in members if fnmatch.fnmatch(n,str(Path(self.name)/pattern))]
 root=ArchivePath()
 read=lambda name:json.loads((root/name).read_text())
 before=read('rolling-upgrade-source-before.json');assert before==read('rolling-upgrade-source-after.json')
 assert before['revision']==revision and before['clean']
 tracked=subprocess.check_output(['git','ls-tree','-r','--name-only',revision],text=True).splitlines()
 assert set(before['files'])=={n for n in tracked if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum')}
 for name in ('scripts/check-tier3-journal-row.py','scripts/explain-tier3-events.py','scripts/review-tier3-fencing.py'):
  assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==before['files'][name]
 for name,digest in before['files'].items():
  raw=subprocess.check_output(['git','show',revision+':'+name]);assert hashlib.sha256(raw).hexdigest()==digest,name
 events=[json.loads(line) for line in (root/'tier3-mixed-journal-events.jsonl').read_text().splitlines()]
 report=row.check(events,'10m','rolling_upgrade',1,require_upgrade_start_gap=True,expected_upgrade_shutdown=a.shutdown)
 report['upgrade_artifact_checks']=row.check_upgrade_artifacts(root,report)
 assert report['upgrade_artifact_checks']['forced_start_gaps']==5 and report['upgrade_artifact_checks']['upgraded_peers']==5
 report['start_scan_progress_checks']=row.check_start_scan_progress(root)
 report['checkpoint_audit_checks']=row.check_checkpoint_audits(root,report)
 assert json.dumps(report,indent=2)+'\n'==(root/'tier3-mixed-journal-result.json').read_text()
 explanation=explain.check(read('worker-metrics.json'),read('fencing.json'),read('repairs.json'))
 explanation['input_sha256']={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ('worker-metrics.json','fencing.json','repairs.json')}
 assert json.dumps(explanation,indent=2)+'\n'==(root/'event-explanations.json').read_text()
 review=fencing.review(*(read(n) for n in ('fencing.json','dispatch.json','latencies.json','faults.json')))
 uploaded=read('fencing-timeline-review.json')
 assert json.dumps(review,indent=2)+'\n'==(root/'fencing-timeline-review.json').read_text()
 gaps=[read(f'fault-{i}-start-gap/completion.json') for i in range(1,6)]
 scans=read('start-scans.json')
 result=dict(run_id=a.run_id,revision=revision,accepted=True,shutdown_mode=a.shutdown,
             archive_members_verified=len(members),source_files_verified=len(before['files']),
             invocations=report['invocations'],journal_entries=report['journal_entries'],upgraded_peers=report['upgrade_artifact_checks']['upgraded_peers'],
             native_rejections=report['upgrade_artifact_checks']['native_rejections'],fencing_records=explanation['fencing_records'],
             inprocess_counter_cross_checks=5,checkpoint_audits=report['checkpoint_audit_checks']['completed_cohort_audits'],
             worst_terminal_p99_seconds=max(c['terminal_p99_seconds'] for c in report['cells'].values()),
             worst_progress_p99_seconds=max(c['progress_p99_seconds'] for c in report['cells'].values()),
             row_report_regenerates_identically=True,forced_start_gaps=5,
             gap_kill_to_terminal_seconds=[g['kill_to_terminal_ns']/1e9 for g in gaps],
             start_scan_progress=report['start_scan_progress_checks'],
             failed_scan_calls=sum(bool(s.get('error')) for s in scans),
             scope='R5 five-peer forced cohort Start gaps across retained-store rolling upgrades; seed1/10m atc5125cd. Independent hash/raw-report verification; no independent physical store reopen.200 seeds/24h and newer prefix checkpoint source remain open.',clears_full_tier3_release=False)

a.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
