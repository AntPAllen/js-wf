from pathlib import Path
import json,subprocess,hashlib,importlib.util,sys,io,datetime
repo=Path('/home/exedev/js-wf');r=Path('/tmp/js-wf-lease-raft-offline-bound-20261006');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
rev=subprocess.check_output(['git','rev-parse','640c5a9'],cwd=repo,text=True).strip()
before=json.loads((r/'source-before.json').read_text());assert before['revision']==rev and before==json.loads((r/'source-after.json').read_text())
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(expected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+n+'\n' for n in expected).encode()))
for n in expected:
 h=stream.readline().split();assert h[1]==b'blob';data=stream.read(int(h[2]));assert stream.read(1)==b'\n';assert hashlib.sha256(data).hexdigest()==before['files'][n]==shared.sha(repo/n)==shared.sha(r/'selected-source'/n)
assert not stream.read()
for label,n in [('decoder','inspect-lease-raft-wal'),('adapter','lease-raft-reference-adapter'),('reference','lease-raft-reference-cli')]:assert (r/(label+'.go')).read_bytes()==subprocess.check_output(['git','cat-file','blob',rev+':scripts/'+n+'.go.txt'],cwd=repo)
deps=json.loads((r/'dependencies-before.json').read_text());assert deps==json.loads((r/'dependencies-after.json').read_text())
original=json.loads((r/'copied-fixture/external-source-before.json').read_text());matched=0
for path,row in deps.items():
 assert shared.sha(path)==shared.sha(r/row['captured'])==row['sha256']
 p=Path(path)
 if p.is_relative_to(r/'reference-nats-source'):
  originalpath='/home/exedev/go/pkg/mod/github.com/nats-io/nats-server/v2@v2.15.0/'+str(p.relative_to(r/'reference-nats-source'))
  assert originalpath in original and row['sha256']==original[originalpath]['sha256']==shared.sha(r/'copied-fixture'/original[originalpath]['captured']);matched+=1
assert matched>50
for label,row in json.loads((r/'commands.json').read_text()).items():assert shared.sha(r/label)==row['binary_sha256']
manifest=json.loads((r/'fixture-inventory.json').read_text());assert fixture_archive.inventory(r/'copied-fixture')==manifest['files']
result=json.loads((r/'result.json').read_text());start=datetime.datetime.fromisoformat(result['killed'].replace('Z','+00:00')).timestamp();end=datetime.datetime.fromisoformat(result['routes_heal_requested'].replace('Z','+00:00')).timestamp()
records=0
for node in range(3):
 rows=[json.loads(l) for l in (r/f'node-{node}.jsonl').read_text().splitlines() if '"append"' in l]
 refs=subprocess.run([str(r/'reference')],input=''.join(row['raw_append_base64']+'\n' for row in rows).encode(),capture_output=True,check=True)
 assert [json.loads(l) for l in refs.stdout.splitlines()]==[row['append'] for row in rows];records+=len(rows)
 if node==2:
  assert len(rows)==368 and [row['wal_sequence'] for row in rows]==list(range(3168,3536))
  for row in rows:
   assert start<row['timestamp_ns']/1e9<end and row['append']['commit']==3167 and row['wal_sequence']>3167
   for e in row['append']['entries']:
    if 'stream' in e:assert start<e['stream']['stream_timestamp_ns']/1e9<end and 'Nats-Marker-Reason: MaxAge' in e['stream']['headers']
assert records==369 and 'Installing snapshot of 822 bytes [1:3167]' in (r/'copied-fixture/originals/cluster/node-2.log').read_text()
assert 'checksum mismatch' in (r/'checksum-control.stderr').read_text()
proof=r.with_name(r.name+'-proof');meta=json.loads((proof/'archive-verification.json').read_text());inventory=json.loads((proof/'fixture-inventory.json').read_text());assert fixture_archive.verify(r.with_suffix('.tar.gz'))==inventory and fixture_archive.inventory(r)==inventory['files']
with r.with_suffix('.tar.gz').open('rb') as f:assert fixture_archive.digest(f)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
out=repo/'docs/scale/lease-partition-component-2026-10-06/offline-wal-review'
report=dict(source=rev,selected_source_files=len(expected),dependency_files=len(deps),pinned_server_sources_matched_original_failure=matched,selected_source_Git_current_retained_before_after_equal=True,copied_fixture_unchanged=True,upstream_decoder_all_369_records_equal=True,minority_marker_and_delete_timestamps_all_within_confirmed_isolation=True,shutdown_snapshot_index=3167,minority_tail_sequence_range=[3168,3535],all_archive_members_verified=meta['members'],closure=shared.closure(r),scope='Retained divergent tail contains expiry marker/delete proposals above commit3167, written while minority isolated; raw logs show slow conflict rollback. No original reopened, native fix, fullmatrix or causalTier1 qualification.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');(out/'executed-independent-review.py').write_bytes(Path(__file__).read_bytes());print(json.dumps(report))
