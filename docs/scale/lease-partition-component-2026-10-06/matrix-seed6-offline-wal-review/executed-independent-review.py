from pathlib import Path
import json,subprocess,hashlib,importlib.util,sys,io,datetime
repo=Path('/home/exedev/js-wf');r=Path('/tmp/js-wf-matrix-seed6-offline-bound-20261006');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
rev=subprocess.check_output(['git','rev-parse','8b1a2c8'],cwd=repo,text=True).strip()
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
  assert originalpath in original and row['sha256']==original[originalpath]==shared.sha(r/'copied-fixture/selected-external-source/modules'/originalpath.split('/pkg/mod/',1)[1]);matched+=1
assert matched>50
for label,row in json.loads((r/'commands.json').read_text()).items():assert shared.sha(r/label)==row['binary_sha256']
manifest=json.loads((r/'fixture-inventory.json').read_text());assert fixture_archive.inventory(r/'copied-fixture')==manifest['files']
result=json.loads((r/'input-timing.json').read_text())['faults'][0];start=datetime.datetime.fromisoformat(result['killed'].replace('Z','+00:00')).timestamp();end=start+10;assert result['partition_routes']==[4,4,0] and result['node']==2 and result['majority_sequence']==1 and 'S-R3F-GnIzyO0Q' in result['error']
records=0
for node in range(3):
 rows=[json.loads(l) for l in (r/f'node-{node}.jsonl').read_text().splitlines() if '"append"' in l]
 refs=subprocess.run([str(r/'reference')],input=''.join(row['raw_append_base64']+'\n' for row in rows).encode(),capture_output=True,check=True)
 assert [json.loads(l) for l in refs.stdout.splitlines()]==[row['append'] for row in rows];records+=len(rows)
 if node==2:
  rows=[row for row in rows if row['wal_sequence']>2687]
  assert len(rows)==9 and [row['wal_sequence'] for row in rows]==list(range(2688,2697))
  for row in rows:
   assert start<row['timestamp_ns']/1e9<end and row['append']['commit']==2687 and row['wal_sequence']>2687
   for e in row['append']['entries']:
    if 'stream' in e:assert start<e['stream']['stream_timestamp_ns']/1e9<end and 'Nats-Marker-Reason: MaxAge' in e['stream']['headers']
assert records==3546
assert (r/'copied-fixture/originals/TestMixedMatrixServerPartitionEveryThirtySeconds/node-2/jetstream/$SYS/_js_/S-R3F-GnIzyO0Q/snapshots/snap.1.2687').is_file()
majority={row['wal_sequence']:row for row in [json.loads(l) for l in (r/'node-0.jsonl').read_text().splitlines()] if 'append' in row}
minority={row['wal_sequence']:row for row in [json.loads(l) for l in (r/'node-2.jsonl').read_text().splitlines()] if 'append' in row and row['wal_sequence']>2687}
assert set(majority)&set(minority)==set(range(2689,2697))
for index in set(majority)&set(minority):assert majority[index]['append']['term']==2 and minority[index]['append']['term']==1 and majority[index]['record_sha256']!=minority[index]['record_sha256']

assert 'checksum mismatch' in (r/'checksum-control.stderr').read_text()
proof=r.with_name(r.name+'-proof');meta=json.loads((proof/'archive-verification.json').read_text());inventory=json.loads((proof/'fixture-inventory.json').read_text());assert fixture_archive.verify(r.with_suffix('.tar.gz'))==inventory and fixture_archive.inventory(r)==inventory['files']
with r.with_suffix('.tar.gz').open('rb') as f:assert fixture_archive.digest(f)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
out=repo/'docs/scale/lease-partition-component-2026-10-06/matrix-seed6-offline-wal-review'
out.mkdir(exist_ok=True)
import shutil
for n in ['source-before.json','source-after.json','dependencies-before.json','dependencies-after.json','original-nats-source-matches.json','commands.json','restoration.json','review.json','input-timing.json','closure.json','executed-review.py','node-0.jsonl','node-1.jsonl','node-2.jsonl','checksum-control.stderr']:shutil.copyfile(r/n,out/n)
for n in ['archive-verification.json','fixture-inventory.json']:shutil.copyfile(proof/n,out/n)
report=dict(source=rev,selected_source_files=len(expected),dependency_files=len(deps),pinned_server_sources_matched_original_failure=matched,selected_source_Git_current_retained_before_after_equal=True,copied_fixture_unchanged=True,upstream_decoder_all_3546_records_equal=True,minority_marker_and_delete_timestamps_all_within_confirmed_isolation=True,retained_snapshot_filename_index=2687,minority_tail_sequence_range=[2688,2696],eight_same_index_majority_term2_minority_term1_conflicts_verified=True,all_archive_members_verified=meta['members'],closure=shared.closure(r),scope='Retained divergent tail contains expiry marker/delete proposals above commit2687, written while minority isolated; eight same-index term conflicts and raw catch-up warnings are retained. No original reopened, native fix, fullmatrix or causalTier1 qualification.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');(out/'executed-independent-review.py').write_bytes(Path(__file__).read_bytes());print(json.dumps(report))
