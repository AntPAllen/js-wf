from pathlib import Path
import json,sys,os,hashlib,shutil,datetime,subprocess
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
catalogue=Path('/tmp/js-wf-closed-binary-catalogue-20261006.json')
r=json.loads(catalogue.read_text())
root=Path('/tmp/js-wf-closed-binary-preservation-stage-20261006');root.mkdir()
out=repo/'docs/scale/tmp-storage-review-2026-10-06/closed-test-binaries/complete'
raw=Path('/tmp/js-wf-closed-test-binaries-complete-20261006.tar.gz')
for h,paths in r['groups'].items():
 p=Path(paths[0]);record=r['files'][str(p)];s=p.stat();assert s.st_nlink==1 and s.st_size==record['bytes'] and s.st_mtime_ns==record['mtime_ns'] and s.st_mode&0o777==record['mode']
 with p.open('rb') as stream:assert fixture_archive.digest(stream)==dict(bytes=record['bytes'],sha256=h)
 os.link(p,root/(h+'.test'))
(root/'original-paths.json').write_bytes(catalogue.read_bytes())
print('PRESERVATION_STAGE',len(r['groups']),'UNIQUE_BINARIES',flush=True)
proof=fixture_archive.capture(root,raw,out,compresslevel=1)
assert (root/'original-paths.json').read_bytes()==catalogue.read_bytes()
for name in root.iterdir():name.unlink()
root.rmdir()
for path,record in r['files'].items():
 p=Path(path);s=p.stat();assert s.st_nlink==1 and s.st_size==record['bytes'] and s.st_mtime_ns==record['mtime_ns'] and s.st_mode&0o777==record['mode']
 with p.open('rb') as stream:assert fixture_archive.digest(stream)==dict(bytes=record['bytes'],sha256=record['sha256'])
shutil.copyfile(catalogue,out/'original-paths.json');shutil.copyfile(__file__,out/'executed-capture.py')
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),original_files=len(r['files']),unique_binaries=len(r['groups']),original_allocated_bytes=sum(x['allocated_bytes'] for x in r['files'].values()),proof=proof,original_bytes_modes_mtimes_unchanged=True,temporary_hardlink_stage_removed=True,scope='File preservation only. Catalogue records visible closed-root observation and its inaccessible-process limits. No native/source/terminal gate qualification or provider durability claim. Local originals retained pending committed S3 proof and fresh closure/body/member/original-file verification.')
(out/'capture.json').write_text(json.dumps(report,indent=2)+'\n');print('COMPLETE_BINARY_CAPTURE',proof,'FREE',shutil.disk_usage(repo).free,flush=True)
