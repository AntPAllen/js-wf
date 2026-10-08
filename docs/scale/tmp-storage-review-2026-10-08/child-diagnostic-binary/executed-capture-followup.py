import sys,json,subprocess,hashlib,shutil
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
base=repo/'docs/scale/tmp-storage-review-2026-10-08/child-diagnostic-binary'
stage=Path('/home/exedev/child-diagnostic-staging-20261008');archive=Path('/home/exedev/child-diagnostic-20261008.tar.gz')
roots=[Path('/tmp/graph-child-diagnostic.test'),Path('/tmp/graph-child-diagnostic-cpu.pprof'),Path('/tmp/graph-child-diagnostic-profile.log')]
inv=json.loads((base/'fixture-inventory.json').read_text())
assert fixture_archive.inventory(stage)==inv['files']
for p in roots:
 st=p.stat();assert dict(bytes=st.st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),mode=st.st_mode&0o777,mtime_ns=st.st_mtime_ns)==inv['files'][p.name]
closure=json.loads(subprocess.check_output(['sudo','-n','python3','/home/exedev/archive-child-diagnostic-20261008.py','closure'],text=True));assert not closure['blocked'] and not closure['permission_limits']
(base/'capture.json').write_text(json.dumps(dict(roots=[str(p) for p in roots],archive=str(archive),stage=str(stage),unchanged_originals=True,fresh_privileged_closure=closure,initial_guard='Rejected the calling shell argv because its heredoc contained the original paths. No local removal occurred; this separate invocation has no path references or permission gaps.',scope='Closed development profiling artifacts; preservation only, no qualification changes.'),indent=2)+'\n')
shutil.copyfile('/home/exedev/archive-child-diagnostic-20261008.py',base/'executed-storage.py');shutil.copyfile(__file__,base/'executed-capture-followup.py')
(base/'README.md').write_text('''# Closed child-model diagnostic binary offload

Full archive preserves the25,082,502-byte compiled profiling binary, original profile/stdout, Go binary build metadata and source/profile binding. This is development profiling evidence only, not frozen native/release admission. Git retains its full inventory and verified S3 receipt; original local files and staging/archive remain until final removal verification.

The initial usage guard rejected the calling shell argv because its heredoc contained the source paths. No files were removed. A separate privileged scan now has no references or permission gaps, and every original/staged file matches the archive inventory. Final retirement requires a pushed receipt, fresh complete S3 member/compressed-body readback and another privileged usage check.
''')
print('capture originals/staging unchanged; fresh privileged usage check clear')
