import sys,shutil,json,hashlib,subprocess,importlib.util
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
base=repo/'docs/scale/tmp-storage-review-2026-10-08/child-diagnostic-binary'
stage=Path('/home/exedev/child-diagnostic-staging-20261008');archive=Path('/home/exedev/child-diagnostic-20261008.tar.gz')
roots=[Path('/tmp/graph-child-diagnostic.test'),Path('/tmp/graph-child-diagnostic-cpu.pprof'),Path('/tmp/graph-child-diagnostic-profile.log')]
source=repo/'docs/scale/graph-child-transfer-followup-2026-10-08-development'
if len(sys.argv)>1 and sys.argv[1]=='closure':
 spec=importlib.util.spec_from_file_location('closed',repo/'docs/scale/tmp-storage-review-2026-10-08/followup-small-files/executed-closure-library.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 print(json.dumps(m.closure(roots+[archive])))
else:
 assert not stage.exists() and not archive.exists() and not base.exists()
 stage.mkdir();base.mkdir(parents=True)
 binding=json.loads((source/'profile-binding.json').read_text())
 assert hashlib.sha256(roots[0].read_bytes()).hexdigest()==binding['binary_sha256']
 for p in roots:shutil.copy2(p,stage/p.name)
 for name in ['profile-binding.json','profile-top.txt']:shutil.copy2(source/name,stage/name)
 (stage/'binary-go-version.txt').write_text(subprocess.check_output(['go','version','-m',str(roots[0])],text=True))
 proof=fixture_archive.capture(stage,archive,base,compresslevel=1)
 checks=json.loads(subprocess.check_output(['sudo','-n','python3',__file__,'closure'],text=True))
 assert not checks['blocked'] and not checks['permission_limits'],checks
 (base/'capture.json').write_text(json.dumps(dict(roots=[str(p) for p in roots],archive=str(archive),closure=checks,source_profile_binding=binding,scope='Preservation of closed development profiling binary, profile and output only; no test verdict changes or frozen SDK admission.'),indent=2)+'\n')
 shutil.copyfile(__file__,base/'executed-storage.py');shutil.rmtree(stage)
 print(json.dumps(dict(archive_bytes=proof['archive_bytes'],members=proof['members'],sha256=proof['archive_sha256'])))
