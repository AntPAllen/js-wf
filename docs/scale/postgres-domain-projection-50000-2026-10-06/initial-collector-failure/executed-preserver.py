from pathlib import Path
import sys,json,subprocess,importlib.util,shutil
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
root=Path('/tmp/js-wf-postgres-domain-projection-50000-20261006');out=repo/'docs/scale/postgres-domain-projection-50000-2026-10-06/initial-collector-failure'
unit=dict(l.split('=',1) for l in subprocess.check_output(['systemctl','--user','show','js-wf-postgres-domain-projection-50000-20261006.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines());assert unit==dict(ActiveState='failed',MainPID='0',ExecMainStatus='1')
assert not (root/'actual-sdk.json').exists() and not (root/'postgres-image.json').exists()
closure=shared.closure(root)
proof=fixture_archive.capture(root,root.with_suffix('.tar.gz'),out,compresslevel=1)
log=subprocess.check_output(['journalctl','--user','-u','js-wf-postgres-domain-projection-50000-20261006.service','--no-pager'],text=True)
(out/'native-producer-failure.log').write_text(log)
report=dict(unit=unit,closure=closure,source=json.loads((root/'source-before.json').read_text())['revision'],complete_archive=proof,scope='Original partial external-input collector failure only. No PostgreSQL container or native SDK was launched; no domain/recovery qualification. Complete existing partial files preserved without broker reopening.')
(out/'preservation.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-preserver.py')
print(json.dumps(report))
