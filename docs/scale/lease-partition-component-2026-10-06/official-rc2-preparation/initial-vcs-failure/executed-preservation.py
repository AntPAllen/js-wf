import sys,json,subprocess,shutil,importlib.util,datetime
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
root=Path('/tmp/js-wf-lease-component-official-rc2-20261007')
unit=dict(row.split('=',1) for row in subprocess.check_output(['systemctl','show','js-wf-lease-component-official-rc2-20261007.service','--property=ActiveState,SubState,MainPID,ExecMainPID,ExecMainStatus,Result,InvocationID'],text=True).splitlines())
assert unit['ActiveState']=='failed' and unit['MainPID']=='0' and unit['ExecMainStatus']=='1' and unit['Result']=='exit-code'
before=json.loads((root/'source-before.json').read_text());after=shared.source_inventory(before['revision']);assert before==after
assert not (root/'diagnostic').exists() and not (root/'originals').exists()
(root/'source-after.json').write_text(json.dumps(after,indent=2)+'\n')
(root/'failed-preparation.json').write_text(json.dumps(dict(unit=unit,no_native_helper_or_store_created=True,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),reason='go list attempted VCS stamping of copied official module; error obtaining VCS status exit128 before any helper/broker launch'),indent=2)+'\n')
shutil.copyfile('/tmp/js-wf-lease-component-official-rc2-20261007-producer.log',root/'producer.log')
(root/'closure.json').write_text(json.dumps(shared.closure(root),indent=2)+'\n')
out=repo/'docs/scale/lease-partition-component-2026-10-06/official-rc2-preparation/initial-vcs-failure'
proof=fixture_archive.capture(root,Path('/tmp/js-wf-lease-component-official-rc2-preparation-failure-20261007.tar.gz'),out,compresslevel=1)
for n in ['source-before.json','source-after.json','failed-preparation.json','producer.log','closure.json']:shutil.copyfile(root/n,out/n)
shutil.copyfile(__file__,out/'executed-preservation.py')
print('FAILED_PREPARATION_COMPLETE_ARCHIVE',proof['archive_bytes'],flush=True)
