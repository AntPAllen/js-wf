import sys
sys.dont_write_bytecode=True
from pathlib import Path
import json,subprocess,importlib.util,shutil
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
root=Path('/tmp/js-wf-worker-leaf-wire-20261007');out=Path('/tmp/js-wf-worker-leaf-wire-20261007-proof')
unit=subprocess.check_output(['systemctl','show',root.name+'.service','-p','MainPID','-p','SubState','-p','ExecMainStatus'],text=True)
assert 'MainPID=0\n' in unit and 'SubState=failed\n' in unit and 'ExecMainStatus=1\n' in unit
assert json.loads((root/'execution.json').read_text())['exit_code']==0
before=shared.closure(root)
proof=fixture_archive.capture(root,root.with_suffix('.tar.gz'),out,compresslevel=1)
after=shared.closure(root)
log=subprocess.check_output(['sudo','-n','journalctl','-u',root.name+'.service','--no-pager'],text=True)
assert 'TypeError: unhashable type:' in log
(out/'original-collector-failure.log').write_text(log)
(out/'storage-capture.json').write_text(json.dumps(dict(original_producer_exit=1,native_exit=0,unit=unit,closure_before=before,closure_after=after,proof=proof,scope='Fresh complete storage capture after collector TypeError. Original producer remains failed; native Go suites passed. No original files edited, stores opened, process or tests rerun.'),indent=2)+'\n')
shutil.copyfile(__file__,out/'executed-storage-capture.py')
print('PRESERVED',proof,flush=True)
