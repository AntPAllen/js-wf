import sys,json,importlib.util,shutil,datetime
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
spec=importlib.util.spec_from_file_location('old','/tmp/cleanup-evidence-storage-20261008.py');old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
root=Path('/tmp/js-wf-graph-worker-failure-1128673321')
base=repo/'docs/scale/graph-worker-2026-10-08/development-failure'
archive=Path('/tmp/graph-worker-development-failure-20261008.tar.gz')
if len(sys.argv)>1 and sys.argv[1]=='closure':print(json.dumps(old.closure([root,archive])));sys.exit()
assert not base.exists() and not archive.exists()
import subprocess
closure=json.loads(subprocess.check_output(['sudo','-n','python',__file__,'closure'],text=True));assert not closure['blocked'] and not closure['permission_limits'],closure
base.mkdir(parents=True)
proof=fixture_archive.capture(root,archive,base,compresslevel=1)
(base/'capture.json').write_text(json.dumps(dict(root=str(root),archive=str(archive),privileged_closure=closure,utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Failed development model trace, seed1/readback_lost, wrong partition fixture; not a frozen acceptance campaign or server failure. Native development separately failed unbounded payload reads before explicit budgets were added. Native runs before the fix failed their two-minute contexts; corrected R1/R3 pass.'),indent=2)+'\n')
shutil.copyfile(__file__,base/'executed-capture.py')
(base/'README.md').write_text('# Failed graph-worker development model\n\nComplete seed1 trace from the initial fixture using a workflow identity mapped to a different partition from its consumer. It produced a tight empty-fetch loop and eventually failed its 5s context; saving the 942MiB trace dominated the 65.45s elapsed test. A termination attempt occurred after the SDK was already gone; it was not restarted on an observation timeout. The corrected fixture uses the intended partition and passes all six modes. This failed development artifact remains a failure and establishes no native/release acceptance.\n')
print(json.dumps(dict(archive_bytes=proof['archive_bytes'],sha256=proof['archive_sha256'])))
