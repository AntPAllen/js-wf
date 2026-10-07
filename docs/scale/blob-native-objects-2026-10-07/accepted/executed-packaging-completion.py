import sys,json,shutil
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import closure,fixture_archive
root=Path('/tmp/js-wf-native-blob-objects-20261007')
assert json.loads((root/'source-before.json').read_text())==json.loads((root/'source-after.json').read_text())
assert json.loads((root/'execution.json').read_text())['exit_code']==0
shutil.copyfile('/tmp/native-blob-objects-producer.log',root/'initial-packaging-failure.log')
shutil.copyfile(__file__,root/'executed-packaging-completion.py')
(root/'closure.json').write_text(json.dumps(closure(root),indent=2)+'\n')
proof=fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
print(json.dumps(proof),flush=True)
