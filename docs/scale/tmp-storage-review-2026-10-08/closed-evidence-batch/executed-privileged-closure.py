import sys,json,importlib.util
from pathlib import Path
sys.dont_write_bytecode=True
spec=importlib.util.spec_from_file_location('batch','/tmp/js-wf-tmp-batch-storage-20261008.py');batch=importlib.util.module_from_spec(spec);spec.loader.exec_module(batch)
c=json.loads((batch.base/'capture.json').read_text())
print(json.dumps(batch.closure([Path(r) for r in c['roots']]+[batch.archive])))
